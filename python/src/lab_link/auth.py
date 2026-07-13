from __future__ import annotations

import hashlib
import ipaddress
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol
from urllib.parse import parse_qs, urlsplit

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Route
from starlette.websockets import WebSocket


class SyncAuth(Protocol):
    """Authentication boundary used by :class:`LabSync`.

    Implementations own credentials and sessions. Applications remain free to
    render their login, sharing, and error UI however they like.
    """

    def routes(self, prefix: str) -> list[BaseRoute]: ...

    def is_http_authorized(self, request: Request) -> bool: ...

    def is_websocket_authorized(self, websocket: WebSocket) -> bool: ...


@dataclass(frozen=True, slots=True)
class AccessInvite:
    """A short-lived, single-use credential intended for a QR code or link."""

    token: str
    expires_at: datetime


class LanPassphraseAuth:
    """Small in-memory access gate for trusted laboratory networks.

    A long-lived passphrase creates an independent session for each browser.
    QR codes should use :meth:`create_invite`; each invite expires quickly and
    can be exchanged only once. Sessions and invitations intentionally vanish
    when the process restarts.
    """

    def __init__(
        self,
        passphrase: str | None = None,
        *,
        cookie_name: str = "lab_link_session",
        session_ttl: float = 12 * 60 * 60,
        invite_ttl: float = 5 * 60,
        trust_loopback: bool = True,
        allowed_origins: set[str] | None = None,
        max_failures: int = 5,
        failure_window: float = 60,
    ) -> None:
        self.passphrase = passphrase or self.generate_passphrase()
        self.cookie_name = cookie_name
        self.session_ttl = session_ttl
        self.invite_ttl = invite_ttl
        self.trust_loopback = trust_loopback
        self.allowed_origins = {origin.rstrip("/") for origin in allowed_origins or set()}
        self.max_failures = max_failures
        self.failure_window = failure_window

        self._sessions: dict[str, float] = {}
        self._invites: dict[str, float] = {}
        self._failures: dict[str, list[float]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def generate_passphrase() -> str:
        """Return a readable high-entropy passphrase such as ``ABCD-EFGH-JK23``."""
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        groups = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)]
        return "-".join(groups)

    def create_invite(self, *, ttl: float | None = None) -> AccessInvite:
        """Create a single-use invitation without exposing the master passphrase."""
        lifetime = self.invite_ttl if ttl is None else ttl
        if lifetime <= 0:
            raise ValueError("invite ttl must be greater than zero")
        token = secrets.token_urlsafe(32)
        expires = time.time() + lifetime
        with self._lock:
            self._prune_locked()
            self._invites[self._digest(token)] = expires
        return AccessInvite(
            token=token,
            expires_at=datetime.fromtimestamp(expires, timezone.utc),
        )

    def is_http_authorized(self, request: Request) -> bool:
        return self._is_trusted_host(self._client_host(request)) or self._has_session(
            request.cookies
        )

    def is_websocket_authorized(self, websocket: WebSocket) -> bool:
        if not self._origin_allowed(websocket.headers):
            return False
        return self._is_trusted_host(self._client_host(websocket)) or self._has_session(
            websocket.cookies
        )

    def routes(self, prefix: str) -> list[BaseRoute]:
        base = prefix.rstrip("/")
        return [
            Route(f"{base}/auth/status", self._status, methods=["GET"]),
            Route(f"{base}/auth/login", self._login, methods=["POST"]),
            Route(f"{base}/auth/invite", self._exchange_invite, methods=["POST"]),
            Route(f"{base}/auth/logout", self._logout, methods=["POST"]),
        ]

    async def _status(self, request: Request) -> JSONResponse:
        return JSONResponse({"authorized": self.is_http_authorized(request)})

    async def _login(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        client_host = self._client_host(request) or "unknown"
        if self._is_rate_limited(client_host):
            return self._error("rate_limited", 429)
        values = await self._values(request)
        supplied = str(values.get("passphrase", ""))
        if not secrets.compare_digest(supplied, self.passphrase):
            self._record_failure(client_host)
            return self._error("invalid_credentials", 401)
        self._clear_failures(client_host)
        return self._session_response(request)

    async def _exchange_invite(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        values = await self._values(request)
        token = str(values.get("invite", values.get("token", "")))
        if not self._consume_invite(token):
            return self._error("invalid_or_expired_invite", 401)
        return self._session_response(request)

    async def _logout(self, request: Request) -> Response:
        token = request.cookies.get(self.cookie_name, "")
        if token:
            with self._lock:
                self._sessions.pop(self._digest(token), None)
        response = JSONResponse({"authorized": False})
        response.delete_cookie(self.cookie_name, path="/")
        return response

    async def _values(self, request: Request) -> dict[str, Any]:
        body = await request.body()
        if len(body) > 4096:
            return {}
        if "application/json" in request.headers.get("content-type", ""):
            try:
                value = await request.json()
                return value if isinstance(value, dict) else {}
            except Exception:
                return {}
        parsed = parse_qs(body.decode("utf-8", errors="replace"))
        return {key: values[0] for key, values in parsed.items() if values}

    def _session_response(self, request: Request) -> JSONResponse:
        token = secrets.token_urlsafe(32)
        with self._lock:
            self._prune_locked()
            self._sessions[self._digest(token)] = time.time() + self.session_ttl
        response = JSONResponse({"authorized": True})
        response.set_cookie(
            self.cookie_name,
            token,
            max_age=max(1, int(self.session_ttl)),
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="strict",
            path="/",
        )
        return response

    def _has_session(self, cookies: dict[str, str]) -> bool:
        token = cookies.get(self.cookie_name, "")
        if not token:
            return False
        digest = self._digest(token)
        now = time.time()
        with self._lock:
            expires = self._sessions.get(digest, 0)
            if expires <= now:
                self._sessions.pop(digest, None)
                return False
            return True

    def _consume_invite(self, token: str) -> bool:
        if not token:
            return False
        digest = self._digest(token)
        now = time.time()
        with self._lock:
            expires = self._invites.pop(digest, 0)
            self._prune_locked(now)
        return expires > now

    def _is_rate_limited(self, client_host: str) -> bool:
        now = time.monotonic()
        with self._lock:
            recent = [
                value
                for value in self._failures.get(client_host, [])
                if now - value < self.failure_window
            ]
            self._failures[client_host] = recent
            return len(recent) >= self.max_failures

    def _record_failure(self, client_host: str) -> None:
        with self._lock:
            self._failures.setdefault(client_host, []).append(time.monotonic())

    def _clear_failures(self, client_host: str) -> None:
        with self._lock:
            self._failures.pop(client_host, None)

    def _prune_locked(self, now: float | None = None) -> None:
        now = time.time() if now is None else now
        self._sessions = {key: expiry for key, expiry in self._sessions.items() if expiry > now}
        self._invites = {key: expiry for key, expiry in self._invites.items() if expiry > now}

    def _is_trusted_host(self, host: str | None) -> bool:
        if not self.trust_loopback or not host:
            return False
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return host.lower() == "localhost"

    def _origin_allowed(self, headers: Any) -> bool:
        origin = headers.get("origin")
        if not origin:
            return True
        normalized = origin.rstrip("/")
        if normalized in self.allowed_origins:
            return True
        try:
            parsed = urlsplit(normalized)
            if parsed.netloc.lower() != headers.get("host", "").lower():
                return False
            hostname = parsed.hostname or ""
            if hostname.lower() == "localhost":
                return True
            ipaddress.ip_address(hostname)
            return True
        except ValueError:
            return False

    @staticmethod
    def _client_host(connection: Any) -> str | None:
        return connection.client.host if connection.client else None

    @staticmethod
    def _digest(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    @staticmethod
    def _error(code: str, status_code: int) -> JSONResponse:
        return JSONResponse({"authorized": False, "error": code}, status_code=status_code)
