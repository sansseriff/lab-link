from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import logging
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Literal, Protocol
from urllib.parse import parse_qs, urlsplit

from argon2 import PasswordHasher
from argon2.exceptions import VerificationError, VerifyMismatchError
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import BaseRoute, Route
from starlette.websockets import WebSocket

from .auth_store import SQLiteAuthStore, StoredApiToken, StoredSession

logger = logging.getLogger(__name__)
InviteStatus = Literal["active", "consumed", "expired", "revoked"]
InviteCallback = Callable[["InviteEvent"], Any]


class SyncAuth(Protocol):
    """Authentication boundary used by :class:`LabSync`."""

    def routes(self, prefix: str) -> list[BaseRoute]: ...

    def is_http_authorized(self, request: Request) -> bool: ...

    def is_websocket_authorized(self, websocket: WebSocket) -> bool: ...


@dataclass(frozen=True, slots=True)
class AuthPrincipal:
    """Authenticated identity attached to a connection and its commands."""

    id: str
    kind: Literal["local", "session", "api_token"]
    label: str
    capabilities: frozenset[str]
    session_id: str | None = None

    def can(self, capability: str) -> bool:
        return "*" in self.capabilities or capability in self.capabilities


@dataclass(frozen=True, slots=True)
class AccessInvite:
    """A short-lived, single-use credential intended for a QR code or link."""

    id: str
    token: str
    expires_at: datetime
    status: InviteStatus = "active"


@dataclass(frozen=True, slots=True)
class InviteEvent:
    invite_id: str
    status: InviteStatus
    occurred_at: datetime


@dataclass(frozen=True, slots=True)
class SessionInfo:
    id: str
    label: str
    created_at: datetime
    last_used_at: datetime
    expires_at: datetime
    remembered: bool
    auth_method: str
    capabilities: frozenset[str]


@dataclass(frozen=True, slots=True)
class ApiTokenCredential:
    """New API token. The plaintext token is returned only at creation time."""

    id: str
    token: str
    label: str
    created_at: datetime
    expires_at: datetime | None
    capabilities: frozenset[str]


@dataclass(slots=True)
class _InviteRecord:
    id: str
    token_hash: str
    expires_at: float
    status: InviteStatus = "active"


class LanPassphraseAuth:
    """Headless access control for laboratory applications on trusted networks.

    Without ``store``, behavior remains process-local and a readable passphrase
    is generated when none is supplied. With :class:`SQLiteAuthStore`, the
    passphrase hash, remembered devices, and API tokens survive restarts. An
    empty persistent store starts fail-closed for non-loopback clients until a
    local caller completes first-run setup.
    """

    def __init__(
        self,
        passphrase: str | None = None,
        *,
        store: SQLiteAuthStore | None = None,
        cookie_name: str = "lab_link_session",
        session_ttl: float = 12 * 60 * 60,
        remembered_session_ttl: float = 30 * 24 * 60 * 60,
        invite_ttl: float = 5 * 60,
        trust_loopback: bool = True,
        allowed_origins: set[str] | None = None,
        max_failures: int = 5,
        failure_window: float = 60,
        passphrase_capabilities: set[str] | None = None,
        invite_capabilities: set[str] | None = None,
    ) -> None:
        self.store = store
        self.cookie_name = cookie_name
        self.session_ttl = session_ttl
        self.remembered_session_ttl = remembered_session_ttl
        self.invite_ttl = invite_ttl
        self.trust_loopback = trust_loopback
        self.allowed_origins = {
            origin.rstrip("/") for origin in allowed_origins or set()
        }
        self.max_failures = max_failures
        self.failure_window = failure_window
        self.passphrase_capabilities = frozenset(
            passphrase_capabilities or {"control", "manage_access"}
        )
        self.invite_capabilities = frozenset(invite_capabilities or {"control"})

        self._hasher = PasswordHasher()
        self._display_passphrase: str | None = None
        self._passphrase_hash: str | None = (
            self.store.get_passphrase_hash() if self.store else None
        )
        if self.store is None:
            configured = passphrase or self.generate_passphrase()
            self._set_passphrase(configured, retain_for_display=True)
        elif self._passphrase_hash is None and passphrase:
            self._set_passphrase(passphrase, retain_for_display=True)
        elif self._passphrase_hash is not None and passphrase:
            if not self.verify_passphrase(passphrase):
                raise ValueError(
                    "configured passphrase does not match the persistent auth store"
                )
            self._display_passphrase = passphrase

        self._sessions: dict[str, StoredSession] = {}
        self._api_tokens: dict[str, StoredApiToken] = {}
        self._invites_by_hash: dict[str, _InviteRecord] = {}
        self._invites_by_id: dict[str, _InviteRecord] = {}
        self._invite_timers: dict[str, asyncio.TimerHandle] = {}
        self._invite_callbacks: set[InviteCallback] = set()
        self._failures: dict[str, list[float]] = {}
        self._lock = threading.RLock()

    @property
    def configured(self) -> bool:
        return self._passphrase_hash is not None

    @property
    def passphrase(self) -> str | None:
        """Readable bootstrap passphrase when one is available.

        Persistent passphrases created through first-run setup are deliberately
        not recoverable from their stored hash. Applications should display a
        user-entered value at setup time or rely on a password manager.
        """
        return self._display_passphrase

    @staticmethod
    def generate_passphrase() -> str:
        alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
        groups = ["".join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3)]
        return "-".join(groups)

    def setup_passphrase(self, passphrase: str) -> None:
        if self.configured:
            raise RuntimeError("authentication is already configured")
        self._set_passphrase(passphrase, retain_for_display=False)

    def rotate_passphrase(
        self,
        passphrase: str,
        *,
        revoke_sessions: bool = True,
        revoke_invites: bool = True,
    ) -> None:
        self._set_passphrase(passphrase, retain_for_display=False)
        if revoke_sessions:
            self.revoke_all_sessions()
        if revoke_invites:
            self.revoke_all_invites()

    def verify_passphrase(self, supplied: str) -> bool:
        if self._passphrase_hash is None or not supplied:
            return False
        try:
            valid = self._hasher.verify(self._passphrase_hash, supplied)
            if valid and self._hasher.check_needs_rehash(self._passphrase_hash):
                self._set_passphrase(supplied, retain_for_display=False)
            return bool(valid)
        except (VerifyMismatchError, VerificationError):
            return False

    def create_invite(self, *, ttl: float | None = None) -> AccessInvite:
        lifetime = self.invite_ttl if ttl is None else ttl
        if lifetime <= 0:
            raise ValueError("invite ttl must be greater than zero")
        token = secrets.token_urlsafe(32)
        invite_id = secrets.token_hex(12)
        expires_at = time.time() + lifetime
        record = _InviteRecord(
            id=invite_id,
            token_hash=self._digest(token),
            expires_at=expires_at,
        )
        with self._lock:
            self._prune_locked()
            self._invites_by_hash[record.token_hash] = record
            self._invites_by_id[record.id] = record
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                pass
            else:
                self._invite_timers[record.id] = loop.call_later(
                    lifetime, self._expire_invite, record.id
                )
        return AccessInvite(
            id=invite_id,
            token=token,
            expires_at=self._datetime(expires_at),
        )

    def invite_status(self, invite_id: str) -> InviteStatus | None:
        event: InviteEvent | None = None
        with self._lock:
            record = self._invites_by_id.get(invite_id)
            if record is None:
                return None
            if record.status == "active" and record.expires_at <= time.time():
                record.status = "expired"
                event = self._invite_event(record)
            status = record.status
        if event:
            self._emit_invite(event)
        return status

    def revoke_invite(self, invite_id: str) -> bool:
        with self._lock:
            record = self._invites_by_id.get(invite_id)
            if record is None or record.status != "active":
                return False
            record.status = "revoked"
            self._cancel_invite_timer_locked(invite_id)
            event = self._invite_event(record)
        self._emit_invite(event)
        return True

    def revoke_all_invites(self) -> None:
        events: list[InviteEvent] = []
        with self._lock:
            for record in self._invites_by_id.values():
                if record.status == "active":
                    record.status = "revoked"
                    self._cancel_invite_timer_locked(record.id)
                    events.append(self._invite_event(record))
        for event in events:
            self._emit_invite(event)

    def on_invite_event(self, callback: InviteCallback) -> Callable[[], None]:
        self._invite_callbacks.add(callback)
        return lambda: self._invite_callbacks.discard(callback)

    def create_api_token(
        self,
        label: str,
        *,
        capabilities: set[str] | frozenset[str],
        ttl: float | None = None,
    ) -> ApiTokenCredential:
        if not label.strip():
            raise ValueError("API token label is required")
        if ttl is not None and ttl <= 0:
            raise ValueError("API token ttl must be greater than zero")
        raw = f"ll_{secrets.token_urlsafe(32)}"
        now = time.time()
        expires_at = now + ttl if ttl is not None else None
        record = StoredApiToken(
            id=secrets.token_hex(12),
            token_hash=self._digest(raw),
            label=label.strip(),
            created_at=now,
            last_used_at=None,
            expires_at=expires_at,
            capabilities=frozenset(capabilities),
        )
        with self._lock:
            self._api_tokens[record.token_hash] = record
            if self.store:
                self.store.save_api_token(record)
        return ApiTokenCredential(
            id=record.id,
            token=raw,
            label=record.label,
            created_at=self._datetime(now),
            expires_at=self._datetime(expires_at) if expires_at else None,
            capabilities=record.capabilities,
        )

    def list_api_tokens(self) -> list[dict[str, Any]]:
        records = {record.id: record for record in self._api_tokens.values()}
        if self.store:
            records.update(
                {record.id: record for record in self.store.list_api_tokens()}
            )
        now = time.time()
        return [
            self._api_token_dict(record)
            for record in records.values()
            if record.expires_at is None or record.expires_at > now
        ]

    def revoke_api_token(self, token_id: str) -> bool:
        found = False
        with self._lock:
            for token_hash, record in list(self._api_tokens.items()):
                if record.id == token_id:
                    self._api_tokens.pop(token_hash, None)
                    found = True
            if self.store:
                found = found or any(
                    record.id == token_id for record in self.store.list_api_tokens()
                )
                self.store.delete_api_token(token_id)
        return found

    def revoke_all_api_tokens(self) -> None:
        with self._lock:
            self._api_tokens.clear()
            if self.store:
                self.store.delete_all_api_tokens()

    def list_sessions(self) -> list[SessionInfo]:
        records = {record.id: record for record in self._sessions.values()}
        if self.store:
            self.store.prune_sessions(time.time())
            records.update({record.id: record for record in self.store.list_sessions()})
        return [self._session_info(record) for record in records.values()]

    def revoke_session(self, session_id: str) -> bool:
        found = False
        with self._lock:
            for token_hash, record in list(self._sessions.items()):
                if record.id == session_id:
                    self._sessions.pop(token_hash, None)
                    found = True
            if self.store:
                found = found or any(
                    record.id == session_id for record in self.store.list_sessions()
                )
                self.store.delete_session(session_id)
        return found

    def revoke_all_sessions(self) -> None:
        with self._lock:
            self._sessions.clear()
            if self.store:
                self.store.delete_all_sessions()

    def principal_for_http(self, request: Request) -> AuthPrincipal | None:
        return self._principal_for_connection(request, check_origin=False)

    def principal_for_websocket(self, websocket: WebSocket) -> AuthPrincipal | None:
        return self._principal_for_connection(websocket, check_origin=True)

    def is_http_authorized(self, request: Request) -> bool:
        return self.principal_for_http(request) is not None

    def is_websocket_authorized(self, websocket: WebSocket) -> bool:
        return self.principal_for_websocket(websocket) is not None

    def routes(self, prefix: str) -> list[BaseRoute]:
        base = prefix.rstrip("/")
        return [
            Route(f"{base}/auth/status", self._status, methods=["GET"]),
            Route(f"{base}/auth/setup", self._setup, methods=["POST"]),
            Route(f"{base}/auth/login", self._login, methods=["POST"]),
            Route(f"{base}/auth/invite", self._exchange_invite, methods=["POST"]),
            Route(f"{base}/auth/logout", self._logout, methods=["POST"]),
            Route(f"{base}/auth/passphrase", self._change_passphrase, methods=["POST"]),
            Route(f"{base}/auth/sessions", self._sessions_route, methods=["GET"]),
            Route(
                f"{base}/auth/sessions/revoke",
                self._revoke_session_route,
                methods=["POST"],
            ),
            Route(
                f"{base}/auth/sessions/revoke-all",
                self._revoke_all_sessions_route,
                methods=["POST"],
            ),
            Route(f"{base}/auth/invites", self._create_invite_route, methods=["POST"]),
            Route(
                f"{base}/auth/invites/revoke",
                self._revoke_invite_route,
                methods=["POST"],
            ),
            Route(f"{base}/auth/tokens", self._tokens_route, methods=["GET", "POST"]),
            Route(
                f"{base}/auth/tokens/revoke", self._revoke_token_route, methods=["POST"]
            ),
            Route(
                f"{base}/auth/tokens/revoke-all",
                self._revoke_all_tokens_route,
                methods=["POST"],
            ),
        ]

    async def _status(self, request: Request) -> JSONResponse:
        principal = self.principal_for_http(request)
        return JSONResponse(
            {
                "configured": self.configured,
                "authorized": principal is not None,
                "principal": self._principal_dict(principal) if principal else None,
            }
        )

    async def _setup(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        if not self._is_loopback(self._client_host(request)):
            return self._error("local_setup_required", 403)
        if self.configured:
            return self._error("already_configured", 409)
        values = await self._values(request)
        try:
            self.setup_passphrase(str(values.get("passphrase", "")))
        except ValueError as exc:
            return self._error("weak_passphrase", 400, str(exc))
        return self._session_response(
            request,
            values,
            auth_method="setup",
            capabilities=self.passphrase_capabilities,
        )

    async def _login(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        if not self.configured:
            return self._error("setup_required", 503)
        client_host = self._client_host(request) or "unknown"
        if self._is_rate_limited(client_host):
            return self._error("rate_limited", 429)
        values = await self._values(request)
        if not self.verify_passphrase(str(values.get("passphrase", ""))):
            self._record_failure(client_host)
            return self._error("invalid_credentials", 401)
        self._clear_failures(client_host)
        return self._session_response(
            request,
            values,
            auth_method="passphrase",
            capabilities=self.passphrase_capabilities,
        )

    async def _exchange_invite(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        values = await self._values(request)
        token = str(values.get("invite", values.get("token", "")))
        if not self._consume_invite(token):
            return self._error("invalid_or_expired_invite", 401)
        return self._session_response(
            request,
            values,
            auth_method="invite",
            capabilities=self.invite_capabilities,
        )

    async def _logout(self, request: Request) -> Response:
        if not self._origin_allowed(request.headers):
            return self._error("origin_not_allowed", 403)
        token = request.cookies.get(self.cookie_name, "")
        if token:
            self._delete_session_token(self._digest(token))
        response = JSONResponse({"configured": self.configured, "authorized": False})
        response.delete_cookie(self.cookie_name, path="/")
        return response

    async def _change_passphrase(self, request: Request) -> Response:
        principal = self._require(request, "manage_access")
        if principal is None:
            return self._error("forbidden", 403)
        values = await self._values(request)
        try:
            self.rotate_passphrase(
                str(values.get("passphrase", "")),
                revoke_sessions=bool(values.get("revokeSessions", True)),
                revoke_invites=bool(values.get("revokeInvites", True)),
            )
        except ValueError as exc:
            return self._error("weak_passphrase", 400, str(exc))
        return JSONResponse({"changed": True})

    async def _sessions_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        return JSONResponse(
            {"sessions": [self._session_dict(x) for x in self.list_sessions()]}
        )

    async def _revoke_session_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        values = await self._values(request)
        return JSONResponse({"revoked": self.revoke_session(str(values.get("id", "")))})

    async def _revoke_all_sessions_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        self.revoke_all_sessions()
        return JSONResponse({"revoked": True})

    async def _create_invite_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        values = await self._values(request)
        ttl_value = values.get("ttl")
        try:
            invite = self.create_invite(
                ttl=float(ttl_value) if ttl_value else None
            )
        except (TypeError, ValueError) as exc:
            return self._error("invalid_invite", 400, str(exc))
        return JSONResponse(self._invite_dict(invite))

    async def _revoke_invite_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        values = await self._values(request)
        return JSONResponse({"revoked": self.revoke_invite(str(values.get("id", "")))})

    async def _tokens_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        if request.method == "GET":
            return JSONResponse({"tokens": self.list_api_tokens()})
        values = await self._values(request)
        capabilities = {
            str(value) for value in values.get("capabilities", []) if str(value)
        }
        ttl_value = values.get("ttl")
        try:
            token = self.create_api_token(
                str(values.get("label", "")),
                capabilities=capabilities,
                ttl=float(ttl_value) if ttl_value else None,
            )
        except (TypeError, ValueError) as exc:
            return self._error("invalid_token", 400, str(exc))
        return JSONResponse(self._api_token_credential_dict(token))

    async def _revoke_token_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        values = await self._values(request)
        return JSONResponse(
            {"revoked": self.revoke_api_token(str(values.get("id", "")))}
        )

    async def _revoke_all_tokens_route(self, request: Request) -> Response:
        if self._require(request, "manage_access") is None:
            return self._error("forbidden", 403)
        self.revoke_all_api_tokens()
        return JSONResponse({"revoked": True})

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

    def _session_response(
        self,
        request: Request,
        values: dict[str, Any],
        *,
        auth_method: str,
        capabilities: frozenset[str],
    ) -> JSONResponse:
        remembered = bool(values.get("remember", False))
        ttl = self.remembered_session_ttl if remembered else self.session_ttl
        raw = secrets.token_urlsafe(32)
        now = time.time()
        record = StoredSession(
            id=secrets.token_hex(12),
            token_hash=self._digest(raw),
            label=str(values.get("deviceName", "")).strip() or "Browser",
            created_at=now,
            last_used_at=now,
            expires_at=now + ttl,
            remembered=remembered,
            auth_method=auth_method,
            capabilities=capabilities,
        )
        with self._lock:
            self._sessions[record.token_hash] = record
            if remembered and self.store:
                self.store.save_session(record)
        response = JSONResponse(
            {
                "configured": self.configured,
                "authorized": True,
                "session": self._session_dict(self._session_info(record)),
            }
        )
        response.set_cookie(
            self.cookie_name,
            raw,
            max_age=max(1, int(ttl)),
            httponly=True,
            secure=request.url.scheme == "https",
            samesite="strict",
            path="/",
        )
        return response

    def _principal_for_connection(
        self, connection: Any, *, check_origin: bool
    ) -> AuthPrincipal | None:
        if check_origin and not self._origin_allowed(connection.headers):
            return None
        host = self._client_host(connection)
        if self.trust_loopback and self._is_loopback(host):
            return AuthPrincipal(
                id="local",
                kind="local",
                label="Local host",
                capabilities=frozenset({"*"}),
            )
        bearer = self._bearer_token(connection.headers)
        if bearer:
            principal = self._principal_for_api_token(bearer)
            if principal:
                return principal
        cookie = connection.cookies.get(self.cookie_name, "")
        return self._principal_for_session(cookie) if cookie else None

    def _principal_for_session(self, raw: str) -> AuthPrincipal | None:
        token_hash = self._digest(raw)
        now = time.time()
        with self._lock:
            record = self._sessions.get(token_hash)
            if self.store and (record is None or record.remembered):
                persisted = self.store.get_session(token_hash)
                if persisted is None and record is not None and record.remembered:
                    self._sessions.pop(token_hash, None)
                    return None
                if persisted is not None:
                    record = persisted
                    self._sessions[token_hash] = persisted
            if record is None:
                return None
            if record.expires_at <= now:
                self._delete_session_token(token_hash)
                return None
            if now - record.last_used_at >= 60:
                updated = StoredSession(
                    id=record.id,
                    token_hash=record.token_hash,
                    label=record.label,
                    created_at=record.created_at,
                    last_used_at=now,
                    expires_at=record.expires_at,
                    remembered=record.remembered,
                    auth_method=record.auth_method,
                    capabilities=record.capabilities,
                )
                self._sessions[token_hash] = updated
                record = updated
                if record.remembered and self.store:
                    self.store.touch_session(record.id, now)
        return AuthPrincipal(
            id=record.id,
            kind="session",
            label=record.label,
            capabilities=record.capabilities,
            session_id=record.id,
        )

    def _principal_for_api_token(self, raw: str) -> AuthPrincipal | None:
        token_hash = self._digest(raw)
        now = time.time()
        with self._lock:
            record = self._api_tokens.get(token_hash)
            if self.store:
                record = self.store.get_api_token(token_hash)
                if record:
                    self._api_tokens[token_hash] = record
                else:
                    self._api_tokens.pop(token_hash, None)
            if record is None or (
                record.expires_at is not None and record.expires_at <= now
            ):
                return None
            if record.last_used_at is None or now - record.last_used_at >= 60:
                updated = StoredApiToken(
                    id=record.id,
                    token_hash=record.token_hash,
                    label=record.label,
                    created_at=record.created_at,
                    last_used_at=now,
                    expires_at=record.expires_at,
                    capabilities=record.capabilities,
                )
                self._api_tokens[token_hash] = updated
                if self.store:
                    self.store.touch_api_token(record.id, now)
                record = updated
        return AuthPrincipal(
            id=record.id,
            kind="api_token",
            label=record.label,
            capabilities=record.capabilities,
        )

    def _consume_invite(self, token: str) -> bool:
        if not token:
            return False
        with self._lock:
            record = self._invites_by_hash.get(self._digest(token))
            if record is None or record.status != "active":
                return False
            if record.expires_at <= time.time():
                record.status = "expired"
                event = self._invite_event(record)
                accepted = False
            else:
                record.status = "consumed"
                event = self._invite_event(record)
                accepted = True
            self._cancel_invite_timer_locked(record.id)
        self._emit_invite(event)
        return accepted

    def _expire_invite(self, invite_id: str) -> None:
        with self._lock:
            record = self._invites_by_id.get(invite_id)
            if record is None or record.status != "active":
                return
            record.status = "expired"
            self._invite_timers.pop(invite_id, None)
            event = self._invite_event(record)
        self._emit_invite(event)

    def _emit_invite(self, event: InviteEvent) -> None:
        for callback in list(self._invite_callbacks):
            try:
                callback(event)
            except Exception:
                logger.exception("Unhandled invitation lifecycle callback")

    def _set_passphrase(self, passphrase: str, *, retain_for_display: bool) -> None:
        self._validate_passphrase(passphrase)
        value = self._hasher.hash(passphrase)
        self._passphrase_hash = value
        self._display_passphrase = passphrase if retain_for_display else None
        if self.store:
            self.store.set_passphrase_hash(value)

    @staticmethod
    def _validate_passphrase(passphrase: str) -> None:
        if len(passphrase) < 12:
            raise ValueError("passphrase must contain at least 12 characters")

    def _require(self, request: Request, capability: str) -> AuthPrincipal | None:
        if not self._origin_allowed(request.headers):
            return None
        principal = self.principal_for_http(request)
        return principal if principal and principal.can(capability) else None

    def _delete_session_token(self, token_hash: str) -> None:
        with self._lock:
            self._sessions.pop(token_hash, None)
            if self.store:
                self.store.delete_session_by_hash(token_hash)

    def _prune_locked(self) -> None:
        now = time.time()
        for token_hash, record in list(self._sessions.items()):
            if record.expires_at <= now:
                self._sessions.pop(token_hash, None)
        if self.store:
            self.store.prune_sessions(now)
            self.store.prune_api_tokens(now)

    def _cancel_invite_timer_locked(self, invite_id: str) -> None:
        timer = self._invite_timers.pop(invite_id, None)
        if timer:
            timer.cancel()

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
    def _is_loopback(host: str | None) -> bool:
        if not host:
            return False
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return host.lower() == "localhost"

    @staticmethod
    def _bearer_token(headers: Any) -> str | None:
        value = headers.get("authorization", "")
        scheme, _, token = value.partition(" ")
        return token if scheme.lower() == "bearer" and token else None

    @staticmethod
    def _digest(token: str) -> str:
        return hashlib.sha256(token.encode()).hexdigest()

    @staticmethod
    def _datetime(timestamp: float) -> datetime:
        return datetime.fromtimestamp(timestamp, timezone.utc)

    def _invite_event(self, record: _InviteRecord) -> InviteEvent:
        return InviteEvent(record.id, record.status, datetime.now(timezone.utc))

    @staticmethod
    def _principal_dict(principal: AuthPrincipal) -> dict[str, Any]:
        return {
            "id": principal.id,
            "kind": principal.kind,
            "label": principal.label,
            "capabilities": sorted(principal.capabilities),
            "sessionId": principal.session_id,
        }

    def _session_info(self, record: StoredSession) -> SessionInfo:
        return SessionInfo(
            id=record.id,
            label=record.label,
            created_at=self._datetime(record.created_at),
            last_used_at=self._datetime(record.last_used_at),
            expires_at=self._datetime(record.expires_at),
            remembered=record.remembered,
            auth_method=record.auth_method,
            capabilities=record.capabilities,
        )

    @staticmethod
    def _session_dict(session: SessionInfo) -> dict[str, Any]:
        return {
            "id": session.id,
            "label": session.label,
            "createdAt": session.created_at.isoformat(),
            "lastUsedAt": session.last_used_at.isoformat(),
            "expiresAt": session.expires_at.isoformat(),
            "remembered": session.remembered,
            "authMethod": session.auth_method,
            "capabilities": sorted(session.capabilities),
        }

    @staticmethod
    def _invite_dict(invite: AccessInvite) -> dict[str, Any]:
        return {
            "id": invite.id,
            "token": invite.token,
            "expiresAt": invite.expires_at.isoformat(),
            "status": invite.status,
        }

    @staticmethod
    def _api_token_dict(record: StoredApiToken) -> dict[str, Any]:
        return {
            "id": record.id,
            "label": record.label,
            "createdAt": LanPassphraseAuth._datetime(record.created_at).isoformat(),
            "lastUsedAt": (
                LanPassphraseAuth._datetime(record.last_used_at).isoformat()
                if record.last_used_at
                else None
            ),
            "expiresAt": (
                LanPassphraseAuth._datetime(record.expires_at).isoformat()
                if record.expires_at
                else None
            ),
            "capabilities": sorted(record.capabilities),
        }

    @staticmethod
    def _api_token_credential_dict(token: ApiTokenCredential) -> dict[str, Any]:
        return {
            "id": token.id,
            "token": token.token,
            "label": token.label,
            "createdAt": token.created_at.isoformat(),
            "expiresAt": token.expires_at.isoformat() if token.expires_at else None,
            "capabilities": sorted(token.capabilities),
        }

    @staticmethod
    def _error(code: str, status_code: int, message: str | None = None) -> JSONResponse:
        payload: dict[str, Any] = {"authorized": False, "error": code}
        if message:
            payload["message"] = message
        return JSONResponse(payload, status_code=status_code)
