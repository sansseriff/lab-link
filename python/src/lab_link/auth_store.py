from __future__ import annotations

import json
import os
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class StoredSession:
    id: str
    token_hash: str
    label: str
    created_at: float
    last_used_at: float
    expires_at: float
    remembered: bool
    auth_method: str
    capabilities: frozenset[str]


@dataclass(frozen=True, slots=True)
class StoredApiToken:
    id: str
    token_hash: str
    label: str
    created_at: float
    last_used_at: float | None
    expires_at: float | None
    capabilities: frozenset[str]


class SQLiteAuthStore:
    """Persistent credential, remembered-session, and API-token storage.

    Passphrases are already Argon2id hashes when they reach this class. Session
    and API-token values are high-entropy secrets and are stored only as SHA-256
    digests. The database never contains a credential that can be presented to
    the server directly.
    """

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self) -> None:
        existed = self.path.exists()
        with self._lock, self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(
                """
                CREATE TABLE IF NOT EXISTS auth_metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS auth_sessions (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT UNIQUE NOT NULL,
                    label TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    last_used_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    remembered INTEGER NOT NULL,
                    auth_method TEXT NOT NULL,
                    capabilities TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS auth_sessions_token_hash
                    ON auth_sessions(token_hash);
                CREATE TABLE IF NOT EXISTS auth_api_tokens (
                    id TEXT PRIMARY KEY,
                    token_hash TEXT UNIQUE NOT NULL,
                    label TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    last_used_at REAL,
                    expires_at REAL,
                    capabilities TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS auth_api_tokens_token_hash
                    ON auth_api_tokens(token_hash);
                """
            )
        if not existed:
            os.chmod(self.path, 0o600)

    def get_passphrase_hash(self) -> str | None:
        with self._lock, self._connect() as db:
            row = db.execute(
                "SELECT value FROM auth_metadata WHERE key = 'passphrase_hash'"
            ).fetchone()
        return str(row["value"]) if row else None

    def set_passphrase_hash(self, value: str) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """INSERT INTO auth_metadata(key, value)
                   VALUES ('passphrase_hash', ?)
                   ON CONFLICT(key) DO UPDATE SET value = excluded.value""",
                (value,),
            )

    def save_session(self, session: StoredSession) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """INSERT OR REPLACE INTO auth_sessions(
                       id, token_hash, label, created_at, last_used_at,
                       expires_at, remembered, auth_method, capabilities
                   ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    session.id,
                    session.token_hash,
                    session.label,
                    session.created_at,
                    session.last_used_at,
                    session.expires_at,
                    int(session.remembered),
                    session.auth_method,
                    json.dumps(sorted(session.capabilities)),
                ),
            )

    def get_session(self, token_hash: str) -> StoredSession | None:
        with self._lock, self._connect() as db:
            row = db.execute(
                "SELECT * FROM auth_sessions WHERE token_hash = ?", (token_hash,)
            ).fetchone()
        return self._session(row) if row else None

    def list_sessions(self) -> list[StoredSession]:
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT * FROM auth_sessions ORDER BY created_at DESC"
            ).fetchall()
        return [self._session(row) for row in rows]

    def touch_session(self, session_id: str, last_used_at: float) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE auth_sessions SET last_used_at = ? WHERE id = ?",
                (last_used_at, session_id),
            )

    def delete_session(self, session_id: str) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_sessions WHERE id = ?", (session_id,))

    def delete_session_by_hash(self, token_hash: str) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_sessions WHERE token_hash = ?", (token_hash,))

    def delete_all_sessions(self) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_sessions")

    def prune_sessions(self, now: float) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_sessions WHERE expires_at <= ?", (now,))

    def save_api_token(self, token: StoredApiToken) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                """INSERT OR REPLACE INTO auth_api_tokens(
                       id, token_hash, label, created_at, last_used_at,
                       expires_at, capabilities
                   ) VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    token.id,
                    token.token_hash,
                    token.label,
                    token.created_at,
                    token.last_used_at,
                    token.expires_at,
                    json.dumps(sorted(token.capabilities)),
                ),
            )

    def get_api_token(self, token_hash: str) -> StoredApiToken | None:
        with self._lock, self._connect() as db:
            row = db.execute(
                "SELECT * FROM auth_api_tokens WHERE token_hash = ?", (token_hash,)
            ).fetchone()
        return self._api_token(row) if row else None

    def list_api_tokens(self) -> list[StoredApiToken]:
        with self._lock, self._connect() as db:
            rows = db.execute(
                "SELECT * FROM auth_api_tokens ORDER BY created_at DESC"
            ).fetchall()
        return [self._api_token(row) for row in rows]

    def touch_api_token(self, token_id: str, last_used_at: float) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "UPDATE auth_api_tokens SET last_used_at = ? WHERE id = ?",
                (last_used_at, token_id),
            )

    def delete_api_token(self, token_id: str) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_api_tokens WHERE id = ?", (token_id,))

    def delete_all_api_tokens(self) -> None:
        with self._lock, self._connect() as db:
            db.execute("DELETE FROM auth_api_tokens")

    def prune_api_tokens(self, now: float) -> None:
        with self._lock, self._connect() as db:
            db.execute(
                "DELETE FROM auth_api_tokens WHERE expires_at IS NOT NULL AND expires_at <= ?",
                (now,),
            )

    @staticmethod
    def _session(row: sqlite3.Row) -> StoredSession:
        return StoredSession(
            id=str(row["id"]),
            token_hash=str(row["token_hash"]),
            label=str(row["label"]),
            created_at=float(row["created_at"]),
            last_used_at=float(row["last_used_at"]),
            expires_at=float(row["expires_at"]),
            remembered=bool(row["remembered"]),
            auth_method=str(row["auth_method"]),
            capabilities=frozenset(json.loads(row["capabilities"])),
        )

    @staticmethod
    def _api_token(row: sqlite3.Row) -> StoredApiToken:
        return StoredApiToken(
            id=str(row["id"]),
            token_hash=str(row["token_hash"]),
            label=str(row["label"]),
            created_at=float(row["created_at"]),
            last_used_at=(
                float(row["last_used_at"]) if row["last_used_at"] is not None else None
            ),
            expires_at=(
                float(row["expires_at"]) if row["expires_at"] is not None else None
            ),
            capabilities=frozenset(json.loads(row["capabilities"])),
        )
