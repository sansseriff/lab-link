from __future__ import annotations

import asyncio
import json
from typing import Any


class PersistenceManager:
    """Optional SQLite persistence using sqlmodel. Activated when LabSync(persist=True)."""

    def __init__(self, db_url: str = "sqlite:///lab_sync.db") -> None:
        self._db_url = db_url
        self._engine = None
        self._pending_save: asyncio.Task | None = None
        self._debounce_seconds = 1.0

    def initialize(self) -> dict[str, Any] | None:
        """Create tables, return persisted state dict if exists, else None."""
        from sqlmodel import Field, Session, SQLModel, create_engine, select

        class LabSyncState(SQLModel, table=True):
            id: int | None = Field(default=None, primary_key=True)
            state_json: str

        self._LabSyncState = LabSyncState
        self._engine = create_engine(self._db_url)
        SQLModel.metadata.create_all(self._engine)

        with Session(self._engine) as session:
            row = session.exec(select(LabSyncState)).first()
            if row:
                return json.loads(row.state_json)
        return None

    def save_sync(self, state: dict[str, Any]) -> None:
        from sqlmodel import Session, select

        LabSyncState = self._LabSyncState
        with Session(self._engine) as session:
            row = session.exec(select(LabSyncState)).first()
            if row:
                row.state_json = json.dumps(state)
            else:
                row = LabSyncState(state_json=json.dumps(state))
                session.add(row)
            session.commit()

    async def save_debounced(self, state: dict[str, Any]) -> None:
        """Coalesce rapid saves; only persist after debounce_seconds of inactivity."""
        if self._pending_save and not self._pending_save.done():
            self._pending_save.cancel()

        loop = asyncio.get_event_loop()
        self._pending_save = loop.create_task(self._debounced_save(state))

    async def _debounced_save(self, state: dict[str, Any]) -> None:
        await asyncio.sleep(self._debounce_seconds)
        self.save_sync(state)
