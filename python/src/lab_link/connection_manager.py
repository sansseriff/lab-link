from __future__ import annotations

import asyncio
import uuid
from typing import Any

from fastapi import WebSocket


class ConnectionManager:
    def __init__(self) -> None:
        self.connections: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        client_id: str,
        state_snapshot: dict[str, Any],
        state_version: int,
        stream_snapshots: list[dict[str, Any]] | None = None,
    ) -> None:
        await websocket.accept()
        async with self._lock:
            self.connections[client_id] = websocket
        await websocket.send_json(
            {"type": "snapshot", "data": state_snapshot, "version": state_version}
        )
        for msg in (stream_snapshots or []):
            await websocket.send_json(msg)

    async def disconnect(self, client_id: str) -> None:
        async with self._lock:
            self.connections.pop(client_id, None)

    async def broadcast_patch(
        self,
        patch: list[dict[str, Any]],
        version: int,
        *,
        origin_client_id: str | None = None,
        request_id: str | None = None,
        command: str | None = None,
    ) -> None:
        message = {"type": "patch", "patch": patch, "version": version}
        if origin_client_id is not None:
            message["originClientId"] = origin_client_id
        if request_id is not None:
            message["requestId"] = request_id
        if command is not None:
            message["command"] = command
        await self._broadcast_json(message)

    async def broadcast_json(self, message: dict[str, Any]) -> None:
        await self._broadcast_json(message)

    async def broadcast_binary(self, frame: bytes) -> None:
        async with self._lock:
            items = list(self.connections.items())

        disconnected: list[str] = []
        for client_id, websocket in items:
            try:
                await websocket.send_bytes(frame)
            except Exception:
                disconnected.append(client_id)

        if disconnected:
            async with self._lock:
                for cid in disconnected:
                    self.connections.pop(cid, None)

    async def send_to(self, client_id: str, message: dict[str, Any]) -> None:
        async with self._lock:
            ws = self.connections.get(client_id)
        if ws:
            try:
                await ws.send_json(message)
            except Exception:
                async with self._lock:
                    self.connections.pop(client_id, None)

    async def close_all(self) -> None:
        async with self._lock:
            items = list(self.connections.items())
            self.connections.clear()

        for _, websocket in items:
            try:
                await websocket.close()
            except Exception:
                pass

    async def _broadcast_json(self, message: dict[str, Any]) -> None:
        async with self._lock:
            items = list(self.connections.items())

        disconnected: list[str] = []
        for client_id, websocket in items:
            try:
                await websocket.send_json(message)
            except Exception:
                disconnected.append(client_id)

        if disconnected:
            async with self._lock:
                for cid in disconnected:
                    self.connections.pop(cid, None)

    @staticmethod
    def generate_client_id() -> str:
        return str(uuid.uuid4())
