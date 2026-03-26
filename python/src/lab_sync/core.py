from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from inspect import iscoroutinefunction
from typing import Any, Callable, Literal, TypeVar

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect

from .connection_manager import ConnectionManager
from .persistence import PersistenceManager
from .proxy import StateProxy, SyncState
from .state_store import StateStore
from .stream_buffer import AppendBuffer, DeltaBuffer, ReplaceBuffer, StreamRef

T = TypeVar("T")
_F = TypeVar("_F", bound=Callable[..., Any])


class LabSync:
    def __init__(
        self,
        prefix: str = "/sync",
        persist: bool = False,
        db_url: str = "sqlite:///lab_sync.db",
        compress: bool = False,
    ) -> None:
        self._prefix = prefix.rstrip("/")
        self._persist = persist
        self._db_url = db_url
        self._compress = compress

        self._store: StateStore | None = None
        self._commands: dict[str, Callable[..., Any]] = {}
        self._updaters: list[tuple[Callable[..., Any], float]] = []
        self._streams: dict[str, StreamRef] = {}
        self._live_buffers: dict[str, AppendBuffer | ReplaceBuffer | DeltaBuffer] = {}
        self._conn_manager: ConnectionManager | None = None
        self._persistence: PersistenceManager | None = None
        self._patch_queue: asyncio.Queue | None = None
        self._router: APIRouter | None = None

        # ``sync.state`` is always a SyncState instance.
        # Before @sync.state is applied it acts as the decorator (callable).
        # After registration it delegates attribute access to the internal StateProxy.
        self.state = SyncState(self._register_state_model)

    # ── model registration ───────────────────────────────────────────────────

    def _register_state_model(self, cls: type) -> None:
        from pydantic import BaseModel
        if not issubclass(cls, BaseModel):
            raise TypeError(f"{cls.__name__} must be a pydantic BaseModel subclass")
        initial = cls().model_dump(mode="json")
        self._store = StateStore(cls, initial)
        self._patch_queue = asyncio.Queue()
        self.state._set_proxy(StateProxy(self._store, self._patch_queue))

    # ── decorators ──────────────────────────────────────────────────────────

    def command(self, fn: _F) -> _F:
        """@sync.command — registers fn under fn.__name__. Supports sync & async."""
        self._commands[fn.__name__] = fn
        return fn

    def updater(self, interval: float = 1.0) -> Callable[[_F], _F]:
        """@sync.updater(interval=0.1) — registers a background polling coroutine."""
        def decorator(fn: _F) -> _F:
            self._updaters.append((fn, interval))
            return fn
        return decorator

    # ── stream registration ──────────────────────────────────────────────────

    def stream(
        self,
        id: str,
        *,
        mode: Literal["append", "replace", "int_delta"] = "replace",
        capacity: int = 10_000,
        dtype: Literal["float32", "float64", "json"] = "float32",
    ) -> StreamRef:
        """Register a named stream and return a StreamRef.

        The ref is safe to store at module level and use in updaters — it
        materialises the real buffer automatically when the app lifespan starts.
        """
        ref = StreamRef(id, mode, capacity, dtype)
        self._streams[id] = ref
        if self._conn_manager is not None:
            # Already inside lifespan — materialise immediately
            buf = self._make_buffer(id, mode, capacity, dtype)
            ref.materialize(buf)
            self._live_buffers[id] = buf
        return ref

    def _make_buffer(
        self, id: str, mode: str, capacity: int, dtype: str
    ) -> AppendBuffer | ReplaceBuffer | DeltaBuffer:
        cm = self._conn_manager
        if mode == "append":
            return AppendBuffer(id, capacity, cm)
        elif mode == "replace":
            return ReplaceBuffer(id, capacity, dtype, cm)
        elif mode == "int_delta":
            return DeltaBuffer(id, capacity, cm)
        else:
            raise ValueError(f"Unknown stream mode: {mode!r}")

    # ── state access ─────────────────────────────────────────────────────────

    def get(self, path: str) -> Any:
        """Read helper: sync.get('pump/speed') → scalar value."""
        if self._store is None:
            raise RuntimeError("No @sync.state model registered")
        return self._store.get(path)

    @property
    def streams(self) -> dict[str, AppendBuffer | ReplaceBuffer | DeltaBuffer]:
        return self._live_buffers

    # ── FastAPI integration ───────────────────────────────────────────────────

    @property
    def router(self) -> APIRouter:
        if self._router is not None:
            return self._router
        router = APIRouter(prefix=self._prefix)

        @router.get("/state")
        async def get_state() -> dict[str, Any]:
            if self._store is None:
                return {}
            return self._store.snapshot()

        @router.websocket("/ws")
        async def ws_endpoint(websocket: WebSocket) -> None:
            await self._handle_ws(websocket)

        self._router = router
        return router

    @asynccontextmanager
    async def lifespan(self, app: FastAPI | None = None):
        """Use in FastAPI lifespan to start drain task, updaters, persistence."""
        self._conn_manager = ConnectionManager()

        # Materialise all StreamRefs now that we have a conn_manager
        for sid, ref in self._streams.items():
            buf = self._make_buffer(sid, ref.mode, ref.capacity, ref.dtype)
            ref.materialize(buf)
            self._live_buffers[sid] = buf

        # Recreate queue in running loop and rebind proxy
        self._patch_queue = asyncio.Queue()
        proxy = self.state._get_proxy()
        if proxy is not None:
            proxy._rebind_queue(self._patch_queue)

        # Optional persistence
        if self._persist and self._store is not None:
            self._persistence = PersistenceManager(self._db_url)
            saved = self._persistence.initialize()
            if saved:
                try:
                    self._store.replace_state(saved)
                except Exception:
                    pass

        tasks: list[asyncio.Task[None]] = []

        if self._store is not None:
            tasks.append(
                asyncio.create_task(
                    _drain_patch_queue(
                        self._patch_queue,
                        self._store,
                        self._conn_manager,
                        self._persistence,
                    )
                )
            )

        for fn, interval in self._updaters:
            tasks.append(asyncio.create_task(_run_updater(fn, interval)))

        yield

        for task in tasks:
            task.cancel()
        for task in tasks:
            try:
                await task
            except asyncio.CancelledError:
                pass

        if self._persistence and self._store:
            self._persistence.save_sync(self._store.snapshot())

        await self._conn_manager.close_all()

    def create_app(self, **fastapi_kwargs: Any) -> FastAPI:
        """Convenience: creates FastAPI app with lifespan + router pre-wired."""
        @asynccontextmanager
        async def _lifespan(app: FastAPI):
            async with self.lifespan(app):
                yield

        app = FastAPI(lifespan=_lifespan, **fastapi_kwargs)
        app.include_router(self.router)
        return app

    # ── internal WebSocket handler ────────────────────────────────────────────

    async def _handle_ws(self, websocket: WebSocket) -> None:
        client_id = ConnectionManager.generate_client_id()
        snapshot = self._store.snapshot() if self._store else {}
        version = self._store.version() if self._store else 0
        stream_snapshots = [
            buf.snapshot_message()
            for buf in self._live_buffers.values()
        ]
        await self._conn_manager.connect(
            websocket, client_id, snapshot, version, stream_snapshots
        )
        try:
            while True:
                data = await websocket.receive_json()
                msg_type = data.get("type")
                if msg_type == "command":
                    await self._dispatch_command(
                        websocket=websocket,
                        command=str(data.get("command", "")),
                        params=dict(data.get("params") or {}),
                        request_id=data.get("requestId"),
                    )
                elif msg_type == "stream_resync":
                    stream_id = data.get("id")
                    buf = self._live_buffers.get(stream_id)
                    if buf:
                        await self._conn_manager.send_to(client_id, buf.snapshot_message())
        except WebSocketDisconnect:
            pass
        except Exception:
            pass
        finally:
            await self._conn_manager.disconnect(client_id)

    async def _dispatch_command(
        self,
        websocket: WebSocket,
        command: str,
        params: dict[str, Any],
        request_id: str | None,
    ) -> None:
        handler = self._commands.get(command)
        if handler is None:
            if request_id:
                await websocket.send_json(
                    {
                        "type": "command_error",
                        "command": command,
                        "requestId": request_id,
                        "error": f"Unknown command: {command!r}",
                    }
                )
            return

        try:
            if iscoroutinefunction(handler):
                await handler(**params)
            else:
                handler(**params)

            if self._patch_queue is not None:
                await self._patch_queue.join()

            if request_id:
                version = self._store.version() if self._store else 0
                await websocket.send_json(
                    {
                        "type": "command_ack",
                        "command": command,
                        "requestId": request_id,
                        "version": version,
                    }
                )
        except Exception as exc:
            if request_id:
                await websocket.send_json(
                    {
                        "type": "command_error",
                        "command": command,
                        "requestId": request_id,
                        "error": str(exc),
                    }
                )


# ── background tasks ──────────────────────────────────────────────────────────

async def _drain_patch_queue(
    queue: asyncio.Queue,
    store: StateStore,
    conn_manager: ConnectionManager,
    persistence: PersistenceManager | None,
) -> None:
    while True:
        path, value = await queue.get()
        try:
            patch, version = store.apply_value(path, value)
            await conn_manager.broadcast_patch(patch, version)
            if persistence:
                await persistence.save_debounced(store.snapshot())
        finally:
            queue.task_done()


async def _run_updater(fn: Callable[..., Any], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        if iscoroutinefunction(fn):
            await fn()
        else:
            fn()
