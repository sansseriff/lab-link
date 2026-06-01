from __future__ import annotations

import asyncio
import contextvars
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from inspect import Parameter, iscoroutinefunction, signature
from typing import Any, Callable, Literal, TypeVar

from fastapi import APIRouter, FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

from .connection_manager import ConnectionManager
from .errors import CommandError
from .persistence import PersistenceManager
from .proxy import StateProxy, SyncState
from .state_store import StateStore
from .stream_buffer import AppendBuffer, DeltaBuffer, ReplaceBuffer, StreamRef

T = TypeVar("T", bound=BaseModel)
_F = TypeVar("_F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CommandContext:
    client_id: str
    request_id: str | None
    command: str


@dataclass(frozen=True, slots=True)
class PatchMetadata:
    origin_client_id: str | None = None
    request_id: str | None = None
    command: str | None = None


_current_command_context: contextvars.ContextVar[CommandContext | None] = (
    contextvars.ContextVar("lab_link_command_context", default=None)
)


class StateTransaction:
    def __init__(self, sync: "LabSync", meta: PatchMetadata) -> None:
        self._sync = sync
        self._meta = meta
        self._changes: list[tuple[str, Any]] = []
        self._closed = False

    def __enter__(self) -> "StateTransaction":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self._closed = True
        if exc_type is None and self._changes:
            self._sync._commit_changes(self._changes, self._meta)

    def set(self, path: str, value: Any) -> None:
        if self._closed:
            raise RuntimeError("transaction is already closed")
        self._changes.append((path, value))


class LabSync:
    def __init__(
        self,
        prefix: str = "/sync",
        persist: bool = False,
        db_url: str = "sqlite:///lab_link.db",
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
        self._pending_patch_tasks: set[asyncio.Task[None]] = set()

        # ``sync.state`` is always a SyncState instance.
        # Before @sync.state is applied it acts as the decorator (callable).
        # After registration it delegates attribute access to the internal StateProxy.
        self.state = SyncState(self._register_state_model)

    # ── model registration ───────────────────────────────────────────────────

    def _register_state_model(
        self,
        cls: type[BaseModel],
        initial: BaseModel | dict[str, Any] | None = None,
    ) -> None:
        if not issubclass(cls, BaseModel):
            raise TypeError(f"{cls.__name__} must be a pydantic BaseModel subclass")
        if initial is None:
            initial = cls().model_dump(mode="json")
        self._store = StateStore(cls, initial)
        self._patch_queue = asyncio.Queue()
        self.state._set_proxy(
            StateProxy(self._store, self._patch_queue, self._metadata_from_current_context)
        )

    def register_state(
        self,
        model_class: type[T],
        *,
        initial: T | dict[str, Any] | None = None,
    ) -> None:
        self._register_state_model(model_class, initial)

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

    def set(self, path: str, value: Any) -> tuple[list[dict[str, Any]], int]:
        """Set a JSON Pointer value, validate state, and broadcast one patch."""
        return self._commit_changes([(path, value)], self._metadata_from_current_context())

    def replace_state(self, state: BaseModel | dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
        if self._store is None:
            raise RuntimeError("No sync state model registered")
        patch, version = self._store.replace_state(state)
        if patch:
            self._schedule_patch_broadcast(
                patch,
                version,
                self._metadata_from_current_context(),
            )
        return patch, version

    def transaction(
        self,
        *,
        origin: str | None = None,
        request_id: str | None = None,
        command: str | None = None,
    ) -> StateTransaction:
        ctx = _current_command_context.get()
        meta = PatchMetadata(
            origin_client_id=origin if origin is not None else (ctx.client_id if ctx else None),
            request_id=request_id if request_id is not None else (ctx.request_id if ctx else None),
            command=command if command is not None else (ctx.command if ctx else None),
        )
        return StateTransaction(self, meta)

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
                        client_id=client_id,
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
            logger.exception("Unhandled WebSocket error for client %s", client_id)
        finally:
            await self._conn_manager.disconnect(client_id)

    async def _dispatch_command(
        self,
        websocket: WebSocket,
        client_id: str,
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
                        "code": "unknown_command",
                        "message": f"Unknown command: {command!r}",
                        "severity": "error",
                        "display": "toast",
                        "recoverable": False,
                        "originClientId": client_id,
                        "version": self._store.version() if self._store else 0,
                    }
                )
            return

        ctx = CommandContext(client_id=client_id, request_id=request_id, command=command)
        token = _current_command_context.set(ctx)
        try:
            if iscoroutinefunction(handler):
                result = await self._call_handler(handler, ctx, params)
            else:
                result = self._call_handler(handler, ctx, params)

            if self._patch_queue is not None:
                await self._patch_queue.join()
            await self._flush_pending_patch_tasks()

            if request_id:
                version = self._store.version() if self._store else 0
                message: dict[str, Any] = {
                    "type": "command_ack",
                    "command": command,
                    "requestId": request_id,
                    "version": version,
                }
                if result is not None:
                    message["result"] = result
                await websocket.send_json(message)
        except CommandError as exc:
            await self._send_command_error(websocket, exc, command, request_id, client_id)
        except Exception as exc:
            logger.exception("Command %r failed", command)
            if request_id:
                await self._send_command_error(
                    websocket,
                    CommandError(
                        code="command_failed",
                        message=str(exc) or "Command failed.",
                        detail=repr(exc),
                        recoverable=True,
                    ),
                    command,
                    request_id,
                    client_id,
                )
        finally:
            _current_command_context.reset(token)

    def _call_handler(
        self,
        handler: Callable[..., Any],
        ctx: CommandContext,
        params: dict[str, Any],
    ) -> Any:
        sig = signature(handler)
        call_params = dict(params)
        for name, param in sig.parameters.items():
            if name in call_params:
                continue
            if (
                name == "ctx"
                or param.annotation is CommandContext
                or param.annotation == "CommandContext"
            ):
                call_params[name] = ctx
                break
            if param.kind in {Parameter.VAR_POSITIONAL, Parameter.VAR_KEYWORD}:
                continue
        return handler(**call_params)

    async def _send_command_error(
        self,
        websocket: WebSocket,
        exc: CommandError,
        command: str,
        request_id: str | None,
        client_id: str,
    ) -> None:
        if not request_id:
            return
        await websocket.send_json(
            exc.to_message(
                command=command,
                request_id=request_id,
                version=self._store.version() if self._store else 0,
                origin_client_id=client_id,
            )
        )

    def _metadata_from_current_context(self) -> PatchMetadata:
        ctx = _current_command_context.get()
        if ctx is None:
            return PatchMetadata()
        return PatchMetadata(
            origin_client_id=ctx.client_id,
            request_id=ctx.request_id,
            command=ctx.command,
        )

    def _commit_changes(
        self,
        changes: list[tuple[str, Any]],
        meta: PatchMetadata,
    ) -> tuple[list[dict[str, Any]], int]:
        if self._store is None:
            raise RuntimeError("No sync state model registered")
        patch, version = self._store.apply_values(changes)
        if patch:
            self._schedule_patch_broadcast(patch, version, meta)
        return patch, version

    def _schedule_patch_broadcast(
        self,
        patch: list[dict[str, Any]],
        version: int,
        meta: PatchMetadata,
    ) -> None:
        if self._conn_manager is None:
            return
        task = asyncio.create_task(self._broadcast_patch(patch, version, meta))
        self._pending_patch_tasks.add(task)
        task.add_done_callback(self._pending_patch_tasks.discard)

    async def _broadcast_patch(
        self,
        patch: list[dict[str, Any]],
        version: int,
        meta: PatchMetadata,
    ) -> None:
        if self._conn_manager is None:
            return
        await self._conn_manager.broadcast_patch(
            patch,
            version,
            origin_client_id=meta.origin_client_id,
            request_id=meta.request_id,
            command=meta.command,
        )
        if self._persistence:
            await self._persistence.save_debounced(self._store.snapshot())

    async def _flush_pending_patch_tasks(self) -> None:
        while self._pending_patch_tasks:
            tasks = list(self._pending_patch_tasks)
            await asyncio.gather(*tasks)


# ── background tasks ──────────────────────────────────────────────────────────

async def _drain_patch_queue(
    queue: asyncio.Queue,
    store: StateStore,
    conn_manager: ConnectionManager,
    persistence: PersistenceManager | None,
) -> None:
    while True:
        item = await queue.get()
        try:
            if len(item) == 2:
                path, value = item
                meta = PatchMetadata()
            else:
                path, value, meta = item
            patch, version = store.apply_value(path, value)
            await conn_manager.broadcast_patch(
                patch,
                version,
                origin_client_id=meta.origin_client_id,
                request_id=meta.request_id,
                command=meta.command,
            )
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
