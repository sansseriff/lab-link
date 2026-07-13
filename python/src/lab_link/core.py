from __future__ import annotations

import asyncio
import contextvars
import logging
import warnings
from contextlib import asynccontextmanager, contextmanager
from dataclasses import dataclass
from inspect import Parameter, iscoroutinefunction, signature
from typing import Any, Callable, Iterable, Iterator, Literal, TypeVar, overload

from pydantic import BaseModel
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import BaseRoute, Route, WebSocketRoute
from starlette.websockets import WebSocket, WebSocketDisconnect

from .auth import AuthPrincipal, SyncAuth
from .connection_manager import ConnectionManager
from .errors import CommandError
from .persistence import PersistenceManager
from .reactive import ChangeSink, ReactiveModel
from .state_store import StateStore, _parse_pointer
from .stream_buffer import AppendBuffer, DeltaBuffer, ReplaceBuffer, StreamRef

T = TypeVar("T", bound=BaseModel)
StateT = TypeVar("StateT", bound=ReactiveModel)
_F = TypeVar("_F", bound=Callable[..., Any])
logger = logging.getLogger(__name__)
_AUTH_REVALIDATE_SECONDS = 30.0


@dataclass(frozen=True, slots=True)
class CommandContext:
    client_id: str
    request_id: str | None
    command: str
    auth: AuthPrincipal | None = None


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
            self._sync._commit_transaction(self._changes, self._meta)

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
        auth: SyncAuth | None = None,
    ) -> None:
        self._prefix = prefix.rstrip("/")
        self._persist = persist
        self._db_url = db_url
        self._compress = compress
        self.auth = auth

        self._store: StateStore | None = None
        self._state_obj: ReactiveModel | None = None
        self._sink: ChangeSink | None = None
        self._commands: dict[str, Callable[..., Any]] = {}
        self._command_capabilities: dict[str, frozenset[str]] = {}
        self._updaters: list[tuple[Callable[..., Any], float]] = []
        self._streams: dict[str, StreamRef] = {}
        self._live_buffers: dict[str, AppendBuffer | ReplaceBuffer | DeltaBuffer] = {}
        self._conn_manager: ConnectionManager | None = None
        self._persistence: PersistenceManager | None = None
        self._pending_patch_tasks: set[asyncio.Task[None]] = set()
        self._last_broadcast: asyncio.Task[None] | None = None

    # ── state binding ─────────────────────────────────────────────────────────

    def bind_state(self, instance: StateT) -> StateT:
        """Bind a ReactiveModel instance as the authoritative state.

        After binding, every attribute/list/dict mutation on the tree is
        validated, recorded, batched per event-loop tick, and broadcast as one
        versioned patch message. Returns the instance for a typed reference.
        """
        if not isinstance(instance, ReactiveModel):
            raise TypeError(
                f"bind_state() requires a ReactiveModel instance, got "
                f"{type(instance).__name__}"
            )
        if self._state_obj is not None or self._store is not None:
            raise RuntimeError("a state model is already bound/registered")
        if instance._ll_sink is not None or instance._ll_parent is not None:
            raise RuntimeError(
                "this instance is already bound to a LabSync (or nested in "
                "another bound tree)"
            )
        self._store = StateStore(type(instance), instance)
        self._sink = ChangeSink(
            apply_local=self._apply_reactive_ops_local,
            commit=self._commit_reactive_ops,
            metadata_getter=self._metadata_from_current_context,
        )
        instance._ll_sink = self._sink
        self._state_obj = instance
        return instance

    @property
    def state(self) -> ReactiveModel:
        """The bound ReactiveModel instance."""
        if self._state_obj is None:
            raise RuntimeError("no state bound; call sync.bind_state(instance) first")
        return self._state_obj

    def load_state(
        self, data: BaseModel | dict[str, Any]
    ) -> tuple[list[dict[str, Any]], int]:
        """Bulk state replacement for restore paths.

        Validates ``data`` against the bound class, swaps the contents of the
        bound instance in place (existing references stay valid), and emits a
        single whole-document ``replace`` patch.
        """
        if self._state_obj is None or self._sink is None or self._store is None:
            raise RuntimeError(
                "load_state() requires a bound state; call bind_state() first"
            )
        cls = type(self._state_obj)
        validated = cls.model_validate(data)
        self._sink.flush()  # don't mix earlier pending ops into the swap
        with self._sink.muted():
            for name in cls.model_fields:
                setattr(self._state_obj, name, validated.__dict__.get(name))
        ops: list[dict[str, Any]] = [
            {
                "op": "replace",
                "path": "",
                "value": self._state_obj.model_dump(mode="json"),
            }
        ]
        if self._sink.is_attached:
            version = self._commit_reactive_ops(
                ops, self._metadata_from_current_context()
            )
        else:
            self._apply_reactive_ops_local(ops)
            version = self._store.version()
        return ops, version

    @contextmanager
    def batch(self) -> Iterator[None]:
        """Suspend per-tick flushing; emit one combined patch on exit.

        Avoid awaiting inside the block — mutations from interleaved tasks
        would be attributed to their own metadata and flushed separately.
        """
        if self._sink is None:
            raise RuntimeError(
                "batch() requires a bound state; call bind_state() first"
            )
        self._sink.suspend()
        try:
            yield
        finally:
            self._sink.resume()

    def publish(self) -> tuple[list[dict[str, Any]], int]:
        """Dump-and-diff fallback: diff the bound model against the mirror and
        broadcast the difference. With the reactive engine the diff is normally
        empty; this is the escape hatch and the testing oracle."""
        if self._state_obj is None or self._store is None:
            raise RuntimeError(
                "publish() requires a bound state; call bind_state() first"
            )
        if self._sink is not None:
            self._sink.flush()
        if self._store.snapshot() == self._state_obj.model_dump(mode="json"):
            return [], self._store.version()
        patch, version = self._store.replace_state(self._state_obj)
        if patch:
            self._schedule_patch_broadcast(
                patch, version, self._metadata_from_current_context()
            )
        return patch, version

    # ── legacy (deprecated) registration ──────────────────────────────────────

    def register_state(
        self,
        model_class: type[T],
        *,
        initial: T | dict[str, Any] | None = None,
    ) -> None:
        """Deprecated: use ``bind_state()`` with a ReactiveModel instance."""
        warnings.warn(
            "register_state() is deprecated; subclass lab_link.ReactiveModel "
            "and call sync.bind_state(instance) instead",
            DeprecationWarning,
            stacklevel=2,
        )
        if isinstance(initial, ReactiveModel):
            self.bind_state(initial)
            return
        self._register_state_model(model_class, initial)

    def _register_state_model(
        self,
        cls: type[BaseModel],
        initial: BaseModel | dict[str, Any] | None = None,
    ) -> None:
        if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
            raise TypeError(f"{cls!r} must be a pydantic BaseModel subclass")
        if self._state_obj is not None or self._store is not None:
            raise RuntimeError("a state model is already bound/registered")
        if initial is None:
            initial = cls().model_dump(mode="json")
        self._store = StateStore(cls, initial)

    # ── decorators ──────────────────────────────────────────────────────────

    @overload
    def command(self, fn: _F, /) -> _F: ...

    @overload
    def command(
        self, fn: None = None, /, *, requires: Iterable[str]
    ) -> Callable[[_F], _F]: ...

    def command(
        self,
        fn: _F | None = None,
        /,
        *,
        requires: Iterable[str] | None = None,
    ) -> _F | Callable[[_F], _F]:
        """Register a command, optionally requiring authenticated capabilities.

        Authenticated principals require ``control`` by default. Use
        ``@sync.command(requires={"manage_access"})`` for a more privileged
        operation. Unauthenticated/open-mode applications remain compatible.
        """

        def register(handler: _F) -> _F:
            self._commands[handler.__name__] = handler
            self._command_capabilities[handler.__name__] = frozenset(
                {"control"} if requires is None else requires
            )
            return handler

        return register(fn) if fn is not None else register

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

    # ── path-based state access (deprecated) ──────────────────────────────────

    def get(self, path: str) -> Any:
        """Deprecated: read attributes of ``sync.state`` directly."""
        warnings.warn(
            "sync.get() is deprecated; read attributes of sync.state directly",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._store is None:
            raise RuntimeError("no state bound or registered")
        return self._store.get(path)

    def set(self, path: str, value: Any) -> tuple[list[dict[str, Any]], int]:
        """Deprecated: mutate attributes of ``sync.state`` directly."""
        warnings.warn(
            "sync.set() is deprecated; mutate attributes of sync.state directly",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._state_obj is not None:
            self._set_on_model(path, value)
            ops = self._sink.flush() if self._sink is not None else []
            return ops, self._store.version() if self._store else 0
        return self._commit_changes(
            [(path, value)], self._metadata_from_current_context()
        )

    def replace_state(
        self, state: BaseModel | dict[str, Any]
    ) -> tuple[list[dict[str, Any]], int]:
        """Deprecated: use ``load_state()``."""
        warnings.warn(
            "sync.replace_state() is deprecated; use sync.load_state()",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._state_obj is not None:
            return self.load_state(state)
        if self._store is None:
            raise RuntimeError("no state bound or registered")
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
        """Deprecated: use ``with sync.batch():`` and mutate ``sync.state``."""
        warnings.warn(
            "sync.transaction() is deprecated; use `with sync.batch():` and "
            "mutate sync.state directly",
            DeprecationWarning,
            stacklevel=2,
        )
        ctx = _current_command_context.get()
        meta = PatchMetadata(
            origin_client_id=origin
            if origin is not None
            else (ctx.client_id if ctx else None),
            request_id=request_id
            if request_id is not None
            else (ctx.request_id if ctx else None),
            command=command if command is not None else (ctx.command if ctx else None),
        )
        return StateTransaction(self, meta)

    def _set_on_model(self, path: str, value: Any) -> None:
        parts = _parse_pointer(path)
        if not parts:
            raise ValueError(
                "path must not be empty; use load_state() for root replacement"
            )
        node: Any = self._state_obj
        for part in parts[:-1]:
            node = _model_child(node, part, path)
        last = parts[-1]
        if isinstance(node, ReactiveModel):
            setattr(node, last, value)
        elif isinstance(node, list):
            if last == "-":
                node.append(value)
            else:
                node[int(last)] = value
        elif isinstance(node, dict):
            node[last] = value
        else:
            raise TypeError(f"path {path!r} targets a non-container value")

    @property
    def streams(self) -> dict[str, AppendBuffer | ReplaceBuffer | DeltaBuffer]:
        return self._live_buffers

    # ── ASGI integration ─────────────────────────────────────────────────────

    @property
    def routes(self) -> list[BaseRoute]:
        """Starlette routes for `GET {prefix}/state` and `WS {prefix}/ws`.

        Pass to `Starlette(routes=...)` or `app.routes.extend(...)`. FastAPI
        apps can instead wire `handle_ws` to a route of their choosing.
        """

        async def get_state(request: Any) -> JSONResponse:
            if self.auth is not None and not self.auth.is_http_authorized(request):
                return JSONResponse(
                    {"detail": "Authentication required"}, status_code=401
                )
            return JSONResponse(self._store.snapshot() if self._store else {})

        auth_routes = self.auth.routes(self._prefix) if self.auth is not None else []
        return [
            *auth_routes,
            Route(f"{self._prefix}/state", get_state),
            WebSocketRoute(f"{self._prefix}/ws", self.handle_ws),
        ]

    @asynccontextmanager
    async def lifespan(self, app: Any | None = None):
        """Use as app lifespan to start updaters, persistence, and broadcasting."""
        self._conn_manager = ConnectionManager()

        # Materialise all StreamRefs now that we have a conn_manager
        for sid, ref in self._streams.items():
            buf = self._make_buffer(sid, ref.mode, ref.capacity, ref.dtype)
            ref.materialize(buf)
            self._live_buffers[sid] = buf

        # Optional persistence — a corrupt database must not block startup.
        # Restore happens before the sink attaches to the loop, so the loaded
        # state lands silently at version 0 (there are no clients yet).
        if self._persist and self._store is not None:
            try:
                self._persistence = PersistenceManager(self._db_url)
                saved = self._persistence.initialize()
                if saved:
                    if self._state_obj is not None:
                        self.load_state(saved)
                    else:
                        self._store.replace_state(saved)
            except Exception:
                logger.exception(
                    "Failed to restore persisted state from %s", self._db_url
                )

        if self._sink is not None:
            self._sink.attach_loop(asyncio.get_running_loop())

        tasks: list[asyncio.Task[None]] = []
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

        if self._sink is not None:
            self._sink.flush()
            self._sink.detach_loop()

        if self._persistence and self._store:
            self._persistence.save_sync(self._store.snapshot())

        await self._conn_manager.close_all()

    def create_app(self, **starlette_kwargs: Any) -> Starlette:
        """Convenience: creates a Starlette app with lifespan + routes pre-wired."""

        @asynccontextmanager
        async def _lifespan(app: Starlette):
            async with self.lifespan(app):
                yield

        routes = list(self.routes) + list(starlette_kwargs.pop("routes", []))
        return Starlette(lifespan=_lifespan, routes=routes, **starlette_kwargs)

    # ── WebSocket handler ─────────────────────────────────────────────────────

    async def handle_ws(self, websocket: WebSocket) -> None:
        """Serve one sync client. Attach to any websocket route in your app."""
        authorized, principal = self._websocket_auth(websocket)
        if not authorized:
            await websocket.close(code=4401, reason="Authentication required")
            return
        client_id = ConnectionManager.generate_client_id()
        snapshot = self._store.snapshot() if self._store else {}
        version = self._store.version() if self._store else 0
        stream_snapshots = [
            buf.snapshot_message() for buf in self._live_buffers.values()
        ]
        await self._conn_manager.connect(
            websocket, client_id, snapshot, version, stream_snapshots
        )
        try:
            while True:
                if self.auth is None:
                    data = await websocket.receive_json()
                else:
                    try:
                        data = await asyncio.wait_for(
                            websocket.receive_json(),
                            timeout=_AUTH_REVALIDATE_SECONDS,
                        )
                    except TimeoutError:
                        authorized, principal = self._websocket_auth(websocket)
                        if not authorized:
                            await websocket.close(
                                code=4401, reason="Authentication expired"
                            )
                            return
                        continue
                    authorized, principal = self._websocket_auth(websocket)
                    if not authorized:
                        await websocket.close(
                            code=4401, reason="Authentication expired"
                        )
                        return
                msg_type = data.get("type")
                if msg_type == "command":
                    await self._dispatch_command(
                        websocket=websocket,
                        client_id=client_id,
                        command=str(data.get("command", "")),
                        params=dict(data.get("params") or {}),
                        request_id=data.get("requestId"),
                        auth=principal,
                    )
                elif msg_type == "stream_resync":
                    stream_id = data.get("id")
                    buf = self._live_buffers.get(stream_id)
                    if buf:
                        await self._conn_manager.send_to(
                            client_id, buf.snapshot_message()
                        )
        except WebSocketDisconnect:
            pass
        except Exception:
            logger.exception("Unhandled WebSocket error for client %s", client_id)
        finally:
            await self._conn_manager.disconnect(client_id)

    def _websocket_auth(
        self, websocket: WebSocket
    ) -> tuple[bool, AuthPrincipal | None]:
        if self.auth is None:
            return True, None
        principal_getter = getattr(self.auth, "principal_for_websocket", None)
        if principal_getter is not None:
            principal = principal_getter(websocket)
            return principal is not None, principal
        return self.auth.is_websocket_authorized(websocket), None

    async def _dispatch_command(
        self,
        websocket: WebSocket,
        client_id: str,
        command: str,
        params: dict[str, Any],
        request_id: str | None,
        auth: AuthPrincipal | None,
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

        required = self._command_capabilities.get(command, frozenset({"control"}))
        if auth is not None and any(
            not auth.can(capability) for capability in required
        ):
            if request_id:
                await self._send_command_error(
                    websocket,
                    CommandError(
                        code="forbidden",
                        message="This credential is not permitted to run that command.",
                        recoverable=False,
                    ),
                    command,
                    request_id,
                    client_id,
                )
            return

        ctx = CommandContext(
            client_id=client_id,
            request_id=request_id,
            command=command,
            auth=auth,
        )
        token = _current_command_context.set(ctx)
        try:
            if iscoroutinefunction(handler):
                result = await self._call_handler(handler, ctx, params)
            else:
                result = self._call_handler(handler, ctx, params)

            # Every patch produced by this command must reach the wire before
            # its ack, and the ack must carry the post-command version.
            if self._sink is not None:
                self._sink.flush()
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
            await self._send_command_error(
                websocket, exc, command, request_id, client_id
            )
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

    # ── patch commit / broadcast ──────────────────────────────────────────────

    def _commit_reactive_ops(
        self, ops: list[dict[str, Any]], meta: PatchMetadata
    ) -> int:
        version = self._store.apply_patch(ops)
        self._schedule_patch_broadcast(ops, version, meta)
        return version

    def _apply_reactive_ops_local(self, ops: list[dict[str, Any]]) -> None:
        self._store.apply_patch(ops, bump_version=False)

    def _commit_transaction(
        self,
        changes: list[tuple[str, Any]],
        meta: PatchMetadata,
    ) -> None:
        if self._state_obj is not None:
            # Reactive mode: metadata is captured from the command context at
            # record time; explicit origin/request_id overrides are ignored.
            with self.batch():
                for path, value in changes:
                    self._set_on_model(path, value)
            return
        self._commit_changes(changes, meta)

    def _commit_changes(
        self,
        changes: list[tuple[str, Any]],
        meta: PatchMetadata,
    ) -> tuple[list[dict[str, Any]], int]:
        if self._store is None:
            raise RuntimeError("no state bound or registered")
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
        # Chain on the previous broadcast so patches reach the wire in
        # version order even when several flushes land in one tick.
        previous = self._last_broadcast

        async def _ordered_broadcast() -> None:
            if previous is not None:
                try:
                    await previous
                except Exception:
                    pass
            await self._broadcast_patch(patch, version, meta)

        task = asyncio.create_task(_ordered_broadcast())
        self._last_broadcast = task
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


def _model_child(node: Any, part: str, full_path: str) -> Any:
    if isinstance(node, ReactiveModel):
        return getattr(node, part)
    if isinstance(node, list):
        return node[int(part)]
    if isinstance(node, dict):
        return node[part]
    raise TypeError(f"path {full_path!r} traverses a non-container value")


# ── background tasks ──────────────────────────────────────────────────────────


async def _run_updater(fn: Callable[..., Any], interval: float) -> None:
    while True:
        await asyncio.sleep(interval)
        if iscoroutinefunction(fn):
            await fn()
        else:
            fn()
