from __future__ import annotations

import asyncio
import copy
import inspect
import json
import logging
import threading
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

import jsonpatch

try:  # websockets >= 14
    from websockets.asyncio.client import connect as websocket_connect
except ImportError:  # pragma: no cover - compatibility with older websockets
    from websockets import connect as websocket_connect

logger = logging.getLogger(__name__)

PatchCallback = Callable[["PatchEvent"], Any]
SnapshotCallback = Callable[["SnapshotEvent"], Any]
CommandErrorCallback = Callable[["SyncCommandError"], Any]
Unsubscribe = Callable[[], None]


@dataclass(frozen=True, slots=True)
class SnapshotEvent:
    data: dict[str, Any]
    version: int


@dataclass(frozen=True, slots=True)
class PatchEvent:
    patch: list[dict[str, Any]]
    version: int
    origin_client_id: str | None = None
    request_id: str | None = None
    command: str | None = None


@dataclass(frozen=True, slots=True)
class CommandAck:
    command: str
    request_id: str
    version: int
    result: Any | None = None


class SyncCommandError(Exception):
    type = "command_error"

    def __init__(
        self,
        *,
        command: str,
        code: str,
        message: str,
        request_id: str | None = None,
        detail: str | None = None,
        severity: str = "error",
        display: str = "toast",
        recoverable: bool = True,
        path: str | None = None,
        version: int = 0,
        origin_client_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.request_id = request_id
        self.code = code
        self.message = message
        self.detail = detail
        self.severity = severity
        self.display = display
        self.recoverable = recoverable
        self.path = path
        self.version = version
        self.origin_client_id = origin_client_id

    @classmethod
    def from_message(cls, message: dict[str, Any]) -> "SyncCommandError":
        return cls(
            command=str(message.get("command", "")),
            request_id=_optional_str(message.get("requestId")),
            code=str(message.get("code", "command_error")),
            message=str(message.get("message", "Command failed.")),
            detail=_optional_str(message.get("detail")),
            severity=str(message.get("severity", "error")),
            display=str(message.get("display", "toast")),
            recoverable=bool(message.get("recoverable", True)),
            path=_optional_str(message.get("path")),
            version=int(message.get("version", 0)),
            origin_client_id=_optional_str(message.get("originClientId")),
        )


class AsyncLabLinkClient:
    def __init__(
        self,
        url: str,
        *,
        command_timeout: float = 10.0,
        connect_timeout: float = 10.0,
        websocket_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self.url = url
        self.command_timeout = command_timeout
        self.connect_timeout = connect_timeout
        self.websocket_kwargs = dict(websocket_kwargs or {})

        self.version = 0
        self._snapshot: dict[str, Any] | None = None
        self._ws: Any | None = None
        self._receive_task: asyncio.Task[None] | None = None
        self._pending_commands: dict[str, asyncio.Future[CommandAck]] = {}
        self._patch_callbacks: set[PatchCallback] = set()
        self._snapshot_callbacks: set[SnapshotCallback] = set()
        self._command_error_callbacks: set[CommandErrorCallback] = set()
        self._errors: deque[SyncCommandError] = deque(maxlen=20)

    async def __aenter__(self) -> "AsyncLabLinkClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._receive_task is not None

    async def connect(self) -> "AsyncLabLinkClient":
        if self.connected:
            return self

        self._ws = await websocket_connect(self.url, **self.websocket_kwargs)
        try:
            await self._receive_initial_snapshot()
        except Exception:
            await self._close_socket()
            raise

        self._receive_task = asyncio.create_task(self._receive_loop())
        return self

    async def close(self) -> None:
        task = self._receive_task
        self._receive_task = None
        await self._close_socket()

        if task is not None and task is not asyncio.current_task():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self._fail_pending(
            SyncCommandError(
                command="",
                code="not_connected",
                message="Connection closed.",
                recoverable=True,
                version=self.version,
            )
        )

    def snapshot(self) -> dict[str, Any] | None:
        return copy.deepcopy(self._snapshot)

    async def snapshot_async(self) -> dict[str, Any] | None:
        return self.snapshot()

    def last_errors(self) -> list[SyncCommandError]:
        return list(self._errors)

    def on_patch(self, callback: PatchCallback) -> Unsubscribe:
        self._patch_callbacks.add(callback)
        return lambda: self._patch_callbacks.discard(callback)

    def on_snapshot(self, callback: SnapshotCallback) -> Unsubscribe:
        self._snapshot_callbacks.add(callback)
        return lambda: self._snapshot_callbacks.discard(callback)

    def on_command_error(self, callback: CommandErrorCallback) -> Unsubscribe:
        self._command_error_callbacks.add(callback)
        return lambda: self._command_error_callbacks.discard(callback)

    async def send_command(
        self,
        command: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        request_id: str | None = None,
    ) -> CommandAck:
        if self._ws is None:
            raise self._emit_command_error(
                SyncCommandError(
                    command=command,
                    code="not_connected",
                    message="Not connected.",
                    recoverable=True,
                    version=self.version,
                )
            )

        request_id = request_id or str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[CommandAck] = loop.create_future()
        self._pending_commands[request_id] = future

        try:
            await self._ws.send(
                json.dumps(
                    {
                        "type": "command",
                        "command": command,
                        "params": params or {},
                        "requestId": request_id,
                    }
                )
            )
        except Exception:
            self._pending_commands.pop(request_id, None)
            raise

        try:
            return await asyncio.wait_for(
                future,
                timeout=self.command_timeout if timeout is None else timeout,
            )
        except TimeoutError:
            self._pending_commands.pop(request_id, None)
            raise self._emit_command_error(
                SyncCommandError(
                    command=command,
                    request_id=request_id,
                    code="command_timeout",
                    message=f'Command "{command}" timed out.',
                    recoverable=True,
                    version=self.version,
                )
            ) from None

    async def _receive_initial_snapshot(self) -> None:
        if self._ws is None:
            raise RuntimeError("WebSocket is not connected")

        while True:
            raw = await asyncio.wait_for(self._ws.recv(), timeout=self.connect_timeout)
            msg = _decode_message(raw)
            if msg is None:
                continue
            if msg.get("type") != "snapshot":
                continue
            self._handle_snapshot(msg)
            return

    async def _receive_loop(self) -> None:
        try:
            assert self._ws is not None
            async for raw in self._ws:
                msg = _decode_message(raw)
                if msg is None:
                    continue
                self._handle_message(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.debug("lab-link receive loop stopped: %r", exc)
        finally:
            self._receive_task = None
            self._ws = None
            self._fail_pending(
                SyncCommandError(
                    command="",
                    code="not_connected",
                    message="Connection closed.",
                    recoverable=True,
                    version=self.version,
                )
            )

    async def _close_socket(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is None:
            return
        try:
            await ws.close()
        except Exception:
            pass

    def _handle_message(self, msg: dict[str, Any]) -> None:
        msg_type = msg.get("type")
        if msg_type == "snapshot":
            self._handle_snapshot(msg)
        elif msg_type == "patch":
            self._handle_patch(msg)
        elif msg_type == "command_ack":
            self._handle_command_ack(msg)
        elif msg_type == "command_error":
            self._handle_command_error(msg)

    def _handle_snapshot(self, msg: dict[str, Any]) -> None:
        data = msg.get("data")
        self._snapshot = copy.deepcopy(data) if isinstance(data, dict) else {}
        self.version = int(msg.get("version", 0))
        self._emit_callbacks(
            self._snapshot_callbacks,
            SnapshotEvent(data=self.snapshot() or {}, version=self.version),
        )

    def _handle_patch(self, msg: dict[str, Any]) -> None:
        patch = list(msg.get("patch") or [])
        self.version = int(msg.get("version", self.version))
        if self._snapshot is not None:
            self._snapshot = jsonpatch.JsonPatch(patch).apply(
                self._snapshot,
                in_place=False,
            )
        self._emit_callbacks(
            self._patch_callbacks,
            PatchEvent(
                patch=patch,
                version=self.version,
                origin_client_id=_optional_str(msg.get("originClientId")),
                request_id=_optional_str(msg.get("requestId")),
                command=_optional_str(msg.get("command")),
            ),
        )

    def _handle_command_ack(self, msg: dict[str, Any]) -> None:
        request_id = _optional_str(msg.get("requestId"))
        if request_id is None:
            return
        pending = self._pending_commands.pop(request_id, None)
        if pending is None or pending.done():
            return
        result = msg.get("result") if "result" in msg else None
        pending.set_result(
            CommandAck(
                command=str(msg.get("command", "")),
                request_id=request_id,
                version=int(msg.get("version", self.version)),
                result=result,
            )
        )

    def _handle_command_error(self, msg: dict[str, Any]) -> None:
        error = self._emit_command_error(SyncCommandError.from_message(msg))
        if error.request_id is None:
            return
        pending = self._pending_commands.pop(error.request_id, None)
        if pending is not None and not pending.done():
            pending.set_exception(error)

    def _fail_pending(self, error: SyncCommandError) -> None:
        pending = list(self._pending_commands.values())
        self._pending_commands.clear()
        for future in pending:
            if not future.done():
                future.set_exception(error)

    def _emit_command_error(self, error: SyncCommandError) -> SyncCommandError:
        self._errors.append(error)
        self._emit_callbacks(self._command_error_callbacks, error)
        return error

    @staticmethod
    def _emit_callbacks(callbacks: set[Callable[[Any], Any]], event: Any) -> None:
        for callback in tuple(callbacks):
            try:
                result = callback(event)
                if inspect.isawaitable(result):
                    task = asyncio.create_task(result)
                    task.add_done_callback(_log_callback_exception)
            except Exception:
                logger.exception("lab-link callback failed")


class LabLinkClient:
    def __init__(
        self,
        url: str,
        *,
        command_timeout: float = 10.0,
        connect_timeout: float = 10.0,
        websocket_kwargs: dict[str, Any] | None = None,
    ) -> None:
        self._async_client = AsyncLabLinkClient(
            url,
            command_timeout=command_timeout,
            connect_timeout=connect_timeout,
            websocket_kwargs=websocket_kwargs,
        )
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()

    def __enter__(self) -> "LabLinkClient":
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        self.close()

    @property
    def version(self) -> int:
        return self._async_client.version

    @property
    def connected(self) -> bool:
        return self._async_client.connected

    def connect(self) -> "LabLinkClient":
        if self._loop is not None:
            return self

        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        self._ready.wait()
        try:
            self._run(self._async_client.connect())
        except Exception:
            self._stop_loop()
            raise
        return self

    def close(self) -> None:
        if self._loop is None:
            return
        try:
            self._run(self._async_client.close())
        finally:
            self._stop_loop()

    def snapshot(self) -> dict[str, Any] | None:
        return self._run(self._async_client.snapshot_async())

    def last_errors(self) -> list[SyncCommandError]:
        return self._async_client.last_errors()

    def on_patch(self, callback: PatchCallback) -> Unsubscribe:
        return self._async_client.on_patch(callback)

    def on_snapshot(self, callback: SnapshotCallback) -> Unsubscribe:
        return self._async_client.on_snapshot(callback)

    def on_command_error(self, callback: CommandErrorCallback) -> Unsubscribe:
        return self._async_client.on_command_error(callback)

    def send_command(
        self,
        command: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
        request_id: str | None = None,
    ) -> CommandAck:
        return self._run(
            self._async_client.send_command(
                command,
                params,
                timeout=timeout,
                request_id=request_id,
            )
        )

    def _run_loop(self) -> None:
        assert self._loop is not None
        asyncio.set_event_loop(self._loop)
        self._ready.set()
        self._loop.run_forever()
        self._loop.close()

    def _run(self, awaitable: Awaitable[Any]) -> Any:
        if self._loop is None:
            raise RuntimeError("Client is not connected")
        return asyncio.run_coroutine_threadsafe(awaitable, self._loop).result()

    def _stop_loop(self) -> None:
        loop = self._loop
        thread = self._thread
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join()
        self._loop = None
        self._thread = None
        self._ready.clear()


def _decode_message(raw: str | bytes) -> dict[str, Any] | None:
    if isinstance(raw, bytes):
        return None
    msg = json.loads(raw)
    return msg if isinstance(msg, dict) else None


def _optional_str(value: Any) -> str | None:
    return None if value is None else str(value)


def _log_callback_exception(task: asyncio.Task[Any]) -> None:
    try:
        task.result()
    except Exception:
        logger.exception("lab-link async callback failed")
