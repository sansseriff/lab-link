from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, TypeVar

if TYPE_CHECKING:
    from .state_store import StateStore

T = TypeVar("T")


class NestedProxy:
    """Accumulates path segments for nested attribute access. Write-only for mutations."""

    __slots__ = ("_root", "_path")

    def __init__(self, root: "StateProxy", path: list[str]) -> None:
        object.__setattr__(self, "_root", root)
        object.__setattr__(self, "_path", path)

    def __getattr__(self, name: str) -> "NestedProxy":
        if name.startswith("_"):
            raise AttributeError(name)
        path = object.__getattribute__(self, "_path")
        root = object.__getattribute__(self, "_root")
        return NestedProxy(root, path + [name])

    def __setattr__(self, name: str, value: Any) -> None:
        path = object.__getattribute__(self, "_path")
        root = object.__getattribute__(self, "_root")
        full_path = "/" + "/".join(path + [name])
        root._enqueue(full_path, value)


class StateProxy:
    """
    Internal proxy held by SyncState. Enqueues ``(json_path, value)`` onto the drain queue
    on every attribute write. Not exposed directly — use ``sync.state`` instead.
    """

    __slots__ = ("_store", "_queue")

    def __init__(self, store: "StateStore", queue: asyncio.Queue) -> None:
        object.__setattr__(self, "_store", store)
        object.__setattr__(self, "_queue", queue)

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        store: "StateStore" = object.__getattribute__(self, "_store")
        val = store.snapshot().get(name)
        if isinstance(val, dict):
            return NestedProxy(self, [name])
        return val

    def __setattr__(self, name: str, value: Any) -> None:
        self._enqueue(f"/{name}", value)

    def _enqueue(self, path: str, value: Any) -> None:
        queue: asyncio.Queue = object.__getattribute__(self, "_queue")
        queue.put_nowait((path, value))

    def _rebind_queue(self, queue: asyncio.Queue) -> None:
        object.__setattr__(self, "_queue", queue)


class SyncState:
    """
    The object behind ``sync.state``. Dual-purpose:

    - ``@sync.state`` — callable; registers a Pydantic BaseModel class and returns it.
    - ``sync.state.x = 5`` — after registration, delegates to the internal StateProxy
      which enqueues ``("/x", 5)`` onto the drain queue.

    Having a single concrete type for both roles lets type checkers (Pylance, mypy)
    understand ``@sync.state`` as a normal callable decorator that returns the class.
    """

    __slots__ = ("_proxy", "_on_register")

    def __init__(self, on_register: Callable[[type], None]) -> None:
        object.__setattr__(self, "_proxy", None)
        object.__setattr__(self, "_on_register", on_register)

    def __call__(self, cls: type[T]) -> type[T]:
        """Register ``cls`` as the sync state model. Used as ``@sync.state``."""
        on_reg: Callable[[type], None] = object.__getattribute__(self, "_on_register")
        on_reg(cls)
        return cls

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        proxy: StateProxy | None = object.__getattribute__(self, "_proxy")
        if proxy is None:
            raise RuntimeError("No @sync.state model registered yet")
        return getattr(proxy, name)

    def __setattr__(self, name: str, value: Any) -> None:
        proxy: StateProxy | None = object.__getattribute__(self, "_proxy")
        if proxy is None:
            raise RuntimeError("No @sync.state model registered yet")
        setattr(proxy, name, value)

    def _set_proxy(self, proxy: StateProxy) -> None:
        object.__setattr__(self, "_proxy", proxy)

    def _get_proxy(self) -> StateProxy | None:
        return object.__getattribute__(self, "_proxy")
