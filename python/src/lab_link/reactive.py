"""Reactive state models.

Mutating a bound ``ReactiveModel`` records a JSON Patch op, batches it with
other ops from the same event-loop tick, and broadcasts one versioned patch
message — no manual ``sync.set(...)`` and no second copy of the state for the
application to keep in agreement with the first.

Tree bookkeeping lives in private attributes (``_ll_parent`` / ``_ll_key`` on
every node, ``_ll_sink`` on the root), so serialization, persistence, and
downstream TypeScript generation are unaffected.

Rules the engine enforces:

- every nested model must subclass ``ReactiveModel`` (plain ``BaseModel``
  children would silently lose reactivity — we raise instead);
- ``list`` and ``dict`` field values are wrapped into ``ReactiveList`` /
  ``ReactiveDict`` so structural mutations are tracked; ``set`` fields and
  models inside tuples are rejected;
- an object may live at only one location in the tree — adoption overwrites
  the backrefs, so writes through a replaced (orphaned) reference are dropped
  (with a debug log), matching the document semantics;
- mutations must happen on the owning event loop's thread; cross-thread
  writes raise rather than corrupt batching and ordering.
"""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Callable, Iterable, Iterator

from pydantic import BaseModel, ConfigDict, PrivateAttr, TypeAdapter

from .pointer import escape_pointer_part

if TYPE_CHECKING:
    import asyncio

logger = logging.getLogger(__name__)

_MISSING = object()
_ANY_ADAPTER: TypeAdapter[Any] = TypeAdapter(Any)


def _jsonify(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return _ANY_ADAPTER.dump_python(value, mode="json")


class ChangeSink:
    """Per-``LabSync`` op buffer and flusher.

    Ops recorded during one event-loop tick are flushed as a single patch
    message via ``loop.call_soon``. Before a loop is attached (module import,
    before lifespan) ops apply silently to the mirror — there are no clients
    yet and the bound model *is* the state.
    """

    def __init__(
        self,
        *,
        apply_local: Callable[[list[dict[str, Any]]], None],
        commit: Callable[[list[dict[str, Any]], Any], int],
        metadata_getter: Callable[[], Any],
    ) -> None:
        self._apply_local = apply_local
        self._commit = commit
        self._metadata_getter = metadata_getter
        self._buffer: list[tuple[str, str, Any]] = []  # (op, path, raw value)
        self._meta: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._owner_thread: int | None = None
        self._flush_scheduled = False
        self._suspend_depth = 0
        self._mute_depth = 0

    @property
    def is_attached(self) -> bool:
        return self._loop is not None

    def attach_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop
        self._owner_thread = threading.get_ident()

    def detach_loop(self) -> None:
        self._loop = None
        self._owner_thread = None

    def record(self, op: str, path: str, value: Any = None) -> None:
        if self._mute_depth:
            return
        if (
            self._owner_thread is not None
            and threading.get_ident() != self._owner_thread
        ):
            raise RuntimeError(
                "lab-link state was mutated from a thread other than the owning "
                "event loop's. Mutate after awaiting `asyncio.to_thread(...)`, "
                "not inside it, or marshal the value back with "
                "`loop.call_soon_threadsafe`."
            )
        meta = self._metadata_getter()
        if self._buffer and meta != self._meta:
            # One patch message never mixes two commands' metadata.
            self.flush()
        self._meta = meta

        if op == "replace" and self._buffer:
            last_op, last_path, _ = self._buffer[-1]
            if last_op == "replace" and last_path == path:
                self._buffer[-1] = (op, path, value)
                self._ensure_flush()
                return
        self._buffer.append((op, path, value))
        self._ensure_flush()

    def _ensure_flush(self) -> None:
        if self._suspend_depth:
            return
        if self._loop is None:
            self.flush()
        elif not self._flush_scheduled:
            self._flush_scheduled = True
            self._loop.call_soon(self._scheduled_flush)

    def _scheduled_flush(self) -> None:
        self._flush_scheduled = False
        self.flush()

    def flush(self) -> list[dict[str, Any]]:
        """Drain the buffer into one patch. Values are serialized here, so a
        whole-subtree assignment becomes one ``replace`` op with a JSON value."""
        if not self._buffer:
            return []
        ops: list[dict[str, Any]] = []
        for op, path, value in self._buffer:
            entry: dict[str, Any] = {"op": op, "path": path}
            if op != "remove":
                entry["value"] = _jsonify(value)
            ops.append(entry)
        self._buffer.clear()
        meta = self._meta
        self._meta = None
        if self._loop is None:
            self._apply_local(ops)
        else:
            self._commit(ops, meta)
        return ops

    def suspend(self) -> None:
        self._suspend_depth += 1

    def resume(self) -> None:
        self._suspend_depth -= 1
        if self._suspend_depth == 0:
            self.flush()

    @contextmanager
    def muted(self) -> Iterator[None]:
        """Drop records entirely (used by ``load_state``'s in-place swap)."""
        self._mute_depth += 1
        try:
            yield
        finally:
            self._mute_depth -= 1


class ReactiveModel(BaseModel):
    """Base class for application state. Assignments validate (pydantic
    ``validate_assignment``), record a patch op, and batch per tick."""

    model_config = ConfigDict(validate_assignment=True)

    _ll_parent: Any = PrivateAttr(default=None)
    _ll_key: Any = PrivateAttr(default=None)
    _ll_sink: Any = PrivateAttr(default=None)  # set on the bound root only

    def model_post_init(self, context: Any, /) -> None:
        super().model_post_init(context)
        for name in type(self).model_fields:
            value = self.__dict__.get(name, _MISSING)
            if value is _MISSING:
                continue
            wrapped = _wrap(value, owner=f"{type(self).__name__}.{name}")
            if wrapped is not value:
                self.__dict__[name] = wrapped
            _adopt(wrapped, self, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_") or name not in type(self).model_fields:
            super().__setattr__(name, value)
            return
        old = self.__dict__.get(name, _MISSING)
        super().__setattr__(name, value)  # pydantic validates + coerces
        new = self.__dict__.get(name, value)
        wrapped = _wrap(new, owner=f"{type(self).__name__}.{name}")
        if wrapped is not new:
            # validate_assignment rebuilds plain containers; re-wrap them
            self.__dict__[name] = wrapped
            new = wrapped
        if new is old:
            return
        _adopt(new, self, name)
        if old is not _MISSING:
            _orphan_child(old, self, name)
        try:
            changed = old is _MISSING or bool(old != new)
        except Exception:
            changed = True
        if not changed:
            return
        sink, base = _locate(self)
        if sink is None:
            logger.debug(
                "lab-link: write to %s.%s dropped (node is not part of a bound "
                "state tree)",
                type(self).__name__,
                name,
            )
            return
        sink.record("replace", f"{base}/{escape_pointer_part(name)}", new)


class ReactiveList(list):
    """List wrapper that records structural mutations as patch ops and keeps
    children's backrefs (re-keying on insert/remove). Created automatically
    for ``list`` fields of a ``ReactiveModel`` — not meant for direct use."""

    __slots__ = ("_ll_parent", "_ll_key")

    def __init__(self, iterable: Iterable[Any] = ()) -> None:
        super().__init__(iterable)
        self._ll_parent: Any = None
        self._ll_key: Any = None

    # ── helpers ──────────────────────────────────────────────────────────

    def _record(self, op: str, suffix: str, value: Any = None) -> None:
        sink, base = _locate(self)
        if sink is None:
            logger.debug("lab-link: mutation on detached list dropped")
            return
        sink.record(op, base + suffix, value)

    def _record_self_replace(self) -> None:
        sink, base = _locate(self)
        if sink is None:
            logger.debug("lab-link: mutation on detached list dropped")
            return
        # Serialize eagerly: later appends would otherwise leak into this
        # whole-container snapshot and replay twice.
        sink.record("replace", base, _jsonify(self))

    def _reindex(self, start: int = 0) -> None:
        for i in range(start, len(self)):
            item = list.__getitem__(self, i)
            if isinstance(item, (ReactiveModel, ReactiveList, ReactiveDict)):
                item._ll_key = i

    def _norm_index(self, index: int) -> int:
        idx = index + len(self) if index < 0 else index
        if not 0 <= idx < len(self):
            raise IndexError("list index out of range")
        return idx

    # ── mutations ────────────────────────────────────────────────────────

    def __setitem__(self, index: Any, value: Any) -> None:
        if isinstance(index, slice):
            raise TypeError(
                "slice assignment is not supported on reactive lists; assign "
                "the whole list field instead"
            )
        idx = self._norm_index(index)
        wrapped = _wrap(value, owner=f"list[{idx}]")
        old = list.__getitem__(self, idx)
        if wrapped is old:
            return
        list.__setitem__(self, idx, wrapped)
        _adopt(wrapped, self, idx)
        _orphan_child(old, self, idx)
        try:
            changed = bool(old != wrapped)
        except Exception:
            changed = True
        if changed:
            self._record("replace", f"/{idx}", wrapped)

    def __delitem__(self, index: Any) -> None:
        if isinstance(index, slice):
            raise TypeError(
                "slice deletion is not supported on reactive lists; assign "
                "the whole list field instead"
            )
        self.pop(index)

    def append(self, value: Any) -> None:
        wrapped = _wrap(value, owner=f"list[{len(self)}]")
        list.append(self, wrapped)
        idx = len(self) - 1
        _adopt(wrapped, self, idx)
        self._record("add", f"/{idx}", wrapped)

    def insert(self, index: int, value: Any) -> None:
        idx = index + len(self) if index < 0 else index
        idx = min(max(idx, 0), len(self))
        wrapped = _wrap(value, owner=f"list[{idx}]")
        list.insert(self, idx, wrapped)
        _adopt(wrapped, self, idx)
        self._reindex(idx + 1)
        self._record("add", f"/{idx}", wrapped)

    def extend(self, iterable: Iterable[Any]) -> None:
        for value in list(iterable):
            self.append(value)

    def __iadd__(self, iterable: Iterable[Any]) -> "ReactiveList":
        self.extend(iterable)
        return self

    def __imul__(self, value: Any) -> "ReactiveList":
        raise TypeError("in-place repetition is not supported on reactive lists")

    def pop(self, index: int = -1) -> Any:
        idx = self._norm_index(index)
        old = list.pop(self, idx)
        _orphan_child(old, self, idx)
        self._reindex(idx)
        self._record("remove", f"/{idx}")
        return old

    def remove(self, value: Any) -> None:
        self.pop(list.index(self, value))

    def clear(self) -> None:
        for i, item in enumerate(self):
            _orphan_child(item, self, i)
        list.clear(self)
        self._record_self_replace()

    def sort(self, *, key: Any = None, reverse: bool = False) -> None:
        list.sort(self, key=key, reverse=reverse)
        self._reindex()
        self._record_self_replace()

    def reverse(self) -> None:
        list.reverse(self)
        self._reindex()
        self._record_self_replace()


class ReactiveDict(dict):
    """Dict wrapper that records mutations as patch ops. Keys must be strings
    (the wire format is JSON). Created automatically for ``dict`` fields of a
    ``ReactiveModel`` — not meant for direct use."""

    __slots__ = ("_ll_parent", "_ll_key")

    def __init__(self, mapping: dict[str, Any] | None = None) -> None:
        super().__init__(mapping or {})
        self._ll_parent: Any = None
        self._ll_key: Any = None

    def _record(self, op: str, suffix: str, value: Any = None) -> None:
        sink, base = _locate(self)
        if sink is None:
            logger.debug("lab-link: mutation on detached dict dropped")
            return
        sink.record(op, base + suffix, value)

    def __setitem__(self, key: Any, value: Any) -> None:
        if not isinstance(key, str):
            raise TypeError(
                f"reactive dict keys must be str (JSON objects), got {type(key).__name__}"
            )
        wrapped = _wrap(value, owner=f"dict[{key!r}]")
        exists = dict.__contains__(self, key)
        old = dict.get(self, key) if exists else _MISSING
        if wrapped is old:
            return
        dict.__setitem__(self, key, wrapped)
        _adopt(wrapped, self, key)
        if exists:
            _orphan_child(old, self, key)
            try:
                if not bool(old != wrapped):
                    return
            except Exception:
                pass
        self._record(
            "replace" if exists else "add",
            f"/{escape_pointer_part(key)}",
            wrapped,
        )

    def __delitem__(self, key: str) -> None:
        old = dict.__getitem__(self, key)  # raises KeyError like a plain dict
        dict.__delitem__(self, key)
        _orphan_child(old, self, key)
        self._record("remove", f"/{escape_pointer_part(key)}")

    def pop(self, key: str, *default: Any) -> Any:
        if not dict.__contains__(self, key):
            if default:
                return default[0]
            raise KeyError(key)
        value = dict.__getitem__(self, key)
        del self[key]
        return value

    def popitem(self) -> tuple[str, Any]:
        key = next(reversed(self))
        value = self.pop(key)
        return key, value

    def clear(self) -> None:
        for key, value in self.items():
            _orphan_child(value, self, key)
        dict.clear(self)
        sink, base = _locate(self)
        if sink is None:
            logger.debug("lab-link: mutation on detached dict dropped")
            return
        sink.record("replace", base, {})

    def update(self, *args: Any, **kwargs: Any) -> None:
        for key, value in dict(*args, **kwargs).items():
            self[key] = value

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self:
            self[key] = default
        return dict.__getitem__(self, key)


_TRACKED = (ReactiveModel, ReactiveList, ReactiveDict)


def _wrap(value: Any, *, owner: str) -> Any:
    """Convert plain containers to reactive wrappers; reject types whose
    mutations could not be tracked (never silently lose reactivity)."""
    if isinstance(value, _TRACKED):
        return value
    if isinstance(value, BaseModel):
        raise TypeError(
            f"{owner} holds a plain pydantic model ({type(value).__name__}); "
            "every nested model in a reactive state tree must subclass "
            "lab_link.ReactiveModel"
        )
    if isinstance(value, list):
        wrapped = ReactiveList()
        for i, item in enumerate(value):
            item = _wrap(item, owner=f"{owner}[{i}]")
            list.append(wrapped, item)
            _adopt(item, wrapped, i)
        return wrapped
    if isinstance(value, dict):
        wrapped_dict = ReactiveDict()
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError(
                    f"{owner} has a non-str dict key ({key!r}); JSON objects "
                    "require str keys"
                )
            item = _wrap(item, owner=f"{owner}[{key!r}]")
            dict.__setitem__(wrapped_dict, key, item)
            _adopt(item, wrapped_dict, key)
        return wrapped_dict
    if isinstance(value, (set, frozenset)):
        raise TypeError(
            f"{owner} is a set; in-place set mutations cannot be tracked. "
            "Use a list or assign a new value to the field instead."
        )
    if isinstance(value, tuple):
        if any(isinstance(item, (BaseModel, list, dict, set, tuple)) for item in value):
            raise TypeError(
                f"{owner} is a tuple containing models or containers; their "
                "mutations could not be tracked. Use a list of ReactiveModels."
            )
        return value
    return value


def _adopt(value: Any, parent: Any, key: Any) -> None:
    """Point a child node back at its container. Last adoption wins — an
    object may live at only one location in the tree."""
    if isinstance(value, _TRACKED):
        value._ll_parent = parent
        value._ll_key = key


def _orphan_child(old: Any, parent: Any, key: Any) -> None:
    """Clear backrefs of a replaced/removed child, but only if they still
    point here (the child may have been re-adopted into the new subtree)."""
    if (
        isinstance(old, _TRACKED)
        and old._ll_parent is parent
        and old._ll_key == key
    ):
        old._ll_parent = None
        old._ll_key = None


def _locate(node: Any) -> tuple[ChangeSink | None, str | None]:
    """Walk backrefs to the root; return (sink, escaped JSON Pointer of node),
    or (None, None) if the node is not part of a bound tree."""
    parts: list[str] = []
    current = node
    while True:
        parent = current._ll_parent
        if parent is None:
            sink = current._ll_sink if isinstance(current, ReactiveModel) else None
            if sink is None:
                return None, None
            parts.reverse()
            return sink, "".join("/" + part for part in parts)
        parts.append(escape_pointer_part(str(current._ll_key)))
        current = parent


def _orphan(value: Any) -> None:
    if isinstance(value, _TRACKED):
        value._ll_parent = None
        value._ll_key = None
