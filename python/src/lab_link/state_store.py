from __future__ import annotations

import copy
import json
import threading
from collections.abc import Sequence
from typing import Any, Generic, TypeVar

import jsonpatch
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class StateStore(Generic[T]):
    def __init__(self, model_class: type[T], initial: BaseModel | dict[str, Any]) -> None:
        self._model_class = model_class
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {}
        self._version: int = 0
        self._replace_internal(initial)

    def _replace_internal(self, state: BaseModel | dict[str, Any]) -> None:
        validated = self._model_class.model_validate(state)
        self._state = validated.model_dump(mode="json")

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._state))

    def version(self) -> int:
        with self._lock:
            return self._version

    def get(self, path: str) -> Any:
        with self._lock:
            keys = _parse_pointer(path)
            current: Any = self._state
            for key in keys:
                current = _get_child(current, key, path)
            return copy.deepcopy(current)

    def apply_value(self, json_path: str, value: Any) -> tuple[list[dict[str, Any]], int]:
        return self.apply_values([(json_path, value)])

    def apply_values(
        self,
        changes: Sequence[tuple[str, Any]],
    ) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            if not changes:
                raise ValueError("at least one change is required")

            old_state = json.loads(json.dumps(self._state))
            next_state = json.loads(json.dumps(self._state))

            for json_path, value in changes:
                _set_pointer_value(next_state, json_path, value)

            validated = self._validate_state(next_state)
            self._state = validated
            self._version += 1
            patch = jsonpatch.make_patch(old_state, self._state)
            return list(patch), self._version

    def replace_state(self, state: BaseModel | dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            old_state = json.loads(json.dumps(self._state))
            self._replace_internal(state)
            self._version += 1
            patch = jsonpatch.make_patch(old_state, self._state)
            return list(patch), self._version

    def _validate_current_state(self) -> None:
        self._state = self._validate_state(self._state)

    def _validate_state(self, state: Any) -> dict[str, Any]:
        try:
            validated = self._model_class.model_validate(state)
        except ValidationError as exc:
            raise ValueError(f"state validation failed: {exc}") from exc
        return validated.model_dump(mode="json")


def _parse_pointer(path: str) -> list[str]:
    if path == "":
        return []
    if not path.startswith("/"):
        path = "/" + path
    return [_unescape_pointer_part(part) for part in path.split("/")[1:]]


def _unescape_pointer_part(part: str) -> str:
    result = []
    i = 0
    while i < len(part):
        char = part[i]
        if char == "~":
            if i + 1 >= len(part) or part[i + 1] not in {"0", "1"}:
                raise ValueError(f"invalid JSON Pointer escape in segment {part!r}")
            result.append("~" if part[i + 1] == "0" else "/")
            i += 2
        else:
            result.append(char)
            i += 1
    return "".join(result)


def _get_child(current: Any, key: str, full_path: str) -> Any:
    if isinstance(current, dict):
        if key not in current:
            raise KeyError(f"path {full_path!r} does not exist")
        return current[key]
    if isinstance(current, list):
        index = _parse_list_index(key, len(current), full_path)
        return current[index]
    raise TypeError(f"path {full_path!r} traverses non-container value")


def _set_pointer_value(state: Any, path: str, value: Any) -> None:
    keys = _parse_pointer(path)
    if not keys:
        raise ValueError("path must not be empty; use replace_state() for root replacement")

    current = state
    for key in keys[:-1]:
        current = _get_child(current, key, path)

    last = keys[-1]
    if isinstance(current, dict):
        if last not in current:
            raise KeyError(f"path {path!r} does not exist")
        current[last] = value
        return

    if isinstance(current, list):
        if last == "-":
            current.append(value)
            return
        index = _parse_list_index(last, len(current), path)
        current[index] = value
        return

    raise TypeError(f"path {path!r} targets non-container value")


def _parse_list_index(segment: str, length: int, full_path: str) -> int:
    if not segment.isdecimal():
        raise TypeError(f"path {full_path!r} uses non-numeric list index {segment!r}")
    index = int(segment)
    if index >= length:
        raise IndexError(f"path {full_path!r} list index {index} out of range")
    return index
