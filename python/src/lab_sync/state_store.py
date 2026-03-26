from __future__ import annotations

import copy
import json
import threading
from typing import Any, Generic, TypeVar

import jsonpatch
from pydantic import BaseModel, ValidationError

T = TypeVar("T", bound=BaseModel)


class StateStore(Generic[T]):
    def __init__(self, model_class: type[T], initial: dict[str, Any]) -> None:
        self._model_class = model_class
        self._lock = threading.RLock()
        self._state: dict[str, Any] = {}
        self._version: int = 0
        self._replace_internal(initial)

    def _replace_internal(self, state: dict[str, Any]) -> None:
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
            keys = [k for k in path.strip("/").split("/") if k]
            current: Any = self._state
            for key in keys:
                current = current[key]
            return copy.deepcopy(current)

    def apply_value(self, json_path: str, value: Any) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            old_state = json.loads(json.dumps(self._state))

            keys = [k for k in json_path.strip("/").split("/") if k]
            if not keys:
                raise ValueError("path must not be empty")

            current: Any = self._state
            for key in keys[:-1]:
                if key not in current or not isinstance(current[key], dict):
                    current[key] = {}
                current = current[key]
            current[keys[-1]] = value

            self._version += 1
            self._validate_current_state()

            patch = jsonpatch.make_patch(old_state, self._state)
            return list(patch), self._version

    def replace_state(self, state: dict[str, Any]) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            old_state = json.loads(json.dumps(self._state))
            self._replace_internal(state)
            self._version += 1
            patch = jsonpatch.make_patch(old_state, self._state)
            return list(patch), self._version

    def _validate_current_state(self) -> None:
        try:
            validated = self._model_class.model_validate(self._state)
        except ValidationError as exc:
            raise ValueError(f"state validation failed: {exc}") from exc
        self._state = validated.model_dump(mode="json")
