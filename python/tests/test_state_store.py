import pytest
from pydantic import BaseModel

from lab_sync.state_store import StateStore


class Simple(BaseModel):
    x: float = 0.0
    y: float = 0.0


class Nested(BaseModel):
    class Inner(BaseModel):
        speed: int = 0
        running: bool = False

    pump: Inner = Inner()
    temperature: float = 20.0


def test_snapshot_returns_copy():
    store = StateStore(Simple, {"x": 1.0, "y": 2.0})
    snap = store.snapshot()
    snap["x"] = 999
    assert store.snapshot()["x"] == 1.0


def test_apply_value_scalar():
    store = StateStore(Simple, {"x": 0.0, "y": 0.0})
    patch, version = store.apply_value("/x", 5.0)
    assert store.snapshot()["x"] == 5.0
    assert version == 1
    assert any(op["path"] == "/x" for op in patch)


def test_apply_value_nested():
    store = StateStore(Nested, {"pump": {"speed": 0, "running": False}, "temperature": 20.0})
    patch, version = store.apply_value("pump/speed", 1500)
    assert store.get("pump/speed") == 1500
    assert version == 1


def test_version_increments():
    store = StateStore(Simple, {"x": 0.0, "y": 0.0})
    assert store.version() == 0
    store.apply_value("/x", 1.0)
    assert store.version() == 1
    store.apply_value("/y", 2.0)
    assert store.version() == 2


def test_replace_state():
    store = StateStore(Simple, {"x": 0.0, "y": 0.0})
    patch, version = store.replace_state({"x": 10.0, "y": 20.0})
    assert store.snapshot()["x"] == 10.0
    assert version == 1


def test_get_nested():
    store = StateStore(Nested, {"pump": {"speed": 500, "running": True}, "temperature": 22.0})
    assert store.get("pump/speed") == 500
    assert store.get("temperature") == 22.0


def test_validation_error():
    store = StateStore(Simple, {"x": 0.0, "y": 0.0})
    with pytest.raises(ValueError):
        store.apply_value("/x", "not_a_number")
