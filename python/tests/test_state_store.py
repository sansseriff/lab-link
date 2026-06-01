import pytest
from pydantic import BaseModel, Field

from lab_link import ptr
from lab_link.state_store import StateStore


class Simple(BaseModel):
    x: float = 0.0
    y: float = 0.0


class Nested(BaseModel):
    class Inner(BaseModel):
        speed: int = 0
        running: bool = False

    pump: Inner = Inner()
    temperature: float = 20.0


class Channel(BaseModel):
    bias_voltage: float = 0.0
    label: str = ""


class VSource(BaseModel):
    channels: list[Channel] = Field(default_factory=list)


class Module(BaseModel):
    kind: str
    vsource: VSource


class SystemState(BaseModel):
    data: list[Module] = Field(default_factory=list)
    escaped: dict[str, float] = Field(default_factory=dict)


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


def test_apply_value_nested_list_path():
    store = StateStore(
        SystemState,
        {
            "data": [
                {"kind": "empty", "vsource": {"channels": []}},
                {"kind": "vsource", "vsource": {"channels": [{"bias_voltage": 0.1, "label": "a"}]}},
            ],
            "escaped": {},
        },
    )
    patch, version = store.apply_value("/data/1/vsource/channels/0/bias_voltage", 1.25)
    assert store.get("/data/1/vsource/channels/0/bias_voltage") == 1.25
    assert version == 1
    assert patch == [
        {
            "op": "replace",
            "path": "/data/1/vsource/channels/0/bias_voltage",
            "value": 1.25,
        }
    ]


def test_ptr_builds_escaped_json_pointer():
    assert ptr("data", 2, "vsource", "channels", 7, "bias/voltage~raw") == (
        "/data/2/vsource/channels/7/bias~1voltage~0raw"
    )


def test_apply_value_list_append():
    store = StateStore(
        SystemState,
        {
            "data": [{"kind": "vsource", "vsource": {"channels": []}}],
            "escaped": {},
        },
    )
    store.apply_value(
        "/data/0/vsource/channels/-",
        {"bias_voltage": 0.5, "label": "new"},
    )
    assert store.get("/data/0/vsource/channels/0/label") == "new"


def test_escaped_pointer_segments():
    store = StateStore(SystemState, {"data": [], "escaped": {"a/b": 1.0, "tilde~key": 2.0}})
    store.apply_value("/escaped/a~1b", 3.0)
    store.apply_value("/escaped/tilde~0key", 4.0)
    assert store.get("/escaped/a~1b") == 3.0
    assert store.get("/escaped/tilde~0key") == 4.0


def test_invalid_list_path_errors_without_mutating():
    store = StateStore(
        SystemState,
        {
            "data": [{"kind": "vsource", "vsource": {"channels": [{"bias_voltage": 0.0, "label": ""}]}}],
            "escaped": {},
        },
    )
    with pytest.raises(IndexError):
        store.apply_value("/data/0/vsource/channels/9/bias_voltage", 1.0)
    assert store.get("/data/0/vsource/channels/0/bias_voltage") == 0.0


def test_apply_values_transaction_one_version():
    store = StateStore(Nested, {"pump": {"speed": 0, "running": False}, "temperature": 20.0})
    patch, version = store.apply_values([
        ("/pump/speed", 1500),
        ("/pump/running", True),
    ])
    assert version == 1
    assert store.snapshot()["pump"] == {"speed": 1500, "running": True}
    assert len(patch) == 2


def test_validation_error():
    store = StateStore(Simple, {"x": 0.0, "y": 0.0})
    with pytest.raises(ValueError):
        store.apply_value("/x", "not_a_number")
