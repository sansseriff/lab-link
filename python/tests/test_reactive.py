"""Tests for the reactive state engine.

The core oracle, used throughout: after every flush, the StateStore mirror
(evolved incrementally by patch ops) must equal a fresh
``model_dump(mode="json")`` of the live bound model. The two diverge only if
the recorded ops were wrong.
"""

import asyncio
import random
import threading
from typing import Annotated, Literal, Union

import jsonpatch
import pytest
from pydantic import BaseModel, Field, ValidationError
from starlette.testclient import TestClient

from lab_link import LabSync, ReactiveDict, ReactiveList, ReactiveModel


class Channel(ReactiveModel):
    name: str = "ch"
    voltage: float = 0.0
    active: bool = False


class ModuleA(ReactiveModel):
    type: Literal["a"] = "a"
    channels: list[Channel] = Field(default_factory=lambda: [Channel(), Channel()])


class ModuleB(ReactiveModel):
    type: Literal["b"] = "b"
    gain: float = 1.0


Module = Annotated[Union[ModuleA, ModuleB], Field(discriminator="type")]


class Root(ReactiveModel):
    label: str = "root"
    count: int = 0
    main: Channel = Field(default_factory=Channel)
    data: list[Module] = Field(default_factory=list)
    tags: dict[str, str] = Field(default_factory=dict)
    slot: Module | None = None


class _Recorder:
    """Replaces ConnectionManager.broadcast_patch to capture patch messages."""

    def __init__(self) -> None:
        self.messages = []

    async def broadcast_patch(self, patch, version, **meta):
        self.messages.append({"patch": patch, "version": version, **meta})


def _bound() -> tuple[LabSync, Root]:
    sync = LabSync()
    state = sync.bind_state(Root())
    return sync, state


async def _settle(sync: LabSync) -> None:
    """Let the per-tick call_soon flush run, then await broadcast tasks."""
    await asyncio.sleep(0)
    await sync._flush_pending_patch_tasks()


def _assert_mirror(sync: LabSync, state: Root) -> None:
    assert sync._store.snapshot() == state.model_dump(mode="json")


# ── batching, metadata, equality ──────────────────────────────────────────────


@pytest.mark.asyncio
async def test_same_tick_mutations_batch_into_one_patch():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.count = 1
        state.label = "x"
        state.main.voltage = 1.5
        await _settle(sync)

        assert len(recorder.messages) == 1
        msg = recorder.messages[0]
        assert msg["version"] == 1
        assert msg["patch"] == [
            {"op": "replace", "path": "/count", "value": 1},
            {"op": "replace", "path": "/label", "value": "x"},
            {"op": "replace", "path": "/main/voltage", "value": 1.5},
        ]
        _assert_mirror(sync, state)


@pytest.mark.asyncio
async def test_consecutive_writes_to_same_path_coalesce():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.count = 1
        state.count = 2
        state.count = 3
        await _settle(sync)

        assert len(recorder.messages) == 1
        assert recorder.messages[0]["patch"] == [
            {"op": "replace", "path": "/count", "value": 3}
        ]


@pytest.mark.asyncio
async def test_equal_assignment_emits_nothing():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.count = 0          # unchanged scalar
        state.label = "root"     # unchanged string
        await _settle(sync)

        assert recorder.messages == []
        assert sync._store.version() == 0


@pytest.mark.asyncio
async def test_assignment_validates_and_coerces():
    sync, state = _bound()
    async with sync.lifespan():
        with pytest.raises(ValidationError):
            state.count = "not a number"
        state.count = "7"  # coercible — post-coercion value recorded
        await _settle(sync)
        assert state.count == 7
        _assert_mirror(sync, state)


@pytest.mark.asyncio
async def test_batch_emits_one_combined_patch():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        with sync.batch():
            state.count = 5
            state.data.append(ModuleB(gain=2.0))
            state.tags["k"] = "v"
        # batch flushes synchronously on exit; only the broadcast is async
        await sync._flush_pending_patch_tasks()

        assert len(recorder.messages) == 1
        assert recorder.messages[0]["version"] == 1
        _assert_mirror(sync, state)


# ── list reactivity ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_list_field_is_wrapped_and_rewrapped_on_assignment():
    sync, state = _bound()
    assert isinstance(state.data, ReactiveList)
    state.data = [ModuleB(gain=3.0)]
    assert isinstance(state.data, ReactiveList)
    assert isinstance(state.tags, ReactiveDict)
    _assert_mirror(sync, state)


@pytest.mark.asyncio
async def test_list_ops_emit_correct_ops_and_rekey():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        a, b, c = ModuleB(gain=1.0), ModuleB(gain=2.0), ModuleB(gain=3.0)
        state.data.append(a)
        state.data.append(b)
        await _settle(sync)
        state.data.insert(0, c)          # a, b shift right
        await _settle(sync)

        # after re-keying, mutating the shifted module records its new index
        a.gain = 10.0
        await _settle(sync)
        assert recorder.messages[-1]["patch"] == [
            {"op": "replace", "path": "/data/1/gain", "value": 10.0}
        ]
        _assert_mirror(sync, state)

        state.data.pop(0)                # a, b shift back left
        await _settle(sync)
        b.gain = 20.0
        await _settle(sync)
        assert recorder.messages[-1]["patch"] == [
            {"op": "replace", "path": "/data/1/gain", "value": 20.0}
        ]
        _assert_mirror(sync, state)

        state.data.remove(a)
        del state.data[0]
        await _settle(sync)
        assert state.data == []
        _assert_mirror(sync, state)


@pytest.mark.asyncio
async def test_clear_then_append_replays_correctly():
    sync, state = _bound()
    async with sync.lifespan():
        state.data.append(ModuleB(gain=1.0))
        await _settle(sync)

        before = sync._store.snapshot()
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        # clear + append in the same tick: the whole-list replace must be
        # snapshotted eagerly or the append would replay twice
        state.data.clear()
        state.data.append(ModuleB(gain=2.0))
        await _settle(sync)

        _assert_mirror(sync, state)
        replayed = jsonpatch.apply_patch(before, recorder.messages[0]["patch"])
        assert replayed == sync._store.snapshot()


@pytest.mark.asyncio
async def test_whole_subtree_assignment_is_one_replace_op():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.data = [ModuleA(), ModuleB(gain=9.0)]
        await _settle(sync)

        assert len(recorder.messages) == 1
        (op,) = recorder.messages[0]["patch"]
        assert op["op"] == "replace"
        assert op["path"] == "/data"
        _assert_mirror(sync, state)

        # children of the new subtree are adopted: mutations record
        state.data[1].gain = 10.0
        await _settle(sync)
        assert recorder.messages[-1]["patch"][0]["path"] == "/data/1/gain"
        _assert_mirror(sync, state)


# ── dict reactivity ───────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_dict_ops():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.tags["a"] = "1"
        await _settle(sync)
        assert recorder.messages[-1]["patch"] == [
            {"op": "add", "path": "/tags/a", "value": "1"}
        ]

        state.tags["a"] = "2"
        state.tags.update({"b": "3"})
        await _settle(sync)
        del state.tags["a"]
        state.tags.setdefault("c", "4")
        await _settle(sync)
        _assert_mirror(sync, state)

        state.tags.clear()
        await _settle(sync)
        assert state.tags == {}
        _assert_mirror(sync, state)

        with pytest.raises(TypeError):
            state.tags[3] = "x"


@pytest.mark.asyncio
async def test_dict_key_escaping():
    sync, state = _bound()
    async with sync.lifespan():
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        state.tags["a/b~c"] = "v"
        await _settle(sync)
        assert recorder.messages[0]["patch"][0]["path"] == "/tags/a~1b~0c"
        _assert_mirror(sync, state)


# ── orphan semantics ──────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_orphaned_subtree_writes_are_dropped():
    sync, state = _bound()
    async with sync.lifespan():
        state.data.append(ModuleA())
        await _settle(sync)
        old = state.data[0]

        state.data[0] = ModuleB(gain=5.0)
        await _settle(sync)

        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        # old module is no longer part of the document; its writes vanish
        old.channels[0].voltage = 99.0
        old.channels.append(Channel())
        await _settle(sync)

        assert recorder.messages == []
        _assert_mirror(sync, state)


@pytest.mark.asyncio
async def test_popped_child_can_be_readopted():
    sync, state = _bound()
    async with sync.lifespan():
        module = ModuleA()
        state.data.append(module)
        await _settle(sync)

        moved = state.data.pop(0)
        assert moved is module
        state.slot = moved
        await _settle(sync)

        moved.channels[0].voltage = 3.3
        await _settle(sync)
        assert sync._store.get("/slot/channels/0/voltage") == 3.3
        _assert_mirror(sync, state)


# ── discriminated unions ──────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_union_slot_coerces_dict_via_discriminator():
    sync, state = _bound()
    async with sync.lifespan():
        state.slot = {"type": "b", "gain": 4.0}
        await _settle(sync)
        assert isinstance(state.slot, ModuleB)
        _assert_mirror(sync, state)

        # the adopted child is the coerced model: mutations record
        state.slot.gain = 8.0
        await _settle(sync)
        assert sync._store.get("/slot/gain") == 8.0
        _assert_mirror(sync, state)


# ── thread guard ──────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_cross_thread_mutation_raises():
    sync, state = _bound()
    async with sync.lifespan():
        with pytest.raises(RuntimeError, match="thread"):
            await asyncio.to_thread(setattr, state, "count", 5)
        # value may or may not have been set by pydantic before the guard
        # fired; what matters is the loud failure and that the loop thread
        # still works afterwards:
        state.count = 6
        await _settle(sync)
        _assert_mirror(sync, state)


# ── pre-lifespan mutations ────────────────────────────────────────────────────


def test_pre_lifespan_mutations_apply_silently():
    sync, state = _bound()
    state.count = 42
    state.data.append(ModuleB(gain=1.5))
    assert sync._store.version() == 0
    assert sync._store.snapshot() == state.model_dump(mode="json")


# ── load_state ────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_load_state_swaps_in_place_and_emits_one_replace():
    sync, state = _bound()
    async with sync.lifespan():
        state.data.append(ModuleA())
        await _settle(sync)

        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        saved = {
            "label": "restored",
            "count": 9,
            "main": {"name": "m", "voltage": 1.0, "active": True},
            "data": [{"type": "b", "gain": 2.5}],
            "tags": {"k": "v"},
            "slot": None,
        }
        before = state  # identity must be preserved
        ops, version = sync.load_state(saved)
        await sync._flush_pending_patch_tasks()

        assert sync.state is before
        assert state.label == "restored"
        assert isinstance(state.data[0], ModuleB)
        assert ops == [{"op": "replace", "path": "", "value": saved}]
        assert len(recorder.messages) == 1
        assert recorder.messages[0]["version"] == version
        _assert_mirror(sync, state)

        # the restored tree is fully reactive
        state.data[0].gain = 7.0
        await _settle(sync)
        assert sync._store.get("/data/0/gain") == 7.0
        _assert_mirror(sync, state)


def test_load_state_before_lifespan_is_silent():
    sync, state = _bound()
    sync.load_state({"label": "early", "count": 1, "main": {}, "data": [], "tags": {}, "slot": None})
    assert state.label == "early"
    assert sync._store.version() == 0
    assert sync._store.snapshot() == state.model_dump(mode="json")


# ── unsupported types fail loudly ─────────────────────────────────────────────


def test_plain_basemodel_child_rejected():
    class Plain(BaseModel):
        x: int = 0

    class Bad(ReactiveModel):
        child: Plain = Field(default_factory=Plain)

    with pytest.raises(TypeError, match="ReactiveModel"):
        Bad()


def test_set_field_rejected():
    class Bad(ReactiveModel):
        items: set[int] = Field(default_factory=set)

    with pytest.raises(TypeError, match="set"):
        Bad()


def test_double_bind_rejected():
    sync, state = _bound()
    with pytest.raises(RuntimeError):
        LabSync().bind_state(state)
    with pytest.raises(RuntimeError):
        sync.bind_state(Root())


# ── publish() oracle / escape hatch ───────────────────────────────────────────


@pytest.mark.asyncio
async def test_publish_is_noop_when_reactive_engine_kept_up():
    sync, state = _bound()
    async with sync.lifespan():
        state.count = 3
        await _settle(sync)
        version = sync._store.version()
        patch, new_version = sync.publish()
        assert patch == []
        assert new_version == version


# ── ack ordering over a real websocket ────────────────────────────────────────


def test_command_patches_precede_ack_with_post_command_version():
    sync = LabSync()
    state = sync.bind_state(Root())

    @sync.command
    async def configure(gain: float):
        state.data.append(ModuleB(gain=gain))
        state.count += 1
        await asyncio.sleep(0)  # patches flush mid-command; ack must still trail
        state.main.voltage = gain * 2
        return {"ok": True}

    app = sync.create_app()
    with TestClient(app) as client:
        with client.websocket_connect("/sync/ws") as ws:
            assert ws.receive_json()["type"] == "snapshot"
            ws.send_json({
                "type": "command",
                "command": "configure",
                "params": {"gain": 2.0},
                "requestId": "req-r1",
            })
            messages = []
            while True:
                msg = ws.receive_json()
                messages.append(msg)
                if msg["type"] == "command_ack":
                    break

            ack = messages[-1]
            patches = [m for m in messages[:-1] if m["type"] == "patch"]
            assert patches, "expected at least one patch before the ack"
            for patch in patches:
                assert patch["originClientId"] == ack.get("originClientId", patch["originClientId"])
                assert patch["requestId"] == "req-r1"
                assert patch["command"] == "configure"
            assert ack["version"] == patches[-1]["version"]
            assert ack["result"] == {"ok": True}


# ── fuzz: random mutation sequences against the dump oracle ──────────────────


def _random_mutation(rng: random.Random, state: Root) -> None:
    choice = rng.randrange(12)
    if choice == 0:
        state.count = rng.randrange(100)
    elif choice == 1:
        state.label = rng.choice(["a", "b", "c", "d"])
    elif choice == 2:
        state.main.voltage = round(rng.uniform(0, 5), 3)
    elif choice == 3:
        module = rng.choice([ModuleA(), ModuleB(gain=rng.random())])
        state.data.append(module)
    elif choice == 4 and state.data:
        state.data.pop(rng.randrange(len(state.data)))
    elif choice == 5 and state.data:
        state.data[rng.randrange(len(state.data))] = ModuleB(gain=rng.random())
    elif choice == 6 and state.data:
        state.data.insert(rng.randrange(len(state.data) + 1), ModuleA())
    elif choice == 7:
        module = next((m for m in state.data if isinstance(m, ModuleA)), None)
        if module is not None and module.channels:
            ch = module.channels[rng.randrange(len(module.channels))]
            ch.voltage = round(rng.uniform(0, 5), 3)
            ch.active = rng.random() < 0.5
    elif choice == 8:
        state.tags[rng.choice("xyz")] = str(rng.randrange(10))
    elif choice == 9 and state.tags:
        state.tags.pop(rng.choice(list(state.tags)))
    elif choice == 10:
        state.slot = rng.choice([None, {"type": "b", "gain": rng.random()}])
    elif choice == 11 and len(state.data) > 3:
        state.data.clear()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed", [1, 2, 3])
async def test_fuzz_mirror_always_matches_model_dump(seed):
    rng = random.Random(seed)
    sync, state = _bound()
    async with sync.lifespan():
        snapshots = [sync._store.snapshot()]
        recorder = _Recorder()
        sync._conn_manager.broadcast_patch = recorder.broadcast_patch

        for _ in range(60):
            for _ in range(rng.randrange(1, 5)):
                _random_mutation(rng, state)
            await _settle(sync)
            _assert_mirror(sync, state)
            snapshots.append(sync._store.snapshot())

        # replaying every emitted patch over the initial snapshot must
        # reproduce the final state
        doc = snapshots[0]
        for message in recorder.messages:
            doc = jsonpatch.apply_patch(doc, message["patch"])
        assert doc == sync._store.snapshot()
        # versions strictly monotonic
        versions = [m["version"] for m in recorder.messages]
        assert versions == sorted(set(versions))
