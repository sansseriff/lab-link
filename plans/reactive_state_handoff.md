# Handoff: Reactive state models for lab-link

**Goal:** mutating a bound pydantic model on the backend —

```python
channel.activated = True
```

— automatically validates the assignment, records a JSON Patch op, batches it
with other ops from the same event-loop tick, broadcasts one versioned patch
message to all websocket clients, and feeds debounced persistence. No manual
`sync.set(...)`, no publish helpers, no second copy of the state that the
application must keep in agreement with the first.

This replaces the current design in which applications hold live pydantic
objects *and* lab-link's `StateStore` holds a separate JSON dict, with the
application responsible for mirroring every mutation across (the "split
brain"). The reference consumer is **dbay**
(`~/Documents/PROGRAM_LOCAL/dbay/software/gui/backend`), whose
`backend/sync.py` currently contains ~100 lines of hand-written field-by-field
publish helpers that this change deletes.

## Decision already made: pydantic becomes lab-link's foundation

Today pydantic is "a supported input format" (state models must be
`BaseModel`s, but the store works on dicts). After this change, application
state must subclass `lab_link.ReactiveModel` (a `BaseModel` subclass), making
pydantic load-bearing: per-field validation on assignment
(`validate_assignment`), discriminated unions for polymorphic nodes, private
attributes for tree bookkeeping, and `model_dump(mode="json")` as the wire
boundary. This is intentional and approved by the project owner. Pin
`pydantic>=2.7` and treat pydantic-version compatibility as part of CI.

## Non-negotiable invariants

The wire protocol must not change. The JS client (`js/`), Python client
(`python/src/lab_link/client.py`), and existing consumers keep working
unmodified:

1. **Snapshot on connect**: `{"type": "snapshot", "data": ..., "version": n}`.
2. **Patches**: `{"type": "patch", "patch": [RFC-6902 ops], "version": n,
   "originClientId"?, "requestId"?, "command"?}` — one message per logical
   change set, monotonically increasing version.
3. **Ack ordering**: `command_ack` for a command must be sent *after* every
   patch message produced by that command, and must carry the post-command
   version. (Today this is enforced via `patch_queue.join()` +
   `_flush_pending_patch_tasks()` in `core.py::_dispatch_command`; the
   reactive flush must provide the same guarantee.)
4. **Patch metadata**: patches caused by a command carry that command's
   `originClientId`/`requestId`/`command` (currently via the
   `_current_command_context` contextvar). Clients rely on this for echo
   suppression and optimistic-update reconciliation.
5. Streams, `CommandError` shape, and persistence behavior are untouched.

## Architecture

### New module: `reactive.py`

**`ReactiveModel(BaseModel)`** — exported; all application state models
subclass it.

```python
class ReactiveModel(BaseModel):
    model_config = ConfigDict(validate_assignment=True)
    _ll_parent: "ReactiveModel | ReactiveList | None" = PrivateAttr(default=None)
    _ll_key: str | int | None = PrivateAttr(default=None)
    _ll_sink: "ChangeSink | None" = PrivateAttr(default=None)   # set on root only

    def __setattr__(self, name, value):
        if name.startswith("_"):
            return super().__setattr__(name, value)
        old = getattr(self, name, _MISSING)
        super().__setattr__(name, value)        # pydantic validates assignment
        new = getattr(self, name)               # post-coercion value
        if old is _MISSING or old != new:
            sink = self._ll_find_sink()
            if sink is not None:
                _adopt(new, parent=self, key=name)   # re-parent if model/list
                _orphan(old)
                sink.record(self._ll_pointer() + "/" + _escape(name), new)
```

- `_ll_pointer()` walks `_ll_parent`/`_ll_key` up to the root, building an
  escaped JSON Pointer (reuse `pointer.py`; remember `~0`/`~1` escaping).
- Private attrs are excluded from `model_dump` automatically, so
  serialization, persistence, and downstream TypeScript generation are
  unaffected.
- Values recorded into the sink are serialized lazily at flush time with
  `model_dump(mode="json")` for model/list values, so a whole-subtree
  assignment becomes one `replace` op with a JSON value.

**`ReactiveList`** — a `list` subclass (or `MutableSequence`) that intercepts
`__setitem__`, `append`, `insert`, `pop`, `remove`, `clear`, `extend`,
`__delitem__`. Each structural mutation:

- emits the corresponding patch op(s) (`replace` for `lst[i] = x`, `add` for
  insert/append, `remove` for deletions),
- adopts new children (sets backrefs, recursively),
- orphans removed/replaced children,
- **re-keys subsequent children** after insert/remove (their `_ll_key`
  indices shift). dbay only ever replaces whole slots in a fixed-length list,
  but the library must be correct for insert/remove too.

Pydantic validates list fields into plain lists; convert to `ReactiveList`
in `ReactiveModel.model_post_init` (and again whenever a list field is
re-assigned). Nested `list[list[...]]` and `dict` fields: implement
`ReactiveDict` symmetrically or explicitly raise `TypeError` at bind time with
a clear message — do not silently lose reactivity. (dbay's tree is models and
lists only.)

**`ChangeSink`** — per-`LabSync` buffer + flusher.

- `record(path, value)`: append to buffer; on first record of a tick,
  schedule `loop.call_soon(self.flush)`. If no running loop (mutations during
  module import / before lifespan), apply the change to the initial state
  silently — there are no clients yet, and the bound model *is* the state.
- `flush()`: drain buffer → apply ops to the mirror dict → increment version
  once → broadcast **one** patch message (with `PatchMetadata` captured at
  record time from the command contextvar) → `persistence.save_debounced`.
  Consecutive ops on the same path within a batch coalesce to the last value.
- `async def drain()`: awaited by `_dispatch_command` before sending
  `command_ack` (invariant 3). Replaces `patch_queue.join()` +
  `_flush_pending_patch_tasks` for the reactive path.
- **Thread guard**: `record()` must verify it is running on the owning event
  loop's thread; if not, raise `RuntimeError` with a message pointing to
  `call_soon_threadsafe` / "mutate after awaiting `asyncio.to_thread`, not
  inside it". A silent cross-thread mutation corrupts batching and ordering —
  fail loudly.

### `LabSync` API changes (`core.py`)

```python
sync = LabSync()
state = MySystemState(...)            # MySystemState subclasses ReactiveModel
sync.bind_state(state)                # walks tree, sets backrefs, attaches sink
sync.state                            # → the bound ReactiveModel instance (typed!)
```

- `bind_state(instance)` infers the model class, builds the mirror dict via
  `model_dump(mode="json")`, walks the tree adopting every node, attaches the
  `ChangeSink`.
- `sync.load_state(data: dict | BaseModel)` — bulk replacement for restore
  paths: validate against the bound class, swap the contents of the bound
  instance **in place** (field-by-field under a suspended sink, so existing
  references like dbay's `global_state.system_state` stay valid), emit a
  single whole-document `replace` patch. This subsumes today's
  `replace_state` for the restore use case.
- `with sync.batch():` — suspend per-tick flushing; emit one combined patch
  on exit. (Replaces the public `transaction()` API; keep `transaction()` as
  a deprecated alias during migration.)
- Keep `sync.get/set` working against the mirror during migration; mark
  deprecated. Keep `register_state` as a deprecated shim that calls
  `bind_state` when given an instance.
- **Delete `proxy.py`** (`StateProxy`, `NestedProxy`, `SyncState`) and the
  `_drain_patch_queue` task. `sync.state` becomes a plain property returning
  the bound instance — better for type checkers than the current
  decorator/proxy hybrid.
- `StateStore` survives as the **mirror**: snapshot source for new
  connections and `GET /state`, updated incrementally by `ChangeSink.flush`
  applying pointer-sets (no more whole-tree `model_validate` per write —
  validation already happened on assignment; keep full validation only in
  `load_state`).

### Orphan semantics (important)

After `state.data[2] = new_module`, the *old* module object may still be
referenced by application code (dbay's adc4D polling loop holds module
references across `await`s). Orphaning clears the old subtree's backrefs, so
later mutations on it find no sink and are **silently dropped** (debug-log
them). This is the correct behavior — the old object is no longer part of the
state document — and it must be tested explicitly.

## Costs / problems lab-link must solve (checklist)

| # | Problem | Resolution |
|---|---|---|
| 1 | Batching & ack ordering | `ChangeSink` per-tick coalescing; `drain()` awaited before `command_ack`; updaters flush per tick |
| 2 | Patch metadata on reactive writes | capture `_current_command_context` at `record()` time; one batch never mixes two commands' metadata (commands are dispatched sequentially per connection; assert at flush) |
| 3 | List reactivity | `ReactiveList` with re-keying on insert/remove; wrap in `model_post_init` |
| 4 | Subtree replacement | adopt new / orphan old / single `replace` op with dumped JSON value |
| 5 | Cross-thread mutation | loud `RuntimeError` from the thread guard |
| 6 | Bulk load / restore | `sync.load_state()` in-place swap under suspended sink |
| 7 | Pre-lifespan mutations | sink applies silently to mirror (no loop, no clients) |
| 8 | Equality short-circuit | compare post-coercion values; document that `float` churn (e.g. 10 Hz ADC readings) intentionally emits per change |
| 9 | Unsupported containers (`dict`, `set`, tuples of models) | implement `ReactiveDict` or raise clearly at `bind_state` |
| 10 | pydantic coupling | rely only on documented v2 surface: `validate_assignment`, `PrivateAttr`, `model_post_init`, `model_dump`; CI against current pydantic |
| 11 | Discriminated unions | assigning a dict to a union-typed slot coerces via the discriminator; the adopted child is the coerced model — test with a tagged union like dbay's `GenericModule` |
| 12 | Migration safety | Phase 0 below gives a trivially-correct reference engine; property-test the reactive engine against it |

## Implementation phases

**Phase 0 — dump-and-diff baseline (~30 lines, ship first).** Add
`sync.publish()`: `model_dump` the bound instance, `StateStore.replace_state`
against the mirror (this already computes a minimal JSON Patch via
`jsonpatch.make_patch`), broadcast. Auto-call it after each command handler
and updater tick. This alone kills the split brain for consumers and is the
oracle for Phase 1 testing.

**Phase 1 — reactive engine.** `ReactiveModel`, `ReactiveList`, `ChangeSink`,
`bind_state`, `load_state`, `batch()`, thread guard, orphaning.

**Phase 2 — cleanup.** Delete `proxy.py` and the patch queue; deprecate
`register_state`/`set`/`transaction`; update README, docs site, `example.py`,
and the JS-side docs (JS changes: none).

**Testing.** Property tests: random mutation sequences applied to a
reactive-bound model and to a plain copy; after each flush, the reactive
mirror must equal `plain_copy.model_dump(mode="json")`, and replaying the
emitted patches over the previous snapshot must reproduce it. Plus targeted
tests for each row of the cost table, and an ack-ordering integration test
over a real websocket. Run the existing `python/tests` and `js` tests
unmodified — they encode the protocol invariants.

Note: the FastAPI→Starlette refactor (see
`~/Documents/PROGRAM_LOCAL/dbay/lab-link-starlette-refactor.md`) is orthogonal
to this work and can land before or after.

## What the consumer looks like afterward: dbay

Models switch base class (one-line changes in `backend/module.py`,
`backend/addons/*.py`, `backend/state.py`):

```python
from lab_link import ReactiveModel

class ChSourceState(ReactiveModel):
    index: int
    bias_voltage: float
    activated: bool
    heading_text: str
    measuring: bool
```

`backend/sync.py` shrinks from ~130 lines to roughly:

```python
sync = LabSync(persist=PERSIST_ENABLED, db_url=f"sqlite:///{PERSIST_DB_PATH}")
sync.bind_state(global_state.system_state)

@router.websocket("/sync/ws")
async def sync_websocket(websocket: WebSocket) -> None:
    await sync.handle_ws(websocket)
```

Every `publish_vsource_channel` / `publish_vsense_voltages` /
`replace_sync_state` / `set_sync_value` helper is deleted. Command handlers
just mutate:

```python
@sync.command
async def set_adc4d_vsense(ctx: CommandContext, **params):
    change = VsenseChange(**params)
    module = _get_adc4d(change.module_index)
    voltage = await asyncio.to_thread(controller.readChannelVoltage, ...)
    channel = module.vsense.channels[change.index]
    channel.name = change.name          # ← each assignment recorded,
    channel.measuring = change.measuring  #   batched into one patch,
    channel.voltage = voltage             #   acked after broadcast
    return change.model_dump(mode="json")
```

The polling loop's `module.vsense.channels[index].voltage = v` writes sync
automatically; if the slot was re-initialized mid-loop, the orphaned module
swallows the writes harmlessly (the loop already checks `core.type` and
exits). Slot replacement in `GlobalState.add_module`
(`self.system_state.data[slot] = model`) emits one `replace` patch and adopts
the new module — no `replace_sync_state()` call.

The persistence restore (`restore_global_state_from_persistence`) simplifies
to: read saved snapshot → `sync.load_state(saved)` → rebuild hardware
controllers for occupied slots → reset transient flags (`activated`,
`measuring`, `polling.running`, live voltages) by ordinary attribute writes,
which broadcast like everything else. `global_state.system_state` keeps its
identity (in-place swap), so no other dbay code changes.
