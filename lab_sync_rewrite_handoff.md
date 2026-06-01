# lab_link Breaking Rewrite Handoff

This document is context for a new agent working in
`/Users/andrew/Documents/PROGRAM_LOCAL/lab_link`.

The user is the author of `lab_link` and is open to breaking changes. The goal
is to evolve `lab_link` from a simple FastAPI + Svelte sync helper into a
generic experiment-control synchronization library. The concrete motivating app
is dbay, but the implementation should stay generic enough for other lab
software systems.

The visual plan is also available at:

`/Users/andrew/Documents/PROGRAM_LOCAL/dbay/lab_link_rewrite_plan.html`

## Core Architectural Direction

Keep the server-authoritative model:

- WebSocket sends an initial `snapshot`.
- Server broadcasts JSON Patch updates.
- Clients send commands.
- Server validates commands, performs any side effects, commits state, then
  sends patch and command ack.

The TypeScript frontend should be rewritten around framework-agnostic sync
primitives first. Svelte support should become an optional adapter over those
primitives, not the main object model.

Recommended package shape:

- `lab-link/core`: WebSocket lifecycle, reconnects, snapshots, patches,
  command promises, command errors, streams, version tracking.
- `lab-link/model`: `SyncRuntime`, `SyncNode<T>`, path registration, synced
  field metadata, patch routing, field policy evaluation.
- `lab-link/svelte`: optional Svelte 5 helpers and runes-compatible base
  classes.
- `lab-link/react`: optional future adapter, not a design driver.

## Important dbay-Derived Requirements

Do not overfit to dbay names or modules, but make sure the generic design
supports these patterns:

- Authoritative state is a nested Pydantic model.
- State contains lists/arrays, including a list of module slots and nested
  channel arrays.
- State may include discriminated unions such as module variants.
- Frontend does not want plain JSON as the final live object model. It hydrates
  snapshots into TypeScript classes with methods, local UI fields, Svelte
  `$state` fields, and `$derived` fields.
- Some frontend fields are server-authoritative, while others are purely local
  UI state.
- Incoming server patches must sometimes be blocked while a user is editing a
  field.
- Commands can have hardware side effects and can fail.
- Hardware command failures must be visible on the frontend as banners, toasts,
  or inline node/channel errors.
- Existing REST-style command behavior often returns a canonical result, such
  as rounded/clamped values. WebSocket command acknowledgements should preserve
  that ability.

These are representative of many lab-control apps, not dbay-only needs.

## Backend Changes Needed

Prioritize backend foundations before the frontend rewrite if possible.

### 1. JSON Pointer Support

`StateStore.apply_value()` currently assumes object-shaped state. It must
support proper JSON Pointer traversal:

- Dict/object segments.
- List/array numeric indexes.
- Optional support for `-` append where appropriate.
- Escaped pointer segments: `~0` and `~1`.
- Clear errors for invalid paths.

Example path that must work:

```text
/data/2/vsource/channels/7/bias_voltage
```

Add tests with nested list state and discriminated-union-like fixtures.

### 2. Runtime Initial State

Do not require state to come from `Model()`. Support runtime registration:

```python
sync.register_state(SystemState, initial=global_state.system_state)
```

or an equivalent API that accepts a Pydantic instance or dict.

The decorator API can remain for demos:

```python
@sync.state
class AppState(BaseModel):
    ...
```

### 3. Explicit Mutation API

The attribute proxy is useful demo sugar, but command handlers and services need
explicit APIs:

```python
sync.set("/path/to/value", value)
sync.replace_state(next_state)

with sync.transaction(origin=ctx.client_id, request_id=ctx.request_id) as tx:
    tx.set("/path/a", value_a)
    tx.set("/path/b", value_b)
```

Transaction commit should:

1. Validate the resulting Pydantic state.
2. Compute one JSON Patch batch.
3. Increment version once.
4. Broadcast patch with metadata.
5. Allow the command ack to resolve after the patch is committed.

### 4. Command Context

Command handlers should be able to receive context:

```python
@sync.command
async def set_voltage(ctx: CommandContext, path: str, voltage: float):
    ...
```

Useful context fields:

- `client_id`
- `request_id`
- `command`
- maybe raw WebSocket/client metadata later

### 5. Command Result Payloads

`command_ack` should include optional result data:

```json
{
  "type": "command_ack",
  "requestId": "req-123",
  "command": "set_vsource_channel",
  "version": 45,
  "result": {
    "path": "/data/2/vsource/channels/7",
    "bias_voltage": 1.25
  }
}
```

This lets callers receive canonical rounded/clamped values or other command
outputs without doing a separate state read.

### 6. Structured Command Errors

Add a first-class `CommandError` or equivalent exception class:

```python
raise CommandError(
    code="hardware_timeout",
    message="The voltage source did not respond before the timeout.",
    detail="UDP timeout after 5.0 s",
    severity="error",
    display="banner",
    path="/data/2/vsource/channels/7",
    recoverable=True,
)
```

Wire format:

```json
{
  "type": "command_error",
  "requestId": "req-123",
  "command": "set_vsource_channel",
  "code": "hardware_timeout",
  "message": "The voltage source did not respond before the timeout.",
  "detail": "UDP timeout after 5.0 s",
  "severity": "error",
  "display": "banner",
  "recoverable": true,
  "path": "/data/2/vsource/channels/7",
  "originClientId": "client-a",
  "version": 44
}
```

`message` should be safe to display. `detail` can be optional and more
diagnostic.

Suggested fields:

- `code`: stable machine-readable error id.
- `message`: human-readable display text.
- `detail`: optional diagnostic detail.
- `severity`: `info`, `warning`, or `error`.
- `display`: `toast`, `banner`, `inline`, or similar display hint.
- `path`: optional related state path.
- `recoverable`: whether retry/editing might resolve it.

Do not swallow broad WebSocket exceptions silently. Log tracebacks server-side.

### 7. Patch Metadata

Patch messages should include enough metadata for client reconciliation:

```json
{
  "type": "patch",
  "version": 45,
  "patch": [{ "op": "replace", "path": "/data/0/core/name", "value": "DAC A" }],
  "originClientId": "client-a",
  "requestId": "req-123",
  "command": "set_module_name"
}
```

This supports frontend policies like source-aware guards, stale patch checks,
and optimistic update reconciliation.

## Frontend TypeScript Rewrite

### SyncRuntime

`SyncRuntime` should own:

- WebSocket connection lifecycle.
- Reconnects.
- Snapshot handlers.
- Patch handlers.
- Command send/ack/error promises.
- Global command error events.
- Node registry.
- Patch routing to the nearest registered node.
- Optional node error attachment by path.

Example:

```ts
const runtime = createSyncRuntime({ url });

runtime.onSnapshot((snapshot) => {
  systemState = new SystemStateModel(runtime, "", snapshot);
});

runtime.onCommandError((error) => {
  if (error.display === "banner") {
    uiErrors.showBanner(error.message, error);
  } else if (error.path) {
    runtime.setNodeError(error.path, error);
  } else {
    uiErrors.showToast(error.message, error.severity);
  }
});
```

### SyncNode<T>

Frontend apps should be able to hydrate snapshots into class instances:

```ts
abstract class SyncNode<TSnapshot> {
  readonly path: JsonPointer;
  protected readonly sync: SyncRuntime;

  constructor(sync: SyncRuntime, path: JsonPointer) {
    this.sync = sync;
    this.path = path;
    sync.attach(path, this);
  }

  abstract applySnapshot(snapshot: TSnapshot): void;

  protected defineFields<TNode extends object>(
    fields: SyncFieldMap<TNode>,
  ): SyncFieldMap<TNode> {
    return fields;
  }

  applyPatch(relativePath: string[], op: PatchOperation, meta: PatchMeta): void {
    this.sync.applyPatchToNode(this, relativePath, op, meta);
  }
}
```

Patch routing should find the nearest registered node:

```text
Patch path:
/data/2/vsource/channels/7/bias_voltage

Nearest node:
/data/2/vsource/channels/7

Relative path:
["bias_voltage"]
```

Then the runtime applies the field policy and assigns the property.

### Field Policies

Do not require user code to write path-switch logic like:

```ts
if (path === "bias_voltage") ...
```

Instead, classes should declare synced fields:

```ts
class ChSourceStateModel extends SvelteSyncNode<ChSourceState> {
  index = $state(0);
  bias_voltage = $state(0);
  activated = $state(false);
  heading_text = $state("");
  measuring = $state(false);

  editing = $state(false);
  heading_editing = $state(false);

  protected fields = this.defineFields<this>({
    index: { writable: false },
    bias_voltage: {
      blockWhen: () => this.editing,
      onBlocked: "queueLatest",
      validateRemote: (v) => typeof v === "number" && v >= -5 && v <= 5,
      coerceRemote: (v) => Math.round(Number(v) * 10000) / 10000,
      onApplied: () => this.voltageToTemp(),
      setVia: "set_vsource_channel",
    },
    activated: { setVia: "set_vsource_channel" },
    heading_text: {
      blockWhen: () => this.heading_editing,
      onBlocked: "queueLatest",
      setVia: "set_vsource_channel",
    },
    measuring: {},
  });
}
```

Initial policy subset:

- `blockWhen`: skip applying remote patch while true.
- `onBlocked`: `"drop"`, `"queueLatest"`, or callback.
- `validateRemote`: reject bad incoming values.
- `coerceRemote`: normalize values before validation/assignment.
- `onApplied`: local maintenance after assignment.
- `writable`: mark server-only/read-only fields.
- `setVia`: command name for writes that must go through the server.

Defer CRDTs and complex ownership models. Keep metadata extensible.

## Svelte Adapter

Keep Svelte support, but reposition it:

- Simple mode: `useSyncState()` for plain JSON dashboards and simple forms.
- Class mode: `SvelteSyncNode` or helpers for `.svelte.ts` files where classes
  use normal Svelte 5 `$state` and `$derived` fields.

The Svelte adapter should not be required by the core runtime.

## Testing Priorities

Backend:

- Dict/list JSON Pointer traversal.
- Escaped path segments.
- Invalid path errors.
- Runtime initial state registration.
- Transaction emits one patch batch and one version.
- Command ack includes result.
- `CommandError` serializes correctly.
- WebSocket command errors reach clients.
- Patch metadata includes origin/request/command when relevant.

Frontend:

- Runtime connects, reconnects, handles snapshot.
- Patch routing finds nearest node.
- Field policy assignment works.
- `blockWhen` drops or queues as configured.
- `validateRemote` and `coerceRemote`.
- Read-only fields.
- `setVia` command dispatch.
- Command ack result handling.
- Command error promise rejection and global error event.
- Node/path error attachment.

Integration-style fixtures:

- A state object with `data: Module[]`.
- Module variants with nested channel arrays.
- Patches to `/data/{slot}/...`.
- Patches to `/data/{slot}/vsource/channels/{i}/bias_voltage`.
- Module replacement at a slot.
- Linked-channel update as one grouped patch.

## Suggested Implementation Order

1. Backend state store: JSON Pointer lists, escaping, initial state registration.
2. Backend transactions and grouped patch commits.
3. Backend command context, result payloads, and structured command errors.
4. TypeScript core/model rewrite: `SyncRuntime`, `SyncNode`, field policies.
5. Svelte adapter on top of the model layer.
6. dbay-style fixtures and integration tests.
7. Only then integrate with dbay by replacing polling first. Convert mutations
   to WebSocket commands after snapshot/patch and command-error display are
   stable.

## What Not To Do

- Do not make `lab_link` dbay-specific.
- Do not make Svelte the only object model.
- Do not assume frontend live state is plain JSON.
- Do not force all command handlers through the attribute proxy.
- Do not commit server state before hardware side effects succeed unless there
  is an explicit optimistic/rollback mechanism.
- Do not silently swallow backend WebSocket or command exceptions.

