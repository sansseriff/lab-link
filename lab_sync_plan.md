# Plan: `lab_sync` — Generic Server-Authoritative Sync Library

## Context

The `server-lab-json-patch` repo bakes domain-specific models (pumps, sensors, alerts) into its sync infrastructure. `lab_sync` extracts and generalizes that machinery into an installable library where adding real-time state to a FastAPI app is as simple as decorating a Pydantic model and some functions. The target audience is scientists who know Python but not WebSocket/concurrency internals.

**Both a Python package and a JavaScript package will be created.** The Python package is the server; the JS package is the browser client with a Svelte 5 adapter (primary) and a React adapter stub.

Source files to generalize:
- [`server-lab-json-patch/app/state_store.py`](../server-lab-json-patch/app/state_store.py) → `StateStore[T]`
- [`server-lab-json-patch/app/main.py`](../server-lab-json-patch/app/main.py) → `ConnectionManager`, command dispatch, lifespan
- [`server-lab-json-patch/web/src/lib/state/websocket-manager.svelte.ts`](../server-lab-json-patch/web/src/lib/state/websocket-manager.svelte.ts) → `SyncClient`
- [`server-lab-json-patch/web/src/lib/state/reactive-base.svelte.ts`](../server-lab-json-patch/web/src/lib/state/reactive-base.svelte.ts) → Svelte adapter pattern

---

## Repository Structure

```
lab_sync/                          ← monorepo root
├── README.md
├── .gitignore
├── python/                        ← uv-managed Python library
│   ├── pyproject.toml             # name="lab-sync", src layout
│   ├── uv.lock
│   ├── src/
│   │   └── lab_sync/
│   │       ├── __init__.py        # exports: LabSync
│   │       ├── core.py            # LabSync class + decorator logic + lifespan
│   │       ├── state_store.py     # Generic StateStore[T: BaseModel]
│   │       ├── proxy.py           # StateProxy + NestedProxy
│   │       ├── connection_manager.py
│   │       ├── persistence.py     # Optional SQLite (sqlmodel)
│   │       └── router.py          # (or inline in core.py)
│   └── tests/
│       ├── test_state_store.py
│       ├── test_proxy.py
│       └── test_core.py
└── js/                            ← bun-managed TypeScript library
    ├── package.json               # name="lab-sync"
    ├── bun.lock
    ├── tsconfig.json
    ├── tsup.config.ts             # 3 entry points: index, svelte/, react/
    ├── src/
    │   ├── index.ts               # exports SyncClient, createSyncClient
    │   ├── client.ts              # SyncClient class (state + stream dispatch)
    │   ├── apply-patch.ts         # minimal RFC 6902 applier (zero deps)
    │   ├── stream-handle.ts       # StreamHandle class (append + replace modes)
    │   ├── svelte/
    │   │   └── index.ts           # useSyncState(), useStream() → $state / canvas
    │   └── react/
    │       └── index.ts           # useSyncState() stub
    └── tests/
        ├── client.test.ts
        └── stream.test.ts
```

---

## Wire Protocol

Use `snapshot` (not `initial_state` from source repo — semantically accurate; also sent on reconnect).

**Server → Client — state channel:**
```jsonc
{ "type": "snapshot", "data": { ...fullState }, "version": 42 }
{ "type": "patch", "patch": [{"op": "replace", "path": "/temperature", "value": 22.5}], "version": 43 }
{ "type": "command_ack", "command": "set_temperature", "requestId": "abc", "version": 43 }
{ "type": "command_error", "command": "set_temperature", "requestId": "abc", "error": "out of range" }
```

**Server → Client — stream channel (same WebSocket, different `type`):**
```jsonc
// Pattern 1 (append): on connect, full ring buffer
{ "type": "stream_snapshot", "id": "temp_history", "data": [[t0,v0],[t1,v1],...], "seq": 9847 }

// Pattern 1 (append): each tick, only new points
{ "type": "stream_append", "id": "temp_history", "data": [[t_new, v_new]], "seq": 9848 }

// Pattern 2a (replace, float array): compact JSON or binary frame (see below)
{ "type": "stream_replace", "id": "fft", "data": [0.1, 0.4, ...], "seq": 1201 }

// Pattern 2b (int_delta histogram): sparse (index, delta) pairs — only non-zero bins
{ "type": "stream_delta", "id": "histogram", "deltas": [[4, 3], [17, -1], [42, 7]], "seq": 302 }
// on connect: stream_snapshot with full bin array + seq
```

**Binary frame format** (used for `stream_replace` when `dtype` is float32/float64):
```
[1 byte: 0x01 = stream_replace]
[2 bytes: id length (uint16 LE)]
[N bytes: stream id (UTF-8)]
[4 bytes: seq (uint32 LE)]
[rest: raw typed array (Float32Array or Float64Array, little-endian)]
```
Binary frames bypass JSON serialization entirely. The client detects binary vs text frames via `event.data instanceof ArrayBuffer`.

**Client → Server:**
```jsonc
{ "type": "command", "command": "set_temperature", "params": { "value": 22.5 }, "requestId": "abc" }
// Streams are server-push only; no client→server stream messages.
```

`requestId` is optional. If omitted, no ack is sent. If present, `send()` resolves/rejects the returned Promise.

---

## Python: Key Classes

### `state_store.py` — `StateStore[T]`

Generalize existing `StateStore` by replacing hardcoded `LabStateModel` with a type parameter:

```python
class StateStore(Generic[T]):
    def __init__(self, model_class: type[T], initial: dict[str, Any]) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...
    def version(self) -> int: ...
    def apply_value(self, json_path: str, value: Any) -> tuple[list[dict], int]: ...
    def replace_state(self, state: dict[str, Any]) -> tuple[list[dict], int]: ...
```

All existing logic (RLock, `jsonpatch.make_patch`, Pydantic re-validation after mutation) is preserved.

### `proxy.py` — `StateProxy` + `NestedProxy`

The key innovation: intercepts `__setattr__` on the state object and enqueues `(json_path, value)` onto an `asyncio.Queue` for the drain task to broadcast.

```python
class NestedProxy:
    """Accumulates path segments for nested attribute access."""
    def __getattr__(self, name) -> "NestedProxy": ...      # extends path
    def __setattr__(self, name, value): root._enqueue(...)  # enqueues at leaf

class StateProxy:
    """Top-level proxy. sync.state.temperature = 22.5 → enqueues ("/temperature", 22.5)."""
    def __getattr__(self, name):
        val = store.snapshot()[name]
        if isinstance(val, dict): return NestedProxy(self, [name])
        return val                          # scalar read works transparently
    def __setattr__(self, name, value): self._enqueue(f"/{name}", value)
    def _enqueue(self, path, value): queue.put_nowait((path, value))
```

**Read caveat**: reading a nested object like `sync.state.pump` returns a `NestedProxy` (write-only), not the dict. For reads, use `sync.get("pump/speed")` which delegates to `store.get(path)`.

**Why `put_nowait`?**: Command handlers are sync callables called from an async frame. `put_nowait` on an unbounded queue works without `await`. The drain task then picks up items asynchronously.

### `core.py` — `LabSync`

```python
class LabSync:
    def __init__(self, prefix="/sync", persist=False, db_url="sqlite:///lab_sync.db"): ...

    def state(self, cls: type[T]) -> type[T]:
        """@sync.state — requires BaseModel subclass. Creates StateStore + StateProxy."""

    def command(self, fn) -> fn:
        """@sync.command — registers fn under fn.__name__. Supports sync & async."""

    def updater(self, interval: float = 1.0):
        """@sync.updater(interval=0.1) — registers background polling coroutine."""

    @property
    def state(self) -> StateProxy: ...

    def get(self, path: str) -> Any:
        """Read helper: sync.get('pump/speed') → scalar value."""

    @property
    def router(self) -> APIRouter:
        """FastAPI router with /sync/ws and /sync/state endpoints."""

    @asynccontextmanager
    async def lifespan(self):
        """Use in FastAPI lifespan to start drain task, updaters, persistence."""

    def create_app(self, **fastapi_kwargs) -> FastAPI:
        """Convenience: creates FastAPI app with lifespan + router pre-wired."""
```

**Lifespan sequence:**
1. Create `ConnectionManager`
2. Recreate `asyncio.Queue` (must be in running loop; replaces the one from decoration time)
3. Recreate `StateProxy` with new queue
4. If `persist=True`: `PersistenceManager.initialize()` → loads SQLite state into store
5. Start drain task: `asyncio.create_task(_drain_patch_queue(...))`
6. Start updater tasks: one per registered `@sync.updater`
7. `yield`
8. Cancel all tasks; final persistence flush; `conn_manager.close_all()`

**Command dispatch (`_dispatch_command`):**
1. Look up handler in `self._commands` dict
2. If not found → send `command_error` (if `requestId` present)
3. Call handler: `await fn(**params)` or `fn(**params)` depending on `iscoroutinefunction`
4. `await self._patch_queue.join()` — wait for all enqueued patches to broadcast
5. Send `command_ack` with current version (if `requestId` present)
6. On exception → send `command_error`

**Drain task:**
```python
async def _drain_patch_queue(queue, store, conn_manager, persistence):
    while True:
        path, value = await queue.get()
        patch, version = store.apply_value(path, value)
        await conn_manager.broadcast_patch(patch, version)
        if persistence:
            await persistence.save_debounced(store.snapshot())
        queue.task_done()
```

**Updater task:**
```python
async def _run_updater(fn, interval):
    while True:
        await asyncio.sleep(interval)
        if iscoroutinefunction(fn): await fn()
        else: fn()
```

### `connection_manager.py`

Nearly identical to existing `ConnectionManager` in `main.py`. Remove all `StateStore` coupling; just manages WebSocket connections and broadcasts. `connect()` now takes `snapshot` and `version` as arguments (caller provides them).

### `persistence.py`

Activated only when `LabSync(persist=True)`. Uses `sqlmodel` single-row table. `save_debounced()` coalesces rapid saves (debounce 1s) so ~60Hz updaters don't hammer SQLite.

---

## Python: Stream Primitives

Streams bypass `StateStore`, `StateProxy`, and JSON Patch entirely. They have their own buffer objects and broadcast directly through `ConnectionManager`.

### `stream_buffer.py` — `AppendBuffer` + `ReplaceBuffer` + `DeltaBuffer`

```python
class AppendBuffer:
    """Ring buffer for pattern 1 (time series, sensor history)."""
    def __init__(self, id: str, capacity: int, conn_manager: ConnectionManager) -> None: ...

    async def append(self, point: Any) -> None: ...
    async def extend(self, points: list[Any]) -> None: ...
    # Increments seq, sends stream_append to all clients.
    # On new client connect: sends stream_snapshot with full buffer + current seq.

    def snapshot_message(self) -> dict: ...   # {"type": "stream_snapshot", "id": ..., "data": [...], "seq": N}


class ReplaceBuffer:
    """Full-replace buffer for pattern 2 float arrays (FFT, KDE, waveform)."""
    def __init__(self, id: str, capacity: int, dtype: str, conn_manager: ConnectionManager) -> None:
        # dtype: "float32" | "float64" | "json"
        # "json" → compact JSON array  (< ~2000 elements or if user prefers)
        # "float32"/"float64" → raw binary WebSocket frame

    async def replace(self, data: list[float] | np.ndarray) -> None: ...
    # Increments seq, serializes to binary or JSON, broadcasts.

    def snapshot_message(self) -> dict: ...


class DeltaBuffer:
    """Sparse-delta buffer for pattern 2 integer-count histograms."""
    def __init__(self, id: str, num_bins: int, conn_manager: ConnectionManager) -> None:
        self._bins: list[int]   # server-side ground truth

    async def apply_delta(self, deltas: dict[int, int]) -> None:
        # deltas: {bin_index: count_change} — only non-zero bins
        # Updates self._bins, sends stream_delta with sparse pairs, increments seq.

    async def replace(self, bins: list[int]) -> None:
        # Full replace (e.g., sliding window reset); sends stream_snapshot.

    def snapshot_message(self) -> dict: ...
```

**Seq counter and resync**: Each buffer has a monotonically increasing `seq` integer. Clients check that received `seq == last_seq + 1`. On gap detection, the client sends a `stream_resync` request:
```jsonc
{ "type": "stream_resync", "id": "histogram" }   // client → server
```
Server responds with a fresh `stream_snapshot`. Resync is rare (only on WebSocket frame loss, which is unusual on local connections).

### `LabSync` additions for streams

```python
class LabSync:
    def __init__(self, prefix="/sync", persist=False, compress=False, ...): ...
    # compress=True enables WebSocket permessage-deflate (RFC 7692).
    # Effective for int_delta streams (sparse zeros compress extremely well).
    # Minimal benefit for float32/float64 streams (high entropy); off by default.

    def stream(
        self,
        id: str,
        *,
        mode: Literal["append", "replace", "int_delta"] = "replace",
        capacity: int = 10_000,    # ring buffer size (append) or array length (replace/delta)
        dtype: Literal["float32", "float64", "json"] = "float32",  # replace mode only
    ) -> AppendBuffer | ReplaceBuffer | DeltaBuffer:
        """
        Registers a named stream and returns its buffer object.

        Usage:
            temp_history = sync.stream("temp_history", mode="append", capacity=10_000)
            fft_out      = sync.stream("fft", mode="replace", capacity=2048, dtype="float32")
            hist         = sync.stream("histogram", mode="int_delta", capacity=1000)

        Buffers are accessible later via sync.streams["id"].
        """
```

Stream buffers returned by `sync.stream()` are used directly in `@sync.updater` bodies:

```python
temp_history = sync.stream("temp_history", mode="append", capacity=10_000)
fft_out      = sync.stream("fft", mode="replace", capacity=2048, dtype="float32")
hist         = sync.stream("histogram", mode="int_delta", capacity=256)

@sync.updater(interval=1/60)
async def tick():
    # Pattern 1: append new point
    await temp_history.append({"t": time.time(), "v": sensor.read()})

    # Pattern 2 replace: send full float array
    await fft_out.replace(np.abs(np.fft.rfft(window)))

    # Pattern 2 delta: send only changed bins
    new_sample_bin = int((sensor.read() - MIN) / BIN_WIDTH)
    await hist.apply_delta({new_sample_bin: +1})
```

### `ConnectionManager` additions

Two new broadcast methods alongside the existing `broadcast_patch`:

```python
async def broadcast_json(self, message: dict) -> None:
    """Send a JSON text frame to all clients (stream_append, stream_delta, stream_snapshot)."""

async def broadcast_binary(self, frame: bytes) -> None:
    """Send a raw binary frame to all clients (stream_replace with float dtype)."""
```

New client lifecycle hook: `ConnectionManager.on_connect` callback so stream buffers can send their snapshot to the newly joined client without coupling to the WebSocket endpoint:

```python
async def connect(self, websocket, client_id, state_snapshot, state_version, stream_snapshots) -> None:
    await websocket.accept()
    await websocket.send_json({"type": "snapshot", "data": state_snapshot, "version": state_version})
    for msg in stream_snapshots:       # one snapshot per registered stream
        await websocket.send_json(msg)
```

---

## JavaScript: Key Classes

### `client.ts` — `SyncClient`

```typescript
class SyncClient {
  constructor(url: string, options?: { maxReconnectAttempts?: number; commandTimeout?: number })

  connect(): void
  disconnect(): void

  get<T>(path: string): T           // dot notation: "pump.speed"
  subscribe(path: string, handler: (value: unknown) => void): () => void
  send(command: string, params?: Record<string, unknown>): Promise<number>  // resolves version

  onSnapshot(handler: (data, version) => void): () => void
  onPatch(handler: (patch) => void): () => void
  onStatusChange(handler: (status) => void): () => void
}

export function createSyncClient(url: string, options?: object): SyncClient
// (factory: creates + connects)
```

**`send()` with requestId correlation:**
- Generates `crypto.randomUUID()` as `requestId`
- Sets a timeout timer (default 10s)
- Stores `{ resolve, reject, timer }` in `pendingRequests` Map keyed by `requestId`
- On `command_ack`: clears timer, calls `resolve(msg.version)`
- On `command_error`: clears timer, calls `reject(new Error(msg.error))`

**`subscribe(path, handler)` with path matching:**
- Stores handlers in `pathListeners` Map keyed by dot-path
- On each incoming patch op, converts JSON Pointer (`/pump/speed`) to dot-path (`pump.speed`)
- Notifies handlers whose subscribed path is a prefix of the affected path

### `apply-patch.ts`

Zero-dependency minimal RFC 6902 applier supporting `add`, `remove`, `replace` (the only ops the server generates). If full compliance is needed, users can pass `{ patchApplier }` option to `SyncClient`.

### `stream-handle.ts` — `StreamHandle`

Returned by `client.stream(id)`. Holds the local buffer and exposes typed callbacks.

```typescript
type StreamMode = "append" | "replace" | "int_delta"

class StreamHandle {
  readonly id: string
  readonly mode: StreamMode

  // Append mode
  onAppend(handler: (points: unknown[]) => void): () => void
  onSnapshot(handler: (buffer: unknown[], seq: number) => void): () => void

  // Replace mode (JSON)
  onReplace(handler: (data: number[]) => void): () => void
  // Replace mode (binary — handler receives typed array directly)
  onReplaceBinary(handler: (data: Float32Array | Float64Array) => void): () => void

  // Int-delta mode
  onDelta(handler: (deltas: [index: number, delta: number][]) => void): () => void
  onSnapshot(handler: (bins: number[], seq: number) => void): () => void

  // Seq gap detection → triggers resync request to server
  // (internal; user does not need to handle this)
}
```

`SyncClient` dispatches incoming stream messages to the correct `StreamHandle` by `id`. Binary frames are decoded in `client.ts` by reading the binary header and routing to the matching handle.

```typescript
// In SyncClient.handleMessage:
} else if (msg.type === "stream_snapshot") {
    this.getOrCreateStream(msg.id, "append").handleSnapshot(msg)
} else if (msg.type === "stream_append") {
    this.getOrCreateStream(msg.id, "append").handleAppend(msg)
} else if (msg.type === "stream_replace") {
    this.getOrCreateStream(msg.id, "replace").handleReplace(msg)
} else if (msg.type === "stream_delta") {
    this.getOrCreateStream(msg.id, "int_delta").handleDelta(msg)
}

// In SyncClient.handleBinaryMessage (event.data instanceof ArrayBuffer):
// Parse binary header → extract id → route to stream handle
```

### `svelte/index.ts` — Svelte 5 adapter

Exports: `createSyncClient`, `useSyncState`, `useStream`. File must use `.svelte.ts` extension so the Svelte compiler processes runes (`$state`, `$derived`, `$effect`).

```typescript
// lab-sync/src/svelte/index.ts  (.svelte.ts extension required for rune processing)
import { SyncClient } from "../client.js"
import type { StreamHandle } from "../stream-handle.js"

export function createSyncClient(url: string, options?: object): SyncClient {
    const client = new SyncClient(url, options)
    client.connect()
    return client
}

export function useSyncState<T extends Record<string, unknown>>(client: SyncClient): T {
    let state = $state<T>({} as T)

    client.onSnapshot((data) => {
        // Object.assign works: each key= goes through the proxy setter,
        // nested plain objects are re-proxified on assignment.
        Object.assign(state, data)
    })

    client.onPatch((patches) => {
        for (const op of patches) _applyPatchToProxy(state, op)
    })

    return state  // caller receives the $state proxy; reads through it are auto-tracked
}

export function useStream(client: SyncClient, id: string): StreamHandle {
    // Thin passthrough — no Svelte reactivity needed for streams.
    // Chart libraries manage their own canvas buffers; stream callbacks
    // write to those buffers directly, bypassing Svelte's reactivity system.
    return client.stream(id)
}

// Internal: walks JSON Pointer path through proxy chain and assigns at leaf.
// Reading each segment through the proxy registers it as a reactive dependency,
// so only components that actually read state.pump.speed re-render when /pump/speed changes.
function _applyPatchToProxy(state: any, op: PatchOperation): void {
    const parts = op.path.split('/').filter(Boolean)
    let target = state
    for (let i = 0; i < parts.length - 1; i++) {
        target = target[parts[i]]               // each access goes through the proxy
    }
    const leaf = parts[parts.length - 1]

    if (op.op === 'replace' || op.op === 'add') {
        if (leaf === '-' && Array.isArray(target)) target.push(op.value)   // RFC 6902 append
        else target[leaf] = op.value            // assignment through proxy — reactive
    } else if (op.op === 'remove') {
        if (Array.isArray(target)) target.splice(Number(leaf), 1)
        else delete target[leaf]
    }
}
```

---

### Svelte 5 reactivity model (critical for implementing the adapter correctly)

`$state({})` wraps the object in a JavaScript Proxy. The proxy intercepts all property reads (tracking which `$derived`/`$effect`/template expression reads which path) and writes (triggering re-evaluation of those subscribers). **Recursively**: setting `state.pump = { speed: 100 }` causes the new nested object to also be proxified on assignment.

**What is reactive:**
```typescript
state.temperature = 22.5         // ✓ proxy setter — triggers reactivity
state.pump.speed = 1500          // ✓ proxy chain — only pump.speed subscribers re-render
state.pump = { speed: 1500 }     // ✓ whole pump replaced — pump and pump.speed subscribers re-render
Object.assign(state, data)       // ✓ each key= goes through proxy setter
target[leaf] = op.value          // ✓ same as above — the path walk above keeps us inside proxy chain
```

**What breaks reactivity:**
```typescript
let { temperature } = state      // ✗ extracts primitive, loses proxy connection
temperature = 22.5               // ✗ not reactive
```

**Granularity**: Svelte tracks reads at the individual property level. A `$derived` reading `state.pump.speed` and `state.pump.running` does NOT re-evaluate when `state.temperature` changes. At 60 Hz with many streams, this matters.

**Maps**: Not auto-proxified by `$state`. Not used in the sync adapter — JSON deserializes to plain objects, which are fully proxified.

**Class instances in state**: Not deep-proxified. Not relevant — server state is always plain JSON.

---

### Application usage patterns

**Sharing state across a component tree** — set context at root, `getContext` anywhere:

```svelte
<!-- App.svelte -->
<script>
  import { setContext } from 'svelte'
  import { createSyncClient, useSyncState } from 'lab-sync/svelte'

  const client = createSyncClient('ws://localhost:8000/sync/ws')
  const state  = useSyncState(client)
  setContext('lab', state)   // set once; all descendants can read it
</script>
<slot />
```

**Passing a slice to a child** — props are reactive bindings; passing through proxy properties preserves tracking:

```svelte
<!-- Parent reads state, passes slice to child -->
<PumpButton enabled={state.pump.running} />
<SpeedSlider value={state.pump.speed} max={5000} />
```

```svelte
<!-- PumpButton.svelte — receives primitive prop, reactive to parent's state.pump.running -->
<script>
  let { enabled } = $props()
</script>
<button disabled={!enabled}>Toggle</button>
```

**Controller `.svelte.ts` files** — define `$derived` and `$effect` anchored to the shared state. File must end in `.svelte.ts` for rune processing. Use `$derived.by(() => ...)` when referencing `this` inside a class:

```typescript
// pump-controller.svelte.ts
import { getContext } from 'svelte'
import type { AppState } from './app-state.svelte.ts'

export class PumpController {
    private state = getContext<AppState>('lab')

    // $derived.by() required when the lambda references 'this'
    isRunning    = $derived.by(() => this.state.pump.running)
    speedPercent = $derived.by(() => this.state.pump.speed / 5000 * 100)
    isNearLimit  = $derived.by(() => this.state.pump.speed > 4500)

    // $effect anchors a side-effect to reactive state reads
    _ = $effect(() => {
        if (this.isNearLimit) console.warn('Pump near max speed')
    })
}
```

```svelte
<!-- PumpPanel.svelte — no prop drilling; anchors via controller -->
<script>
  import { PumpController } from './pump-controller.svelte.ts'
  const pump = new PumpController()   // reads context internally
</script>
<p>Speed: {pump.speedPercent.toFixed(1)}%</p>
<p class:warn={pump.isNearLimit}>Status: {pump.isRunning ? 'running' : 'stopped'}</p>
```

**Streams bypass Svelte reactivity entirely** — chart callbacks write directly to canvas buffers:

```svelte
<script>
  import { useStream } from 'lab-sync/svelte'

  const fft  = useStream(client, 'fft')
  const hist = useStream(client, 'histogram')

  let chartEl: HTMLCanvasElement
  fft.onReplaceBinary((data: Float32Array) => chart.setData(data))  // direct canvas update at 60 Hz
  hist.onDelta((deltas) => histogram.applyDeltas(deltas))
</script>

<canvas bind:this={chartEl}></canvas>
```

### JS Build: `tsup.config.ts`

Three separate entry points built with `tsup` (wraps esbuild, generates `.d.ts`):
- `dist/index.js` — core client (zero deps)
- `dist/svelte/index.js` — Svelte adapter (peer dep: `svelte >= 5`)
- `dist/react/index.js` — React adapter stub (peer dep: `react`)

`package.json` subpath exports map each to its dist file.

---

## Python Package Init

```bash
cd /Users/andrew/Documents/PROGRAM_LOCAL/lab_sync
mkdir lab_sync && cd lab_sync
mkdir python js
cd python
uv init --lib --name lab-sync
# Produces: src/lab_sync/__init__.py, pyproject.toml, uv.lock
```

Dependencies:
```toml
[project]
dependencies = ["fastapi>=0.115", "jsonpatch>=1.33", "pydantic>=2.0"]

[project.optional-dependencies]
persist = ["sqlmodel>=0.0.24"]
numpy = ["numpy>=1.24"]   # optional: ReplaceBuffer.replace() accepts np.ndarray
dev = ["pytest", "pytest-asyncio", "httpx", "uvicorn[standard]"]
```

## JS Package Init

```bash
cd js
bun init          # generates package.json, tsconfig.json
bun add -d tsup typescript svelte
```

---

## Verification

### Python unit tests
```bash
cd python
uv run pytest tests/ -v
```

### Python smoke test
```python
# example.py
from lab_sync import LabSync
from pydantic import BaseModel

sync = LabSync()

@sync.state
class S(BaseModel):
    x: float = 0.0

@sync.command
def set_x(value: float):
    sync.state.x = value

app = sync.create_app()
```
```bash
uv run uvicorn example:app
curl http://localhost:8000/sync/state          # {"x": 0.0}
wscat -c ws://localhost:8000/sync/ws           # receives snapshot
# send: {"type":"command","command":"set_x","params":{"value":5},"requestId":"1"}
# receive: command_ack + patch
```

### JS build + tests
```bash
cd js
bun run build     # produces dist/
bun test
```

### Multi-client integration
1. Start Python server with an updater (e.g., random value every 0.5s)
2. Open 2+ browser tabs using the Svelte adapter — both receive patches in real time
3. Kill and restore a tab — rejoins with fresh snapshot
4. Send an out-of-range command — `send()` Promise rejects with error message
5. Port `server-lab-json-patch` example to use `lab-sync` — no raw WebSocket code should remain in user code

### Stream verification
```python
# example_streams.py — register all three stream modes
fft_out  = sync.stream("fft",       mode="replace",   capacity=1024, dtype="float32")
history  = sync.stream("history",   mode="append",    capacity=5000)
hist     = sync.stream("histogram", mode="int_delta", capacity=100)

@sync.updater(interval=1/60)
async def tick():
    await fft_out.replace([random.random() for _ in range(1024)])
    await history.append({"t": time.time(), "v": random.gauss(22, 1)})
    await hist.apply_delta({random.randint(0, 99): 1})
```
- Connect a client and verify `stream_snapshot` received for each stream on connect
- Verify `stream_replace` arrives as a binary ArrayBuffer for `fft` stream
- Stop server mid-stream, reconnect — verify all three stream snapshots re-received
- Artificially skip a `seq` on the client — verify `stream_resync` is sent and `stream_snapshot` re-received
