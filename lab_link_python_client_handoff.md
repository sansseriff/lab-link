# lab_link Python Client Handoff

This document describes the desired Python client addition for the `lab_link`
library and how dbay should use it after implementation.

The `lab_link` repo is:

`/Users/andrew/Documents/PROGRAM_LOCAL/lab_link`

The dbay repo is:

`/Users/andrew/Documents/PROGRAM_LOCAL/dbay`

## Current Context

dbay has been migrated to use `lab_link` for GUI synchronization:

- Backend exposes `/sync/ws` and `/sync/state`.
- Frontend uses `lab-link` JavaScript runtime over WebSocket.
- Frontend channel mutations use `sendCommand(...)`.
- Existing REST mutation endpoints still exist for compatibility.

The dbay Python package in:

`software/client`

has two modes:

- `mode="direct"`: sends low-level UDP/serial commands directly to hardware.
- `mode="gui"`: talks to the dbay GUI backend. Today this uses HTTP/REST.

The goal is to avoid writing a dbay-specific WebSocket shim. The generic
WebSocket protocol belongs in `lab_link`, so `lab_link` should provide a Python
client that dbay can use.

## Recommendation

Add a small generic Python client to `lab_link`.

Do not make it dbay-specific. It should understand the `lab_link` WebSocket
protocol only:

- initial `snapshot`
- `patch`
- `command_ack`
- `command_error`
- `send_command(...)`
- current snapshot access
- connection lifecycle

dbay should then use this generic client in its GUI mode and map dbay module
methods to dbay command names.

## Desired Public API

Provide both async and synchronous clients if practical.

The async client should be the primary implementation:

```python
from lab_link import AsyncLabLinkClient

async with AsyncLabLinkClient("ws://127.0.0.1:8345/sync/ws") as sync:
    snapshot = sync.snapshot()
    ack = await sync.send_command(
        "set_dac4d_vsource",
        {
            "module_index": 0,
            "index": 1,
            "bias_voltage": 1.2,
            "activated": True,
            "heading_text": "",
            "measuring": False,
        },
    )
```

The synchronous wrapper should be convenient for scripts:

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8345/sync/ws") as sync:
    snapshot = sync.snapshot()
    ack = sync.send_command(
        "set_dac4d_vsource",
        {
            "module_index": 0,
            "index": 1,
            "bias_voltage": 1.2,
            "activated": True,
            "heading_text": "",
            "measuring": False,
        },
    )
```

## Suggested Python Types

Suggested generic models:

```python
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SnapshotEvent:
    data: dict[str, Any]
    version: int


@dataclass(frozen=True)
class PatchEvent:
    patch: list[dict[str, Any]]
    version: int
    origin_client_id: str | None = None
    request_id: str | None = None
    command: str | None = None


@dataclass(frozen=True)
class CommandAck:
    command: str
    request_id: str
    version: int
    result: Any | None = None
```

For command errors, reuse or mirror the existing `CommandError` semantics.
There should be a client-side exception for errors received from the server:

```python
class SyncCommandError(Exception):
    command: str
    request_id: str | None
    code: str
    message: str
    detail: str | None
    severity: str
    display: str
    recoverable: bool
    path: str | None
    version: int
```

Do not conflate this with server-side `CommandError` if that creates confusing
constructor or inheritance semantics. A distinct `SyncCommandError` is fine.

## Required Behavior

### Connection

`AsyncLabLinkClient.connect()` should:

1. Open the WebSocket.
2. Wait for the initial `snapshot` message.
3. Store the snapshot and version.
4. Start a background receive loop or otherwise ensure patches/acks/errors are
   processed while commands are in flight.

### Snapshot

`snapshot()` should return the latest known state.

Returning a deep copy is preferable so callers do not accidentally mutate the
client cache.

### Patches

Incoming `patch` messages should update the cached snapshot using JSON Patch.

Use a real JSON Patch implementation instead of ad hoc mutation if possible.
For Python, `jsonpatch` is already a `lab_link` dependency.

Optionally expose patch subscriptions:

```python
sync.on_patch(callback)
```

This can be deferred if command support is the first goal.

### Commands

`send_command(command, params)` should:

1. Generate a request id.
2. Send:

```json
{
  "type": "command",
  "command": "command_name",
  "params": {},
  "requestId": "..."
}
```

3. Wait for either matching `command_ack` or matching `command_error`.
4. Return `CommandAck` on success.
5. Raise `SyncCommandError` on error.
6. Timeout cleanly if no response arrives.

It is important that patch messages may arrive before the matching ack. The
client must not assume the next message after a command is the ack.

### Unknown / Unsolicited Errors

If a `command_error` arrives with a request id that matches a pending command,
reject that command.

If it does not match a pending command, optionally store it in a last-error list
or call error subscribers.

### Streams

Streams are not required for the first Python client implementation.

Do not block command/snapshot support on streams.

## Dependency Guidance

The current `lab_link` Python server depends on FastAPI and related server
packages. For the client, prefer a lightweight WebSocket dependency.

Reasonable options:

- `websockets`
- `httpx` WebSocket support only if appropriate

`websockets` is probably the simplest.

If adding `websockets` as a required dependency feels too heavy, make the client
extra optional:

```toml
[project.optional-dependencies]
client = ["websockets>=12.0"]
```

However, since the server package already speaks WebSocket, making this a normal
dependency may also be acceptable.

## Suggested File Layout In lab_link

Add:

```text
python/src/lab_link/client.py
python/tests/test_client.py
```

Update:

```text
python/src/lab_link/__init__.py
python/pyproject.toml
python/README.md
docs/backend.md or docs/get-started.md
```

Exports should include:

```python
from .client import AsyncLabLinkClient, LabLinkClient, SyncCommandError
```

## Test Requirements In lab_link

Add tests using the existing FastAPI `TestClient` only if that can exercise the
real client. If not, start an in-process uvicorn server on a free local port.

Minimum tests:

1. Client connects and receives initial snapshot.
2. Client `send_command(...)` returns ack and result.
3. Patch before ack updates the cached snapshot.
4. Server `CommandError` becomes client `SyncCommandError`.
5. Unknown command becomes `SyncCommandError`.
6. Command timeout raises a clear timeout exception.
7. Multiple concurrent commands resolve by request id, not by message order.

The most important test is message ordering:

```text
client sends command
server broadcasts patch
server sends ack
client updates snapshot and returns ack
```

This is how `LabSync._dispatch_command` currently behaves.

## dbay Integration After lab_link Client Exists

Once `lab_link` publishes the Python client, update:

```text
software/client
```

### DBayClient GUI Mode

In `software/client/dbay/client.py`, GUI mode should create a `LabLinkClient`
instead of using only `Http`.

Potential approach:

```python
self._sync = LabLinkClient(f"ws://{server_address}:{port}/sync/ws")
self._sync.connect()
self._instantiate_modules(self._sync.snapshot()["data"])
```

Keep `Http` temporarily for read-only endpoints like `/server-info` if needed.

### Module Methods

Update GUI-mode module methods to call `send_command(...)`.

Examples:

`software/client/dbay/modules/dac4d.py`

```python
ack = self.sync.send_command(
    "set_dac4d_vsource",
    {
        "module_index": self.slot,
        "index": channel,
        "bias_voltage": voltage,
        "activated": True,
        "heading_text": heading_text,
        "measuring": False,
    },
)
```

`software/client/dbay/modules/dac16d.py`

```python
send_command("set_dac16d_vsource", ...)
send_command("set_dac16d_vsource_shared", ...)
```

`software/client/dbay/modules/adc4d.py`

```python
send_command("set_adc4d_vsense", ...)
```

### Module Constructor Pattern

Today module constructors accept `http=...`.

Add a `sync=...` parameter for GUI mode. During transition, modules can accept
both:

```python
def __init__(..., http=None, sync=None, mode="gui", ...):
    self.http = http
    self.sync = sync
```

Then prefer `sync` when available.

### REST Endpoint Policy In dbay

After dbay Python GUI mode no longer depends on REST mutation endpoints, decide
whether to keep REST as compatibility/debug shims.

If kept, REST should call the same module-local implementation as the WebSocket
command. Avoid two independent mutation implementations.

For example in `backend/modules/dac4D.py`:

```python
def apply_dac4d_vsource(change: VsourceChange):
    ...

@sync.command
def set_dac4d_vsource(ctx, **params):
    return apply_dac4d_vsource(VsourceChange(**params))

@router.put("/vsource/")
async def voltage_set(request, change: VsourceChange):
    return apply_dac4d_vsource(change)
```

## Non-Goals

Do not add dbay module concepts to `lab_link`.

Do not implement dbay command names in `lab_link`.

Do not require Python client users to understand Svelte, `SvelteSyncNode`, or
frontend model routing.

Do not make streams part of the first implementation unless the basic command
client is already complete and tested.

## Success Criteria

The `lab_link` Python client is done when:

- A Python script can connect to `/sync/ws`.
- It receives the initial snapshot.
- It can send a command and receive a typed ack/result.
- It raises a typed error for server command errors.
- It keeps its local snapshot updated by patches.
- It passes ordering tests where patches arrive before command acks.

The dbay follow-up is done when:

- `DBayClient(mode="gui")` can use WebSocket commands.
- Existing direct UDP/serial mode is unchanged.
- Existing dbay Python GUI tests pass after being updated from REST mocks to
  lab-link client mocks.
- The dbay GUI frontend and Python client use the same command names and backend
  command implementations.
