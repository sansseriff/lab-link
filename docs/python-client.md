# Python Client API

The PyPI package includes a generic Python client for controlling a running
`lab-link` server over the same WebSocket protocol used by browser frontends.
It is useful for scripts or Python packages that need to drive a GUI-backed
instrument service without adding app-specific REST endpoints.

## Async Client

`AsyncLabLinkClient` is the primary implementation.

```python
from lab_link import AsyncLabLinkClient, SyncCommandError

async with AsyncLabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    snapshot = sync.snapshot()
    try:
        ack = await sync.send_command(
            "set_voltage",
            {"channel": 0, "value": 1.2},
        )
    except SyncCommandError as exc:
        print(exc.code, exc.message)
```

`connect()` waits for the initial `snapshot` message before returning. Incoming
`patch` messages update the cached snapshot with JSON Patch, including patches
that arrive before the matching command acknowledgement.

## Sync Client

`LabLinkClient` wraps the async client with a background event loop for scripts.

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    print(sync.snapshot())
    ack = sync.send_command("set_voltage", {"channel": 0, "value": 1.2})
```

## Events

Snapshots are returned as deep copies so callers cannot mutate the client cache.
Subscribe to patches or command errors when a script needs to react to live
updates.

```python
unsubscribe = sync.on_patch(lambda event: print(event.version, event.patch))
```

`send_command()` returns `CommandAck` on success and raises `SyncCommandError`
for structured server-side command failures or client-side timeouts.
