# lab-link

`lab-link` is a server-authoritative synchronization library for laboratory
instrument control software. Python owns hardware state and side effects;
browsers or Python control clients receive snapshots and JSON Patch updates,
send commands, and handle structured command errors.

The project is published as two packages:

- `lab-link` on PyPI for the FastAPI/Pydantic backend and Python sync client.
- `lab-link` on npm for the browser runtime, model layer, and Svelte/React
  adapters.

Keep both package versions aligned. They share one protocol, so a breaking
protocol change should bump both packages.

## Install

```bash
uv add lab-link
bun add lab-link
```

## Shape

Backend:

```python
from lab_link import LabSync, ptr

sync = LabSync()
sync.register_state(AppState, initial=state)

@sync.command
async def set_voltage(path: str, value: float):
    sync.set(path, round(value, 3))
    return {"path": path, "value": round(value, 3)}
```

Frontend:

```ts
import { createSyncRuntime, SyncNode } from "lab-link/model"

const runtime = createSyncRuntime({ url: "/sync/ws" })
```

Python client:

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    print(sync.snapshot())
    sync.send_command("set_voltage", {"path": "/channels/0/bias_voltage", "value": 1.2})
```

See the documentation site for API details, examples, and publishing notes:
https://sansseriff.github.io/lab-link/

## Development

```bash
cd python && uv run pytest
cd ../js && bun test && bun run build
```
