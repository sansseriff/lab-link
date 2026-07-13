# lab-link

`lab-link` is a server-authoritative synchronization library for laboratory
instrument control software. Python owns hardware state and side effects;
browsers or Python control clients receive snapshots and JSON Patch updates,
send commands, and handle structured command errors.

The project is published as two packages:

- `lab-link` on PyPI for the Starlette/Pydantic backend and Python sync client.
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
from lab_link import LabSync, LanPassphraseAuth, ReactiveModel

class AppState(ReactiveModel):
    voltage: float = 0.0

auth = LanPassphraseAuth()  # optional LAN passphrase + one-time QR invitations
sync = LabSync(auth=auth)
state = sync.bind_state(AppState())

@sync.command
async def set_voltage(value: float):
    # validated, recorded, batched, and broadcast as one patch message
    state.voltage = round(value, 3)
    return {"value": state.voltage}
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
