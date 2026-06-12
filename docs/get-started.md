# Get Started

Install the Python backend package in the service that owns hardware state:

```bash
uv add lab-link
```

Install the browser package in the frontend:

```bash
bun add lab-link
```

## Backend

```python
from pydantic import Field
from lab_link import CommandContext, LabSync, ReactiveModel

sync = LabSync()

class Channel(ReactiveModel):
    bias_voltage: float = 0.0

class AppState(ReactiveModel):
    channels: list[Channel] = Field(default_factory=lambda: [Channel()])

state = sync.bind_state(AppState())

@sync.command
async def set_voltage(ctx: CommandContext, channel: int, value: float):
    # Do hardware work first. Commit state only after side effects succeed.
    state.channels[channel].bias_voltage = round(value, 3)
    return {"channel": channel, "bias_voltage": round(value, 3)}

app = sync.create_app()
```

## Frontend

```ts
import { createSyncRuntime, SyncNode } from "lab-link/model"

const runtime = createSyncRuntime({
  url: `ws://${window.location.host}/sync/ws`,
})

class ChannelModel extends SyncNode<{ bias_voltage: number }> {
  bias_voltage = 0
  editing = false

  override readonly fields = this.defineFields<this>({
    bias_voltage: {
      blockWhen: () => this.editing,
      onBlocked: "queueLatest",
      setVia: "set_voltage",
    },
  })

  applySnapshot(snapshot: { bias_voltage: number }) {
    this.bias_voltage = snapshot.bias_voltage
  }
}
```

## Python Client

Use the Python client when another Python process needs to control a running
`lab-link` GUI server through the same WebSocket protocol as the browser.

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    snapshot = sync.snapshot()
    ack = sync.send_command(
        "set_voltage",
        {"channel": 0, "value": 1.2},
    )
```
