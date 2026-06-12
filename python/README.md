# lab-link Python

Starlette/Pydantic backend runtime and Python sync client for `lab-link`.

Bind a reactive state model, expose a WebSocket sync endpoint, run commands
with hardware side effects, broadcast versioned JSON Patch updates, or control
a `lab-link` server from Python.

```bash
uv add lab-link
```

```python
from lab_link import LabSync, ReactiveModel

class AppState(ReactiveModel):
    voltage: float = 0.0

sync = LabSync()
state = sync.bind_state(AppState())

@sync.command
def set_voltage(value: float):
    state.voltage = round(value, 3)  # validated, batched, broadcast

app = sync.create_app()
```

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    snapshot = sync.snapshot()
    ack = sync.send_command("set_voltage", {"value": 1.2})
```

Full docs: https://sansseriff.github.io/lab-link/
