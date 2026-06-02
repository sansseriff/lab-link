# lab-link Python

FastAPI/Pydantic backend runtime and Python sync client for `lab-link`.

Use it to register authoritative state, expose a WebSocket sync endpoint, run
commands with hardware side effects, broadcast versioned JSON Patch updates, or
control a `lab-link` server from Python.

```bash
uv add lab-link
```

```python
from lab_link import LabSync

sync = LabSync()
sync.register_state(AppState, initial=state)
app = sync.create_app()
```

```python
from lab_link import LabLinkClient

with LabLinkClient("ws://127.0.0.1:8000/sync/ws") as sync:
    snapshot = sync.snapshot()
    ack = sync.send_command("set_voltage", {"path": "/channels/0/bias_voltage", "value": 1.2})
```

Full docs: https://sansseriff.github.io/lab-link/
