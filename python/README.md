# lab-link Python

FastAPI/Pydantic backend runtime for `lab-link`.

Use it to register authoritative state, expose a WebSocket sync endpoint, run
commands with hardware side effects, and broadcast versioned JSON Patch updates.

```bash
uv add lab-link
```

```python
from lab_link import LabSync

sync = LabSync()
sync.register_state(AppState, initial=state)
app = sync.create_app()
```

Full docs: https://sansseriff.github.io/lab-link/
