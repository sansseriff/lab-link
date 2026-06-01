# Backend API

The backend package provides a FastAPI router, state store, command dispatcher,
and stream buffers.

## State

Register a Pydantic model at startup:

```python
sync.register_state(SystemState, initial=system_state)
```

The decorator form remains useful for examples:

```python
@sync.state
class AppState(BaseModel):
    enabled: bool = False
```

State paths are JSON Pointers. Use `ptr()` instead of hand-building strings:

```python
from lab_link import ptr

path = ptr("data", slot, "vsource", "channels", channel, "bias_voltage")
```

## Mutations

Use explicit mutation APIs in command handlers and services:

```python
sync.set(ptr("enabled"), True)
sync.replace_state(next_state)
```

Batch related updates in one transaction. A transaction validates the resulting
Pydantic state, computes one JSON Patch batch, increments the version once, and
broadcasts one patch message.

```python
with sync.transaction() as tx:
    tx.set(ptr("channels", 0, "bias_voltage"), 1.25)
    tx.set(ptr("channels", 0, "active"), True)
```

## Commands

Command handlers may receive `CommandContext` and may return canonical result
data for the browser.

```python
@sync.command
async def set_channel(ctx: CommandContext, path: str, value: float):
    rounded = round(value, 3)
    sync.set(path, rounded)
    return {"path": path, "value": rounded}
```

Raise `CommandError` for display-ready failures:

```python
raise CommandError(
    code="hardware_timeout",
    message="The voltage source did not respond before the timeout.",
    detail="UDP timeout after 5.0 s",
    display="banner",
    path=path,
)
```
