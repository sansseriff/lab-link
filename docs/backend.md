# Backend API

The backend package provides Starlette routes, a state store, command
dispatcher, and stream buffers. `sync.create_app()` returns a ready-to-serve
Starlette app; alternatively, mount `sync.routes` into your own Starlette app,
or attach `sync.handle_ws` to a websocket route in any ASGI framework built on
Starlette (FastAPI included).

## Serving

The simplest path is the pre-wired app:

```python
app = sync.create_app()  # GET /sync/state and WS /sync/ws, lifespan included
```

To keep control of your own app, pass `sync.routes` and `sync.lifespan`:

```python
from starlette.applications import Starlette

app = Starlette(routes=[*sync.routes, *my_routes], lifespan=sync.lifespan)
```

Or wire the websocket handler to any route yourself. This is how a FastAPI
app integrates — FastAPI's `WebSocket` *is* Starlette's, so `handle_ws`
plugs in directly:

```python
from fastapi import FastAPI, WebSocket

app = FastAPI(lifespan=sync.lifespan)

@app.websocket("/sync/ws")
async def sync_ws(ws: WebSocket):
    await sync.handle_ws(ws)
```

lab-link itself does not depend on FastAPI; install it separately if you want
its dependency injection, validation, or OpenAPI for your own endpoints.

## LAN access control

`LanPassphraseAuth` provides a headless access-control pattern for instruments
served on a trusted local network. It protects `/sync/state` and rejects a
WebSocket before sending its initial snapshot. The application still owns its
HTML, modal, QR code, wording, and visual design.

```python
from lab_link import LabSync, LanPassphraseAuth

auth = LanPassphraseAuth(
    passphrase=configured_passphrase,  # omit to generate one at startup
    allowed_origins={"http://localhost:5173"},
)
sync = LabSync(auth=auth)

# Put this short-lived, single-use token in a QR URL fragment:
invite = auth.create_invite()
url = f"http://192.168.1.20:8000/#invite={invite.token}"
```

The auth endpoints live below the sync prefix:

- `GET /sync/auth/status`
- `POST /sync/auth/login` with `{ "passphrase": "…" }`
- `POST /sync/auth/invite` with `{ "invite": "…" }`
- `POST /sync/auth/logout`

Every successful login or invite exchange creates a separate HttpOnly,
SameSite session cookie. Logging out one browser does not revoke the others.
Passphrase attempts are rate-limited, WebSocket origins are checked, and
invitations are stored as hashes and consumed once. Sessions and invites are
in memory and are revoked by an application restart. Open WebSockets are
periodically revalidated, so an expired or logged-out session cannot remain an
indefinite control channel.

Loopback clients are trusted by default so a desktop shell can open without a
login. Set `trust_loopback=False` to require authentication there too.
Same-origin IP-address and `localhost` URLs are accepted automatically. Add any
named hosts to `allowed_origins` explicitly; this restriction prevents an
arbitrary DNS-rebinding hostname from inheriting loopback trust.

This is access control, not transport encryption. HTTP and `ws://` still expose
traffic to a hostile network; use a trusted LAN or put the app behind HTTPS.
Applications must separately gate their UI document and any other sensitive
routes using `auth.is_http_authorized(request)`.

## State

State models subclass `ReactiveModel` (a pydantic `BaseModel`). Bind one
instance at startup; it is the single authoritative copy of the state:

```python
from pydantic import Field
from lab_link import LabSync, ReactiveModel

class Channel(ReactiveModel):
    bias_voltage: float = 0.0
    active: bool = False

class AppState(ReactiveModel):
    enabled: bool = False
    channels: list[Channel] = Field(default_factory=lambda: [Channel()])

sync = LabSync()
state = sync.bind_state(AppState())   # returns the instance, typed
```

Every nested model must also subclass `ReactiveModel`; `list` and `dict`
fields are tracked automatically (`set` fields and models inside tuples are
rejected at construction, never silently un-tracked).

## Mutations

Mutate the bound model. Each assignment is validated by pydantic, recorded as
a JSON Patch op, batched with other ops from the same event-loop tick, and
broadcast to all clients as one versioned patch message:

```python
state.enabled = True
state.channels[0].bias_voltage = 1.25
state.channels.append(Channel())
del state.channels[0]
```

Patches caused by a command automatically carry that command's
`originClientId` / `requestId` / `command` metadata, and the command ack is
sent only after every patch it produced.

To group mutations across awaits into a single patch message, use `batch()`:

```python
with sync.batch():
    state.channels[0].bias_voltage = 1.25
    state.channels[0].active = True
```

Replacing a whole subtree emits one `replace` op, and the new subtree is
tracked from then on. The *old* object is orphaned: further writes to it are
dropped (debug-logged) because it is no longer part of the state document.

For bulk restore (e.g. loading a saved snapshot), `load_state()` validates the
data, swaps the bound instance's contents in place (existing references stay
valid), and emits a single whole-document patch:

```python
sync.load_state(saved_snapshot)
```

Two rules the engine enforces loudly rather than corrupting state:

- mutations must happen on the event loop's thread — mutate after awaiting
  `asyncio.to_thread(...)`, not inside it;
- an object may live at only one location in the tree.

`sync.publish()` is a dump-and-diff escape hatch: it diffs the bound model
against the wire mirror and broadcasts the difference (normally empty).

The path-based APIs (`register_state`, `sync.get`, `sync.set`,
`sync.transaction`, `sync.replace_state`) still work but are deprecated.

## Commands

Command handlers may receive `CommandContext` and may return canonical result
data for the browser.

```python
@sync.command
async def set_channel(ctx: CommandContext, channel: int, value: float):
    rounded = round(value, 3)
    state.channels[channel].bias_voltage = rounded
    return {"channel": channel, "value": rounded}
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
