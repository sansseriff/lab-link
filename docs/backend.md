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
app integrates — FastAPI's `WebSocket` _is_ Starlette's, so `handle_ws`
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

For process-local access with a generated startup passphrase:

```python
from lab_link import LabSync, LanPassphraseAuth

auth = LanPassphraseAuth(allowed_origins={"http://localhost:5173"})
sync = LabSync(auth=auth)
```

For stable credentials and remembered devices, use a persistent store:

```python
from lab_link import LabSync, LanPassphraseAuth, SQLiteAuthStore

auth = LanPassphraseAuth(
    store=SQLiteAuthStore("instrument-auth.db"),
    allowed_origins={"http://localhost:5173"},
)
sync = LabSync(auth=auth)
```

An empty persistent store is unconfigured and fails closed for non-loopback
clients. A local UI checks `GET /sync/auth/status`, then calls
`POST /sync/auth/setup` once with the chosen passphrase. The passphrase is
stored as an Argon2id hash and remains valid across restarts until explicitly
rotated. The SQLite file is created with owner-only permissions.

The auth endpoints live below the sync prefix:

- `GET /sync/auth/status`
- `POST /sync/auth/setup` (first run, loopback only)
- `POST /sync/auth/login` with `{ "passphrase": "…" }`
- `POST /sync/auth/invite` with `{ "invite": "…" }`
- `POST /sync/auth/logout`
- `POST /sync/auth/passphrase`
- session list/revocation below `/sync/auth/sessions`
- invitation creation/revocation below `/sync/auth/invites`
- API-token creation/revocation below `/sync/auth/tokens`

Every successful login or invite exchange creates a separate HttpOnly,
SameSite session cookie. Pass `{ "remember": true, "deviceName": "Lab iPad" }`
to persist that hashed session in SQLite for 30 days by default. Normal
sessions last 12 hours and deliberately do not survive a server restart.
Logging out or revoking one browser does not affect the others; passphrase
rotation can revoke all browser sessions.

Short-lived invitations have stable IDs and lifecycle status:

```python
invite = auth.create_invite()
url = f"http://192.168.1.20:8000/#invite={invite.token}"

auth.on_invite_event(
    lambda event: update_safe_reactive_status(event.invite_id, event.status)
)
```

The secret token should be returned only to the requesting host UI. Put only
the invitation ID, expiration, and `active` / `consumed` / `expired` /
`revoked` status in shared reactive state. This lets downstream UIs grey out a
used QR code without polling or broadcasting its credential.

Passphrase attempts are rate-limited, WebSocket origins are checked, and
invitations are stored as hashes and consumed once. Open WebSockets are
periodically revalidated, so an expired or logged-out session cannot remain an
indefinite control channel.

### Principals, capabilities, and scripts

`CommandContext.auth` identifies the authenticated session, local host, or API
token. Browser sessions created with the master passphrase receive `control`
and `manage_access`; invitation sessions receive only `control`. A command
requires `control` by default, or may declare a different capability:

```python
@sync.command(requires={"manage_access"})
def create_remote_invite(ctx: CommandContext):
    return auth.create_invite()
```

Create named API tokens for scripts and show their plaintext value only once:

```python
credential = auth.create_api_token(
    "cooldown monitor",
    capabilities={"read_state"},
)
```

API tokens are sent as `Authorization: Bearer ll_…`, stored only as SHA-256
digests, individually revocable, and may have expiration times. A token without
`control` can connect and receive state but cannot execute ordinary commands.

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
tracked from then on. The _old_ object is orphaned: further writes to it are
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

When authentication supplies a principal, commands require the `control`
capability by default. Use `@sync.command(requires={...})` to declare a more
specific requirement. Open-mode applications and legacy boolean auth backends
continue to work without principals.

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
