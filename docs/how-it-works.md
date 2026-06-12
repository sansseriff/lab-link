# How lab-link Works

This page explains the ideas behind lab-link from the ground up: why it is
server-authoritative, what actually travels over the WebSocket, and how the
reactive state engine turns ordinary Python assignments into synchronized
updates. Nothing here is required reading to *use* the library — but it makes
the API choices predictable.

## The problem

Laboratory control software has an awkward shape. The hardware — voltage
sources, ADCs, cryostats — is attached to one computer, and only one process
can own it. But the people using it want a browser GUI, scripts want to
automate it, and a second laptop across the lab wants to watch a measurement
in progress. All of those viewers must agree about the state of the system:
which channels are on, what the setpoints are, what the last reading was.

The classic failure mode is to give every viewer its own copy of the state
and try to keep the copies consistent with ad-hoc REST calls and polling.
Copies drift. Two users change the same setting. A script reads a value the
GUI changed a second ago. lab-link's answer is old and boring, which is why
it works:

> **There is exactly one authoritative copy of the state, and it lives in the
> Python process that owns the hardware.** Everyone else holds a read-only
> replica that the server keeps up to date, and asks the server to make
> changes on their behalf.

Everything in lab-link follows from that one sentence.

## The state is a document

The authoritative state is a [pydantic](https://docs.pydantic.dev) model — a
typed tree of models, lists, and dicts. Serialized to JSON, it is a single
*document*:

```json
{
  "enabled": true,
  "channels": [
    {"bias_voltage": 1.25, "active": true},
    {"bias_voltage": 0.0,  "active": false}
  ]
}
```

Two standards make a JSON document cheap to synchronize:

- **JSON Pointer** (RFC 6901) gives every value a path:
  `/channels/0/bias_voltage`.
- **JSON Patch** (RFC 6902) describes edits as a list of operations on those
  paths: `{"op": "replace", "path": "/channels/0/bias_voltage", "value": 1.3}`.

So a replica never needs to re-download the world. It downloads the document
once, then applies a stream of small patches.

## The wire protocol

A client connects to the WebSocket and the server immediately sends a
**snapshot** — the whole document plus a version number:

```json
{"type": "snapshot", "data": {...}, "version": 41}
```

From then on, every state change reaches every connected client as one
**patch** message:

```json
{"type": "patch", "patch": [{"op": "replace", "path": "/enabled", "value": false}], "version": 42}
```

The version increments by exactly one per patch message. That is the
client's consistency guarantee: if it last saw version 41 and receives
version 42, applying the patch yields precisely the server's version-42
document. A gap means a missed message, and the client can re-request a
snapshot rather than continue from a silently wrong replica.

Clients never edit their replica directly. To change something they send a
**command** — an application-defined verb, not a low-level write:

```json
{"type": "command", "command": "set_voltage", "params": {"channel": 0, "value": 1.3}, "requestId": "r7"}
```

The server runs the matching handler, which talks to the hardware and then
mutates the state. Whatever changed goes out as ordinary patch messages, and
only after every one of those patches has been sent does the server reply:

```json
{"type": "command_ack", "command": "set_voltage", "requestId": "r7", "version": 43, "result": {...}}
```

The ordering is a deliberate invariant: when a client receives the ack, it
already holds the post-command state, and the ack's version says exactly
which document version that is. Failures arrive as a structured
`command_error` carrying a machine-readable code and display hints instead of
a stack trace.

One more refinement: patches caused by a command carry that command's
`originClientId`, `requestId`, and `command`. A GUI that optimistically moved
a slider can recognize its own write coming back ("echo suppression") and
distinguish it from a colleague moving the same slider on another laptop.

## The reactive engine

Everything above describes what crosses the network. The interesting question
on the server is: *who writes the patches?*

Early versions of lab-link made the application do it. You held your pydantic
models for the hardware logic, and lab-link held a separate JSON dict for the
wire, and after every hardware action you mirrored the change across with
`sync.set("/channels/0/bias_voltage", v)`. Two copies of the truth, kept in
agreement by hand — a "split brain". Real applications grew a hundred lines
of publish helpers, and forgetting one meant a GUI that silently disagreed
with the instrument.

The reactive engine deletes that job. State models subclass `ReactiveModel`,
one instance is bound, and from then on plain Python is the entire API:

```python
class Channel(ReactiveModel):
    bias_voltage: float = 0.0
    active: bool = False

class AppState(ReactiveModel):
    enabled: bool = True
    channels: list[Channel] = Field(default_factory=lambda: [Channel(), Channel()])

sync = LabSync()
state = sync.bind_state(AppState())

state.channels[0].bias_voltage = 1.3   # ← this is a broadcast
```

That assignment does five things, in order:

1. **Validate.** `ReactiveModel` enables pydantic's `validate_assignment`, so
   the value is checked and coerced against the field's type *before* it
   enters the state. Invalid writes raise immediately at the assignment site.
2. **Name the change.** Every node in the tree keeps a private backref to its
   parent and its key under that parent. Walking backrefs to the root and
   joining the keys yields the JSON Pointer — here, the channel knows it is
   item `0` of a list that is field `channels` of the root, so the path is
   `/channels/0/bias_voltage`.
3. **Record.** The op (`replace` that path with `1.3`) is appended to a
   buffer called the *change sink*. Nothing is sent yet.
4. **Batch.** The first record of an event-loop tick schedules a flush with
   `loop.call_soon`. Every other mutation in the same tick — the rest of the
   command handler, a loop over twenty channels — lands in the same buffer.
   The flush turns the whole buffer into *one* patch message and bumps the
   version *once*. Consecutive writes to the same path coalesce to the final
   value. (To batch across `await`s, wrap the writes in `with sync.batch():`.)
5. **Mirror and broadcast.** The flushed ops are applied to a plain-dict
   *mirror* of the document — the thing snapshots and `GET /state` are served
   from — and the message goes to every client, tagged with the metadata of
   whichever command was running when the writes were recorded.

The mirror deserves a sentence: it is not a second brain, it is a cache that
only ever changes by applying the very ops that were just broadcast. The live
model and the mirror cannot disagree unless the recorded ops are wrong — which
is exactly what the test suite's oracle checks (`mirror == model_dump()` after
every flush, for randomized mutation sequences).

### Lists, dicts, and structure

Scalar assignment is the easy case. Structural edits work because container
fields are silently wrapped: a `list` field becomes a `ReactiveList`, a
`dict` field a `ReactiveDict`. `append`, `insert`, `pop`, `del`, item
assignment, `update` — each records its RFC 6902 equivalent (`add`,
`remove`, `replace`). After an insert or remove, the wrapper re-keys the
children behind the edit so their backrefs name their *new* indices; a later
write to a shifted module records the path it lives at now, not the one it
was created at.

Some containers cannot be tracked — `set` mutates with no notion of a path,
and a plain (non-reactive) `BaseModel` child would swallow writes invisibly.
Rather than silently losing reactivity, binding such a tree raises
`TypeError` at construction time.

### Replacement and orphans

Assigning a whole subtree — `state.channels[0] = Channel(...)` — emits a
single `replace` op whose value is the serialized subtree, and the new child
is adopted into the tree. The *old* object is orphaned: its backrefs are
cleared, so a later write to it finds no sink and is dropped (with a debug
log). That sounds like data loss until you remember the document model: the
old object is simply no longer part of the state. A polling loop that still
holds a reference to a swapped-out module can keep assigning readings to it
harmlessly — the writes go nowhere because the object *is* nowhere.

The complementary rule is that an object lives at exactly one place in the
tree. Move a child (`m = state.channels.pop(0); state.spare = m`) and it
records under its new path from then on.

### Two loud guard rails

Batching and version ordering only make sense on one thread: the event
loop's. A mutation from inside `asyncio.to_thread(...)` would interleave with
a flush in progress, so the sink checks and raises `RuntimeError` instead of
corrupting the stream. Do the blocking hardware I/O in the worker thread,
then assign the result *after* the `await`.

Before the app's lifespan starts there is no loop and there are no clients —
mutations during import or setup just update the mirror silently. The bound
model simply *is* the initial state.

## The rest of the machinery

**Commands** are plain functions registered with `@sync.command`. Parameters
arrive from the client's JSON; a `ctx: CommandContext` parameter, if declared,
receives the caller's identity. The return value rides back on the ack.
Raising `CommandError` produces the structured error message.

**Updaters** (`@sync.updater(interval=0.1)`) are background polling loops for
hardware that must be read continuously. They mutate state like any other
code; each tick's changes batch into one patch with no command metadata.

**Streams** carry high-rate numeric series — an ADC trace at hundreds of
points per second — as binary frames *outside* the state document, with
ring-buffer snapshots for late joiners. State is for values with identity
("the bias voltage of channel 0"); streams are for values whose history is
the point.

**Persistence** (`LabSync(persist=True)`) debounces mirror snapshots into
SQLite and restores them on startup via `load_state()` — the bulk-restore
path that validates a saved document, swaps the bound instance's contents in
place (so references held elsewhere stay valid), and emits one
whole-document patch.

**Clients**: the browser runtime ([Frontend API](frontend.md)) and the Python
client ([Python Client API](python-client.md)) both speak the protocol above
— snapshot, patches, commands, acks. A pytest script and a Svelte GUI are the
same kind of citizen.

## A button click, end to end

1. A user drags a bias-voltage slider; the browser sends
   `{"type": "command", "command": "set_voltage", "params": {"channel": 0, "value": 1.3}, "requestId": "r7"}`.
2. The handler runs on the server: it tells the instrument to apply 1.3 V
   (in `asyncio.to_thread`, since the driver blocks), awaits the result, then
   assigns `state.channels[0].bias_voltage = 1.3`.
3. The assignment validates, records
   `replace /channels/0/bias_voltage → 1.3`, and schedules a flush.
4. The flush applies the op to the mirror, bumps the version to 43, and
   broadcasts one patch message tagged with `requestId: "r7"` — to the
   originating browser, the colleague's laptop, and a logging script alike.
5. The dispatcher waits until that patch is on the wire, then sends
   `command_ack` with `version: 43`.
6. The originating browser sees its own `requestId` on the patch and
   reconciles its optimistic slider; every other client just applies the
   patch. All replicas now read version 43, and all of them are right.
