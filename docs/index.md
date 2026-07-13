# lab-link

`lab-link` is a small synchronization library for laboratory control software.
It keeps Python instrument services authoritative while giving browser UIs a
typed, reactive view of state.

Because browser pages can attempt cross-origin WebSocket connections to
localhost and private-network addresses, lab-link also provides server-side
origin validation, persistent passphrase sessions, one-use invitations, and
scoped API tokens. See [Why lab-link includes access control](security.md) for
the laboratory-control threat model and the boundary between the library and
an application's UI.

The core protocol is simple:

1. A browser connects over WebSocket and receives a state snapshot.
2. The server broadcasts JSON Patch updates with version and command metadata.
3. The browser sends commands.
4. The server validates, performs side effects, commits state, then sends patch
   and command acknowledgement or a structured command error.

## Packages

`lab-link` is intentionally split into two packages:

- `lab-link` on PyPI: the Starlette/Pydantic backend runtime and Python sync
  client.
- `lab-link` on npm: framework-neutral browser runtime plus Svelte and React
  adapters.

Publish both packages from the same git tag and keep their versions aligned.
That makes app dependency constraints easy to reason about: backend and frontend
protocol changes share one semantic version.

## Start Here

- [Get started](get-started.md)
- [How lab-link works](how-it-works.md)
- [Why lab-link includes access control](security.md)
- [Backend API](backend.md)
- [Python Client API](python-client.md)
- [Frontend API](frontend.md)
- [Publishing](publishing.md)
