# Frontend API

The npm package is split into framework-neutral primitives and optional
adapters.

## Authentication

`lab-link/auth` is a framework-neutral client. An app can use it from its own
login screen while keeping all presentation downstream:

```ts
import { AuthClient, AuthError } from "lab-link/auth"

const auth = new AuthClient()

// Run before connecting the sync runtime. This reads #invite=…, removes it
// from browser history, and exchanges it for an HttpOnly session cookie.
await auth.consumeInviteFragment()

try {
  await auth.login(passphrase)
  location.reload()
} catch (error) {
  if (error instanceof AuthError) showLoginError(error.code)
}
```

Invitation secrets belong in a URL fragment, not the query string: fragments
are not sent in HTTP requests or access logs. The client scrubs the fragment
before exchanging it. A generated invitation expires after five minutes by
default and can be used once.

## Core

`lab-link/core` exposes `SyncConnection`, JSON Pointer helpers, command errors,
and stream handles. Use it when you want transport primitives without object
hydration.

```ts
import { SyncConnection } from "lab-link/core"

const connection = new SyncConnection("ws://localhost:8000/sync/ws")
connection.onCommandError((error) => showError(error.message))
connection.connect()
```

## Model

`lab-link/model` exposes `SyncRuntime` and `SyncNode`. Use this for laboratory
apps whose live frontend state is made of classes with methods, local UI state,
and declared server-synchronized fields.

```ts
class ChannelModel extends SyncNode<ChannelSnapshot> {
  bias_voltage = 0
  editing = false

  override readonly fields = this.defineFields<this>({
    bias_voltage: {
      blockWhen: () => this.editing,
      onBlocked: "queueLatest",
      validateRemote: (value) => typeof value === "number",
      setVia: "set_channel",
    },
  })

  applySnapshot(snapshot: ChannelSnapshot) {
    this.bias_voltage = snapshot.bias_voltage
  }
}
```

Patches are routed to the nearest registered node. If a field is not declared,
the runtime does not mutate the class instance. This keeps server-authoritative
fields explicit and local UI state local.

## Svelte

`lab-link/svelte` exports `createSyncRuntime`, `useSyncState`, `useStream`, and
`SvelteSyncNode`.

```svelte
<script lang="ts">
  import { createSyncRuntime, useSyncState } from "lab-link/svelte"

  const runtime = createSyncRuntime({
    url: `ws://${window.location.host}/sync/ws`,
  })

  const state = useSyncState<{ active: boolean }>(runtime)
</script>
```
