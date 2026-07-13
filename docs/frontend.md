# Frontend API

The npm package is split into framework-neutral primitives and optional
adapters.

## Authentication

`lab-link/auth` is a framework-neutral client. An app can use it from its own
login screen while keeping all presentation downstream:

The auth step must finish **before** constructing or connecting the sync
runtime. A login screen that merely hides the controls is insufficient: the
server session established here is what permits the WebSocket handshake.

### Complete startup flow

```ts
import { AuthClient, AuthError } from "lab-link/auth";

const auth = new AuthClient();

async function authorize(): Promise<void> {
  let status = await auth.status();

  if (!status.configured) {
    // Show this screen only on the instrument computer. The server rejects
    // first-run setup from non-loopback clients.
    const chosenPassphrase = await showFirstRunSetup();
    status = await auth.setup(chosenPassphrase, {
      remember: true,
      deviceName: "Instrument host",
    });
  }

  if (!status.authorized && location.hash.includes("invite=")) {
    // Reads #invite=…, scrubs it from browser history, and exchanges it for
    // an HttpOnly session cookie. An error means the link expired or was used.
    await auth.consumeInviteFragment("invite", {
      remember: true,
      deviceName: describeThisDevice(),
    });
    status = await auth.status();
  }

  while (!status.authorized) {
    const { passphrase, remember, deviceName } = await showLoginForm();
    try {
      status = await auth.login(passphrase, { remember, deviceName });
    } catch (error) {
      if (error instanceof AuthError) showLoginError(error.code);
      else throw error;
    }
  }
}

async function startApplication(): Promise<void> {
  try {
    await authorize();
    connectSyncRuntime(); // Only now open /sync/ws.
  } catch (error) {
    if (error instanceof AuthError) showLoginError(error.code);
    else throw error;
  }
}

void startApplication();
```

Invitation secrets belong in a URL fragment, not the query string: fragments
are not sent in HTTP requests or access logs. The client scrubs the fragment
before exchanging it. A generated invitation expires after five minutes by
default and can be used once.

Host/admin interfaces can use the same headless client to call
`createInvite()`, observe safe invitation status projected through the app's
reactive model, list or revoke `sessions()`, call `revokeAllSessions()`, change
the passphrase, and create or revoke scoped API tokens. lab-link deliberately
does not provide the visual setup, device-management, countdown, or reset UI.

For example, a downstream remote-access panel can issue a fresh invitation
and build one URL for each reachable host address:

```ts
const invite = await auth.createInvite(5 * 60);
const urls = hostAddresses.map((host) =>
  `http://${host}:8000/#invite=${encodeURIComponent(invite.token)}`
);

showQrCodeAndLinks({
  inviteId: invite.id,
  expiresAt: invite.expiresAt,
  urls,
});
```

Do not place `invite.token` in shared reactive state: every connected client
would receive it. Return it only to the authorized caller that requested the
invitation. Publish the ID and status instead, and disable the QR code and
copy buttons when the server reports `consumed`, `expired`, or `revoked`.

The master passphrase should not be saved in local storage. A remembered login
uses an HttpOnly cookie that frontend JavaScript cannot read; if that cookie is
lost, the operator can log in again with the stable master passphrase. See the
[security model](security.md) for credential roles and limitations.

## Core

`lab-link/core` exposes `SyncConnection`, JSON Pointer helpers, command errors,
and stream handles. Use it when you want transport primitives without object
hydration.

```ts
import { SyncConnection } from "lab-link/core";

const connection = new SyncConnection("ws://localhost:8000/sync/ws");
connection.onCommandError((error) => showError(error.message));
connection.connect();
```

## Model

`lab-link/model` exposes `SyncRuntime` and `SyncNode`. Use this for laboratory
apps whose live frontend state is made of classes with methods, local UI state,
and declared server-synchronized fields.

```ts
class ChannelModel extends SyncNode<ChannelSnapshot> {
  bias_voltage = 0;
  editing = false;

  override readonly fields = this.defineFields<this>({
    bias_voltage: {
      blockWhen: () => this.editing,
      onBlocked: "queueLatest",
      validateRemote: (value) => typeof value === "number",
      setVia: "set_channel",
    },
  });

  applySnapshot(snapshot: ChannelSnapshot) {
    this.bias_voltage = snapshot.bias_voltage;
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
