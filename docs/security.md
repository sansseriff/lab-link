# Why lab-link Includes Access Control

lab-link can send commands to physical laboratory equipment. That makes an
unprotected local WebSocket more than a normal development-server risk: any
web page open in a browser may attempt a WebSocket connection to `localhost`
or to an instrument address on the local network.

## The browser attack being prevented

Imagine an instrument UI running at `http://192.168.1.50:8000`. While an
operator has an unrelated website open, JavaScript on that site can attempt:

```js
const socket = new WebSocket("ws://192.168.1.50:8000/sync/ws");
socket.addEventListener("open", () => {
  socket.send(JSON.stringify({
    type: "command",
    command: "set_output",
    params: { enabled: true, voltage: 100 },
    requestId: "attack-1",
  }));
});
```

The browser's fetch CORS policy is not a WebSocket authorization mechanism.
A WebSocket handshake includes an `Origin` header, but the **server** must
validate it; merely enabling CORS middleware does not reject a cross-origin
WebSocket. Without a server-side check or credential, knowing or guessing the
instrument's address may be enough to send a command. Related DNS-rebinding
attacks can make an attacker-controlled hostname resolve to a loopback or LAN
address and are especially important when local callers receive extra trust.

This is sometimes described as a drive-by localhost or drive-by LAN attack.
It does not require the malicious page to read instrument state first. The
dangerous part is its ability to open a control channel and write to it.

## What the auth layer guarantees

`LanPassphraseAuth` puts the decision at the boundary that owns the control
channel:

1. The server validates the WebSocket `Origin` during the handshake.
2. The server requires an authenticated session or API token before it sends
   a state snapshot or accepts a command.
3. The authenticated principal is checked again for every command and while a
   socket is idle, so revocation closes an existing control path.
4. Command capabilities are enforced on the server. Hiding a button in the UI
   is never treated as authorization.
5. The same session protects lab-link's HTTP state route and access-management
   endpoints. Applications must apply the check to any additional sensitive
   routes they add.
6. Passphrase attempts are rate-limited. Passphrases use Argon2id hashes, and
   session, invitation, and API-token secrets are stored only as digests.

These checks are deliberately part of lab-link rather than every downstream
UI. They are protocol invariants and must not depend on whether an application
uses Svelte, React, a native webview, or a custom login screen. The application
still owns all presentation: setup pages, login forms, QR codes, wording,
countdowns, and device-management screens.

## Credentials for people, invitations, and scripts

The persistent master passphrase is the recovery path for a human operator.
It remains valid across application restarts until an administrator changes
it. A successful login creates a separate session for that browser:

- a normal session lasts 12 hours and is lost when the server restarts;
- a remembered-device session is stored as a digest and lasts 30 days by
  default;
- either kind can be revoked independently from another authorized device.

One-use invitations are for quickly joining a phone or tablet. Put the secret
in the URL fragment (`#invite=...`), which is not sent in HTTP request targets,
and exchange it immediately for an HttpOnly cookie. An invitation expires
after five minutes by default and becomes invalid as soon as it is consumed.
The stable invitation ID and its non-secret lifecycle status may be projected
through reactive state so the host UI can disable an expired or consumed QR
code without polling.

Scripts should use named, revocable API tokens with only the capabilities they
need. They should not store the master passphrase or copy a browser cookie.

## What this does not protect against

Access control does not encrypt HTTP or `ws://` traffic. A hostile device on a
shared network may be able to observe and replay credentials or commands. Use
HTTPS/WSS, a trusted wired or wireless network, or an encrypted overlay such
as Tailscale when passive network observation is plausible.

An authorized browser, script, or compromised computer can still perform the
actions its capabilities allow. Treat remembered devices and API tokens like
keys: label them, review them, and revoke them when they are no longer needed.

Finally, lab-link can protect only the routes it knows about. Gate any custom
HTTP endpoint that exposes state or causes hardware action with
`auth.is_http_authorized(request)`, and avoid side-effecting `GET` routes.

Continue with the complete [backend setup](backend.md#complete-persistent-setup)
and [frontend startup flow](frontend.md#complete-startup-flow).
