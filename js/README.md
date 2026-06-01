# lab-link JavaScript

Browser runtime and framework adapters for `lab-link`.

```bash
bun add lab-link
```

```ts
import { createSyncRuntime } from "lab-link/model"

const runtime = createSyncRuntime({
  url: `ws://${window.location.host}/sync/ws`,
})
```

Exports:

- `lab-link/core`: WebSocket transport, command promises, JSON Pointer helpers.
- `lab-link/model`: `SyncRuntime`, `SyncNode`, field policies.
- `lab-link/svelte`: Svelte 5 helpers.
- `lab-link/react`: React helpers.

Full docs: https://sansseriff.github.io/lab-link/
