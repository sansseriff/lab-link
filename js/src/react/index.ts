/**
 * React adapter stub for lab-sync.
 * Full implementation pending — see svelte adapter for the pattern.
 */
import { useEffect, useRef, useState } from "react"
import { SyncClient } from "../client.js"
import type { SyncClientOptions } from "../client.js"

export { SyncClient }
export type { SyncClientOptions }

export function createSyncClient(url: string, options?: SyncClientOptions): SyncClient {
  const client = new SyncClient(url, options)
  client.connect()
  return client
}

export function useSyncState<T extends Record<string, unknown>>(
  client: SyncClient,
): T {
  const [state, setState] = useState<T>({} as T)

  useEffect(() => {
    const unsubSnapshot = client.onSnapshot(({ data }) => {
      setState({ ...data } as T)
    })

    const unsubPatch = client.onPatch(() => {
      // Re-read the full state from client on every patch.
      // A production implementation would apply patches granularly.
      setState({ ...(client.get<T>("") ?? {}) } as T)
    })

    return () => {
      unsubSnapshot()
      unsubPatch()
    }
  }, [client])

  return state
}
