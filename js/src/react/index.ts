import { useEffect, useState } from "react"
import { createSyncRuntime, type SyncRuntime } from "../model/index.js"

export { createSyncRuntime }
export type { SyncRuntime }

export function useSyncState<T extends Record<string, unknown>>(runtime: SyncRuntime): T {
  const [state, setState] = useState<T>({} as T)

  useEffect(() => {
    const unsubSnapshot = runtime.onSnapshot(({ data }) => {
      setState(structuredClone(data as T))
    })
    const unsubPatch = runtime.onPatch(() => {
      setState(structuredClone((runtime.snapshot() ?? {}) as T))
    })
    return () => {
      unsubSnapshot()
      unsubPatch()
    }
  }, [runtime])

  return state
}
