import { applyPatch, type PatchOperation } from "../apply-patch.js"
import {
  createSyncRuntime,
  SyncNode,
  type SyncRuntime,
  type SyncFieldMap,
} from "../model/index.js"
import type { StreamHandle } from "../stream-handle.js"

export { createSyncRuntime, SyncNode as SvelteSyncNode }
export type { SyncRuntime, SyncFieldMap }
export { default as AppendGraph } from "./AppendGraph.svelte"

export function useSyncState<T extends Record<string, unknown>>(runtime: SyncRuntime): T {
  let state = $state<T>({} as T)

  runtime.onSnapshot(({ data }) => {
    replaceObject(state, structuredClone(data as T))
  })

  runtime.onPatch(({ patch }) => {
    applyPatch(state, patch as PatchOperation[])
  })

  return state
}

function replaceObject(target: Record<string, unknown>, source: Record<string, unknown>): void {
  for (const key of Object.keys(target)) {
    if (!(key in source)) delete target[key]
  }
  Object.assign(target, source)
}

export function useStream(runtime: SyncRuntime, id: string): StreamHandle {
  return runtime.stream(id)
}
