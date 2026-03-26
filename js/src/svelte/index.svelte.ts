/**
 * Svelte 5 adapter for lab-sync.
 * File uses .svelte.ts extension so the Svelte compiler processes runes ($state, $effect).
 */
import { SyncClient } from "../client.js"
import type { SyncClientOptions } from "../client.js"
import type { StreamHandle } from "../stream-handle.js"
import type { PatchOperation } from "../apply-patch.js"

export { SyncClient }
export type { SyncClientOptions }
export { default as AppendGraph } from './AppendGraph.svelte'

export function createSyncClient(url: string, options?: SyncClientOptions): SyncClient {
  const client = new SyncClient(url, options)
  client.connect()
  return client
}

export function useSyncState<T extends Record<string, unknown>>(client: SyncClient): T {
  let state = $state<T>({} as T)

  client.onSnapshot(({ data }) => {
    Object.assign(state, data)
  })

  client.onPatch((patches) => {
    for (const op of patches) {
      _applyPatchToProxy(state, op)
    }
  })

  return state
}

export function useStream(client: SyncClient, id: string): StreamHandle {
  return client.stream(id)
}

/**
 * Walk a JSON Pointer path through the Svelte $state proxy chain and assign at leaf.
 * Reading each segment through the proxy registers it as a reactive dependency,
 * so only components that read a specific path re-render when that path changes.
 */
function _applyPatchToProxy(state: Record<string, unknown>, op: PatchOperation): void {
  const parts = op.path.split("/").filter(Boolean)
  if (parts.length === 0) return

  let target: unknown = state
  for (let i = 0; i < parts.length - 1; i++) {
    if (target == null || typeof target !== "object") return
    target = (target as Record<string, unknown>)[parts[i]]
  }

  const leaf = parts[parts.length - 1]
  if (target == null || typeof target !== "object") return

  if (op.op === "replace" || op.op === "add") {
    if (leaf === "-" && Array.isArray(target)) {
      ;(target as unknown[]).push(op.value)
    } else {
      ;(target as Record<string, unknown>)[leaf] = op.value
    }
  } else if (op.op === "remove") {
    if (Array.isArray(target)) {
      ;(target as unknown[]).splice(Number(leaf), 1)
    } else {
      delete (target as Record<string, unknown>)[leaf]
    }
  }
}
