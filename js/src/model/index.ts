import { applyPatch, type PatchOperation } from "../apply-patch.js"
import {
  getJsonPointer,
  joinJsonPointer,
  parseJsonPointer,
  SyncConnection,
  type CommandAck,
  type ConnectionStatus,
  type Handler,
  type JsonPointer,
  type PatchEvent,
  type PatchMeta,
  type SnapshotEvent,
  type SyncConnectionOptions,
  type SyncCommandError,
  type Unsubscribe,
} from "../core/index.js"
import type { StreamHandle, StreamMode } from "../stream-handle.js"

export type BlockedPatchAction<TValue = unknown> =
  | "drop"
  | "queueLatest"
  | ((value: TValue, op: PatchOperation, meta: PatchMeta) => void)

export interface SyncFieldPolicy<TValue = unknown> {
  blockWhen?: () => boolean
  onBlocked?: BlockedPatchAction<TValue>
  validateRemote?: (value: unknown, op: PatchOperation, meta: PatchMeta) => boolean
  coerceRemote?: (value: unknown, op: PatchOperation, meta: PatchMeta) => TValue
  onApplied?: (value: TValue, op: PatchOperation, meta: PatchMeta) => void
  writable?: boolean
  setVia?: string
}

export type SyncFieldMap<TNode extends object> = Partial<{
  [K in keyof TNode & string]: SyncFieldPolicy<TNode[K]>
}>

interface RegisteredNode {
  path: JsonPointer
  node: SyncNode<unknown>
}

interface QueuedRemote {
  value: unknown
  op: PatchOperation
  meta: PatchMeta
  relativePath?: string[]
}

function pathDepth(path: JsonPointer): number {
  return parseJsonPointer(path).length
}

function isPathPrefix(prefix: JsonPointer, path: JsonPointer): boolean {
  if (prefix === "") return true
  return path === prefix || path.startsWith(`${prefix}/`)
}

function patchWithRelativePath(op: PatchOperation, parts: string[]): PatchOperation {
  return { ...op, path: parts.length === 0 ? "" : `/${parts.map(escapeRelative).join("/")}` }
}

function escapeRelative(part: string): string {
  return part.replace(/~/g, "~0").replace(/\//g, "~1")
}

export class SyncRuntime<TSnapshot = unknown> {
  readonly connection: SyncConnection<TSnapshot>

  private nodes = new Map<JsonPointer, RegisteredNode>()
  private queued = new WeakMap<object, Map<string, QueuedRemote>>()

  constructor(urlOrConnection: string | SyncConnection<TSnapshot>, options?: SyncConnectionOptions) {
    this.connection =
      typeof urlOrConnection === "string"
        ? new SyncConnection<TSnapshot>(urlOrConnection, options)
        : urlOrConnection
    this.connection.onPatch((event) => this.routePatch(event))
  }

  connect(): void {
    this.connection.connect()
  }

  disconnect(): void {
    this.connection.disconnect()
  }

  get version(): number {
    return this.connection.version
  }

  get status(): ConnectionStatus {
    return this.connection.status
  }

  snapshot(): TSnapshot | undefined {
    return this.connection.snapshot()
  }

  get<T = unknown>(path: JsonPointer): T {
    return this.connection.get<T>(path)
  }

  sendCommand<TResult = unknown>(
    command: string,
    params: Record<string, unknown> = {},
  ): Promise<CommandAck<TResult>> {
    return this.connection.sendCommand<TResult>(command, params)
  }

  stream(id: string, mode?: StreamMode): StreamHandle {
    return this.connection.stream(id, mode)
  }

  attach(path: JsonPointer, node: SyncNode<unknown>): void {
    this.nodes.set(path, { path, node })
  }

  detach(path: JsonPointer, node: SyncNode<unknown>): void {
    const current = this.nodes.get(path)
    if (current?.node === node) this.nodes.delete(path)
  }

  onStatus(handler: Handler<ConnectionStatus>): Unsubscribe {
    return this.connection.onStatus(handler)
  }

  onSnapshot(handler: Handler<SnapshotEvent<TSnapshot>>): Unsubscribe {
    return this.connection.onSnapshot(handler)
  }

  onPatch(handler: Handler<PatchEvent>): Unsubscribe {
    return this.connection.onPatch(handler)
  }

  onCommandError(handler: Handler<SyncCommandError>): Unsubscribe {
    return this.connection.onCommandError(handler)
  }

  applyPatchToNode(
    node: SyncNode<unknown>,
    relativePath: string[],
    op: PatchOperation,
    meta: PatchMeta,
  ): void {
    if (relativePath.length === 0) {
      node.applySnapshot(op.value)
      return
    }

    const field = relativePath[0]!
    const policy = node.fields[field]
    if (!policy) return

    if (relativePath.length === 1) {
      this.applyFieldPatch(node, field, policy, op, meta)
      return
    }

    this.applyNestedFieldPatch(node, field, relativePath.slice(1), policy, op, meta)
  }

  async setField<TNode extends SyncNode<unknown>>(
    node: TNode,
    field: keyof TNode & string,
    value: unknown,
    params: Record<string, unknown> = {},
  ): Promise<CommandAck | void> {
    const policy = node.fields[field]
    if (policy?.writable === false) {
      throw new Error(`Field "${field}" is read-only.`)
    }
    if (!policy?.setVia) {
      ;(node as Record<string, unknown>)[field] = value
      return
    }
    return this.sendCommand(policy.setVia, {
      path: joinJsonPointer(node.path, field),
      value,
      ...params,
    })
  }

  flushQueued(node?: SyncNode<unknown>, field?: string): void {
    if (node) {
      this.flushQueuedForNode(node, field)
      return
    }
    for (const registered of this.nodes.values()) this.flushQueuedForNode(registered.node)
  }

  private routePatch({ patch, meta }: PatchEvent): void {
    for (const op of patch) {
      const target = this.findNearestNode(op.path)
      if (!target) continue
      const baseParts = parseJsonPointer(target.path)
      const opParts = parseJsonPointer(op.path)
      target.node.applyPatch(opParts.slice(baseParts.length), op, meta)
    }
  }

  private findNearestNode(path: JsonPointer): RegisteredNode | undefined {
    let best: RegisteredNode | undefined
    for (const node of this.nodes.values()) {
      if (!isPathPrefix(node.path, path)) continue
      if (!best || pathDepth(node.path) > pathDepth(best.path)) best = node
    }
    return best
  }

  private applyFieldPatch(
    node: SyncNode<unknown>,
    field: string,
    policy: SyncFieldPolicy,
    op: PatchOperation,
    meta: PatchMeta,
  ): void {
    let value = op.value
    if (policy.coerceRemote) value = policy.coerceRemote(value, op, meta)
    if (policy.validateRemote && !policy.validateRemote(value, op, meta)) return

    if (policy.blockWhen?.()) {
      this.handleBlockedField(node, field, policy, value, op, meta)
      return
    }

    ;(node as Record<string, unknown>)[field] = value
    policy.onApplied?.(value, op, meta)
  }

  private applyNestedFieldPatch(
    node: SyncNode<unknown>,
    field: string,
    relativePath: string[],
    policy: SyncFieldPolicy,
    op: PatchOperation,
    meta: PatchMeta,
  ): void {
    if (policy.blockWhen?.()) {
      this.handleBlockedField(node, field, policy, op.value, op, meta, relativePath)
      return
    }

    const value = (node as Record<string, unknown>)[field]
    if (value == null || typeof value !== "object") return
    applyPatch(value, [patchWithRelativePath(op, relativePath)])
    policy.onApplied?.(value, op, meta)
  }

  private handleBlockedField(
    node: SyncNode<unknown>,
    field: string,
    policy: SyncFieldPolicy,
    value: unknown,
    op: PatchOperation,
    meta: PatchMeta,
    relativePath?: string[],
  ): void {
    const action = policy.onBlocked ?? "drop"
    if (action === "drop") return
    if (action === "queueLatest") {
      let nodeQueue = this.queued.get(node)
      if (!nodeQueue) {
        nodeQueue = new Map()
        this.queued.set(node, nodeQueue)
      }
      nodeQueue.set(field, { value, op, meta, relativePath })
      return
    }
    action(value, op, meta)
  }

  private flushQueuedForNode(node: SyncNode<unknown>, field?: string): void {
    const nodeQueue = this.queued.get(node)
    if (!nodeQueue) return
    const entries = field
      ? ([[field, nodeQueue.get(field)]].filter((entry) => entry[1]) as [string, QueuedRemote][])
      : Array.from(nodeQueue.entries())

    for (const [key, queued] of entries) {
      const policy = node.fields[key]
      if (!policy || policy.blockWhen?.()) continue
      nodeQueue.delete(key)
      if (queued.relativePath) {
        const value = (node as Record<string, unknown>)[key]
        if (value != null && typeof value === "object") {
          applyPatch(value, [patchWithRelativePath(queued.op, queued.relativePath)])
          policy.onApplied?.(value, queued.op, queued.meta)
        }
      } else {
        ;(node as Record<string, unknown>)[key] = queued.value
        policy.onApplied?.(queued.value, queued.op, queued.meta)
      }
    }
  }
}

export abstract class SyncNode<TSnapshot> {
  readonly sync: SyncRuntime
  readonly path: JsonPointer
  readonly fields: SyncFieldMap<this> = {}

  constructor(sync: SyncRuntime, path: JsonPointer) {
    this.sync = sync
    this.path = path
    sync.attach(path, this as unknown as SyncNode<unknown>)
  }

  abstract applySnapshot(snapshot: TSnapshot): void

  applyPatch(relativePath: string[], op: PatchOperation, meta: PatchMeta): void {
    this.sync.applyPatchToNode(this as unknown as SyncNode<unknown>, relativePath, op, meta)
  }

  dispose(): void {
    this.sync.detach(this.path, this as unknown as SyncNode<unknown>)
  }

  protected defineFields<TNode extends object>(fields: SyncFieldMap<TNode>): SyncFieldMap<TNode> {
    return fields
  }

  protected setSyncedField(
    field: keyof this & string,
    value: unknown,
    params?: Record<string, unknown>,
  ): Promise<CommandAck | void> {
    return this.sync.setField(this, field, value, params)
  }

  protected read<T = unknown>(path: JsonPointer = this.path): T {
    return getJsonPointer<T>(this.sync.snapshot(), path)
  }
}

export function createSyncRuntime<TSnapshot = unknown>(
  options: { url: string; autoConnect?: boolean } & SyncConnectionOptions,
): SyncRuntime<TSnapshot> {
  const { url, autoConnect = true, ...connectionOptions } = options
  const runtime = new SyncRuntime<TSnapshot>(url, connectionOptions)
  if (autoConnect) runtime.connect()
  return runtime
}
