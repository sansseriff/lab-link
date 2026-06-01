import { applyPatch, type PatchOperation } from "../apply-patch.js"
import { StreamHandle, type StreamMode } from "../stream-handle.js"

export type JsonPointer = string
export type ConnectionStatus = "connected" | "disconnected" | "reconnecting" | "error"
export type Unsubscribe = () => void
export type Handler<T> = (data: T) => void

export interface SnapshotEvent<TSnapshot = unknown> {
  data: TSnapshot
  version: number
}

export interface PatchMeta {
  version: number
  originClientId?: string
  requestId?: string
  command?: string
}

export interface PatchEvent {
  patch: PatchOperation[]
  meta: PatchMeta
}

export interface CommandAck<TResult = unknown> {
  command: string
  requestId: string
  version: number
  result?: TResult
}

export interface CommandErrorPayload {
  type: "command_error"
  command: string
  requestId?: string
  code: string
  message: string
  detail?: string
  severity: "info" | "warning" | "error"
  display: "toast" | "banner" | "inline"
  recoverable: boolean
  path?: JsonPointer
  originClientId?: string
  version: number
}

export interface SyncConnectionOptions {
  maxReconnectAttempts?: number
  commandTimeoutMs?: number
  reconnectBaseDelayMs?: number
}

interface PendingCommand {
  resolve: (ack: CommandAck) => void
  reject: (err: SyncCommandError) => void
  timer: ReturnType<typeof setTimeout>
}

function subscribe<T>(handlers: Set<Handler<T>>, handler: Handler<T>): Unsubscribe {
  handlers.add(handler)
  return () => handlers.delete(handler)
}

export function parseJsonPointer(path: JsonPointer): string[] {
  if (path === "") return []
  if (!path.startsWith("/")) path = `/${path}`
  return path
    .split("/")
    .slice(1)
    .map((part) => part.replace(/~1/g, "/").replace(/~0/g, "~"))
}

export function escapeJsonPointerPart(part: string): string {
  return part.replace(/~/g, "~0").replace(/\//g, "~1")
}

export function joinJsonPointer(base: JsonPointer, ...parts: string[]): JsonPointer {
  const prefix = base === "/" ? "" : base.replace(/\/$/, "")
  const suffix = parts.map(escapeJsonPointerPart).join("/")
  if (!prefix && !suffix) return ""
  if (!prefix) return `/${suffix}`
  if (!suffix) return prefix
  return `${prefix}/${suffix}`
}

export function getJsonPointer<T = unknown>(doc: unknown, path: JsonPointer): T {
  let current = doc
  for (const part of parseJsonPointer(path)) {
    if (current == null || typeof current !== "object") return undefined as T
    if (Array.isArray(current)) {
      current = current[Number(part)]
    } else {
      current = (current as Record<string, unknown>)[part]
    }
  }
  return current as T
}

function randomRequestId(): string {
  return globalThis.crypto?.randomUUID?.() ?? `req-${Date.now()}-${Math.random()}`
}

function decodeBinaryFrame(
  buf: ArrayBuffer,
): { id: string; seq: number; data: Float32Array } | null {
  const view = new DataView(buf)
  const type = view.getUint8(0)
  if (type !== 0x01) return null
  const idLen = view.getUint16(1, true)
  const idBytes = new Uint8Array(buf, 3, idLen)
  const id = new TextDecoder().decode(idBytes)
  const seq = view.getUint32(3 + idLen, true)
  const dataOffset = 3 + idLen + 4
  return { id, seq, data: new Float32Array(buf, dataOffset) }
}

export class SyncCommandError extends Error implements CommandErrorPayload {
  readonly type = "command_error"
  readonly command: string
  readonly requestId?: string
  readonly code: string
  readonly detail?: string
  readonly severity: "info" | "warning" | "error"
  readonly display: "toast" | "banner" | "inline"
  readonly recoverable: boolean
  readonly path?: JsonPointer
  readonly originClientId?: string
  readonly version: number

  constructor(payload: CommandErrorPayload) {
    super(payload.message)
    this.name = "SyncCommandError"
    this.command = payload.command
    this.requestId = payload.requestId
    this.code = payload.code
    this.detail = payload.detail
    this.severity = payload.severity
    this.display = payload.display
    this.recoverable = payload.recoverable
    this.path = payload.path
    this.originClientId = payload.originClientId
    this.version = payload.version
  }
}

export class SyncConnection<TSnapshot = unknown> {
  readonly url: string
  status: ConnectionStatus = "disconnected"
  version = 0

  private readonly opts: Required<SyncConnectionOptions>
  private ws: WebSocket | null = null
  private reconnectAttempts = 0
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null
  private snapshotData: TSnapshot | undefined

  private statusHandlers = new Set<Handler<ConnectionStatus>>()
  private snapshotHandlers = new Set<Handler<SnapshotEvent<TSnapshot>>>()
  private patchHandlers = new Set<Handler<PatchEvent>>()
  private commandErrorHandlers = new Set<Handler<SyncCommandError>>()
  private pendingCommands = new Map<string, PendingCommand>()
  private streams = new Map<string, StreamHandle>()

  constructor(url: string, options: SyncConnectionOptions = {}) {
    this.url = url
    this.opts = {
      maxReconnectAttempts: options.maxReconnectAttempts ?? 5,
      commandTimeoutMs: options.commandTimeoutMs ?? 10_000,
      reconnectBaseDelayMs: options.reconnectBaseDelayMs ?? 1_000,
    }
  }

  connect(): void {
    this.reconnectAttempts = 0
    this.openSocket()
  }

  disconnect(): void {
    this.reconnectAttempts = this.opts.maxReconnectAttempts
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer)
    this.reconnectTimer = null
    this.ws?.close()
    this.setStatus("disconnected")
  }

  snapshot(): TSnapshot | undefined {
    return structuredClone(this.snapshotData)
  }

  get<T = unknown>(path: JsonPointer): T {
    return getJsonPointer<T>(this.snapshotData, path)
  }

  sendCommand<TResult = unknown>(
    command: string,
    params: Record<string, unknown> = {},
  ): Promise<CommandAck<TResult>> {
    return new Promise((resolve, reject) => {
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
        reject(this.emitCommandError({
          type: "command_error",
          command,
          code: "not_connected",
          message: "Not connected.",
          severity: "error",
          display: "toast",
          recoverable: true,
          version: this.version,
        }))
        return
      }

      const requestId = randomRequestId()
      const timer = setTimeout(() => {
        this.pendingCommands.delete(requestId)
        reject(this.emitCommandError({
          type: "command_error",
          command,
          requestId,
          code: "command_timeout",
          message: `Command "${command}" timed out.`,
          severity: "error",
          display: "toast",
          recoverable: true,
          version: this.version,
        }))
      }, this.opts.commandTimeoutMs)

      this.pendingCommands.set(requestId, {
        resolve: resolve as (ack: CommandAck) => void,
        reject,
        timer,
      })
      this.ws.send(JSON.stringify({ type: "command", command, params, requestId }))
    })
  }

  stream(id: string, mode: StreamMode = "replace"): StreamHandle {
    return this.getOrCreateStream(id, mode)
  }

  onStatus(handler: Handler<ConnectionStatus>): Unsubscribe {
    return subscribe(this.statusHandlers, handler)
  }

  onSnapshot(handler: Handler<SnapshotEvent<TSnapshot>>): Unsubscribe {
    return subscribe(this.snapshotHandlers, handler)
  }

  onPatch(handler: Handler<PatchEvent>): Unsubscribe {
    return subscribe(this.patchHandlers, handler)
  }

  onCommandError(handler: Handler<SyncCommandError>): Unsubscribe {
    return subscribe(this.commandErrorHandlers, handler)
  }

  private openSocket(): void {
    const ws = new WebSocket(this.url)
    this.ws = ws
    ws.binaryType = "arraybuffer"

    ws.onopen = () => {
      this.reconnectAttempts = 0
      this.setStatus("connected")
    }

    ws.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        this.handleBinary(event.data)
        return
      }
      this.handleMessage(JSON.parse(event.data as string) as Record<string, unknown>)
    }

    ws.onclose = () => {
      if (this.reconnectAttempts < this.opts.maxReconnectAttempts) {
        this.setStatus("reconnecting")
        const delay = Math.min(
          this.opts.reconnectBaseDelayMs * 2 ** this.reconnectAttempts,
          30_000,
        )
        this.reconnectAttempts += 1
        this.reconnectTimer = setTimeout(() => this.openSocket(), delay)
      } else {
        this.setStatus("disconnected")
      }
    }

    ws.onerror = () => this.setStatus("error")
  }

  private handleMessage(msg: Record<string, unknown>): void {
    if (msg.type === "snapshot") {
      this.version = msg.version as number
      this.snapshotData = structuredClone(msg.data as TSnapshot)
      this.snapshotHandlers.forEach((handler) => {
        handler({ data: structuredClone(this.snapshotData) as TSnapshot, version: this.version })
      })
      return
    }

    if (msg.type === "patch") {
      const patch = msg.patch as PatchOperation[]
      this.version = msg.version as number
      if (this.snapshotData !== undefined) applyPatch(this.snapshotData, patch)
      const meta: PatchMeta = {
        version: this.version,
        originClientId: msg.originClientId as string | undefined,
        requestId: msg.requestId as string | undefined,
        command: msg.command as string | undefined,
      }
      this.patchHandlers.forEach((handler) => handler({ patch, meta }))
      return
    }

    if (msg.type === "command_ack") {
      const requestId = msg.requestId as string
      const pending = this.pendingCommands.get(requestId)
      if (!pending) return
      clearTimeout(pending.timer)
      this.pendingCommands.delete(requestId)
      pending.resolve({
        command: msg.command as string,
        requestId,
        version: msg.version as number,
        result: msg.result,
      })
      return
    }

    if (msg.type === "command_error") {
      const error = new SyncCommandError(msg as unknown as CommandErrorPayload)
      if (error.requestId) {
        const pending = this.pendingCommands.get(error.requestId)
        if (pending) {
          clearTimeout(pending.timer)
          this.pendingCommands.delete(error.requestId)
          pending.reject(error)
        }
      }
      this.commandErrorHandlers.forEach((handler) => handler(error))
      return
    }

    this.handleStreamMessage(msg)
  }

  private emitCommandError(payload: CommandErrorPayload): SyncCommandError {
    const error = new SyncCommandError(payload)
    this.commandErrorHandlers.forEach((handler) => handler(error))
    return error
  }

  private handleStreamMessage(msg: Record<string, unknown>): void {
    const id = msg.id as string | undefined
    if (!id) return
    if (msg.type === "stream_snapshot") this.getOrCreateStream(id).handleSnapshot(msg as any)
    if (msg.type === "stream_append") this.getOrCreateStream(id, "append").handleAppend(msg as any)
    if (msg.type === "stream_replace") this.getOrCreateStream(id, "replace").handleReplace(msg as any)
    if (msg.type === "stream_delta") this.getOrCreateStream(id, "int_delta").handleDelta(msg as any)
  }

  private handleBinary(buf: ArrayBuffer): void {
    const decoded = decodeBinaryFrame(buf)
    if (!decoded) return
    this.getOrCreateStream(decoded.id, "replace").handleReplaceBinary(decoded.data, decoded.seq)
  }

  private getOrCreateStream(id: string, mode: StreamMode = "replace"): StreamHandle {
    let stream = this.streams.get(id)
    if (!stream) {
      stream = new StreamHandle(id, mode, () => this.requestStreamResync(id))
      this.streams.set(id, stream)
    }
    return stream
  }

  private requestStreamResync(id: string): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify({ type: "stream_resync", id }))
    }
  }

  private setStatus(status: ConnectionStatus): void {
    this.status = status
    this.statusHandlers.forEach((handler) => handler(status))
  }
}

export function createSyncConnection<TSnapshot = unknown>(
  url: string,
  options?: SyncConnectionOptions,
): SyncConnection<TSnapshot> {
  const connection = new SyncConnection<TSnapshot>(url, options)
  connection.connect()
  return connection
}
