import { applyPatch, type PatchOperation } from "./apply-patch.js"
import { StreamHandle, type StreamMode } from "./stream-handle.js"

export type ConnectionStatus = "connected" | "disconnected" | "reconnecting" | "error"

type Handler<T> = (data: T) => void
type Unsubscribe = () => void

function sub<T>(handlers: Set<Handler<T>>, handler: Handler<T>): Unsubscribe {
  handlers.add(handler)
  return () => handlers.delete(handler)
}

interface PendingRequest {
  resolve: (version: number) => void
  reject: (err: Error) => void
  timer: ReturnType<typeof setTimeout>
}

export interface SyncClientOptions {
  maxReconnectAttempts?: number
  commandTimeout?: number
}

// Binary frame header format:
// [1 byte: 0x01 = stream_replace]
// [2 bytes: id length (uint16 LE)]
// [N bytes: stream id (UTF-8)]
// [4 bytes: seq (uint32 LE)]
// [rest: Float32Array or Float64Array data]

function decodeBinaryFrame(
  buf: ArrayBuffer,
): { id: string; seq: number; data: Float32Array | Float64Array; dtype: "float32" | "float64" } | null {
  const view = new DataView(buf)
  const type = view.getUint8(0)
  if (type !== 0x01) return null
  const idLen = view.getUint16(1, true)
  const idBytes = new Uint8Array(buf, 3, idLen)
  const id = new TextDecoder().decode(idBytes)
  const seq = view.getUint32(3 + idLen, true)
  const dataOffset = 3 + idLen + 4
  const remaining = buf.byteLength - dataOffset
  // Determine float32 vs float64 by checking if remaining is divisible by 4 but not 8
  // (We can't know for sure without extra metadata; default to float32 for binary frames)
  const data = new Float32Array(buf, dataOffset, remaining / 4)
  return { id, seq, data, dtype: "float32" }
}

export class SyncClient {
  private _url: string
  private _opts: Required<SyncClientOptions>
  private _ws: WebSocket | null = null
  private _state: Record<string, unknown> = {}
  private _reconnectAttempts = 0
  private _reconnectTimer: ReturnType<typeof setTimeout> | null = null

  private _statusHandlers = new Set<Handler<ConnectionStatus>>()
  private _snapshotHandlers = new Set<Handler<{ data: Record<string, unknown>; version: number }>>()
  private _patchHandlers = new Set<Handler<PatchOperation[]>>()

  private _pathListeners = new Map<string, Set<Handler<unknown>>>()
  private _pendingRequests = new Map<string, PendingRequest>()
  private _streams = new Map<string, StreamHandle>()

  status: ConnectionStatus = "disconnected"

  constructor(url: string, options: SyncClientOptions = {}) {
    this._url = url
    this._opts = {
      maxReconnectAttempts: options.maxReconnectAttempts ?? 5,
      commandTimeout: options.commandTimeout ?? 10_000,
    }
  }

  connect(): void {
    this._reconnectAttempts = 0
    this._openSocket()
  }

  disconnect(): void {
    this._reconnectAttempts = this._opts.maxReconnectAttempts // prevent reconnect
    if (this._reconnectTimer) {
      clearTimeout(this._reconnectTimer)
      this._reconnectTimer = null
    }
    this._ws?.close()
    this._setStatus("disconnected")
  }

  get<T = unknown>(path: string): T {
    const parts = path.split(".")
    let current: unknown = this._state
    for (const part of parts) {
      if (current == null || typeof current !== "object") return undefined as T
      current = (current as Record<string, unknown>)[part]
    }
    return current as T
  }

  subscribe(path: string, handler: Handler<unknown>): Unsubscribe {
    if (!this._pathListeners.has(path)) {
      this._pathListeners.set(path, new Set())
    }
    this._pathListeners.get(path)!.add(handler)
    return () => this._pathListeners.get(path)?.delete(handler)
  }

  send(command: string, params: Record<string, unknown> = {}): Promise<number> {
    return new Promise((resolve, reject) => {
      if (!this._ws || this._ws.readyState !== WebSocket.OPEN) {
        reject(new Error("Not connected"))
        return
      }
      const requestId = crypto.randomUUID()
      const timer = setTimeout(() => {
        this._pendingRequests.delete(requestId)
        reject(new Error(`Command "${command}" timed out`))
      }, this._opts.commandTimeout)

      this._pendingRequests.set(requestId, { resolve, reject, timer })
      this._ws.send(JSON.stringify({ type: "command", command, params, requestId }))
    })
  }

  stream(id: string, mode: StreamMode = "replace"): StreamHandle {
    if (!this._streams.has(id)) {
      this._streams.set(
        id,
        new StreamHandle(id, mode, () => this._requestResync(id)),
      )
    }
    return this._streams.get(id)!
  }

  onSnapshot(handler: Handler<{ data: Record<string, unknown>; version: number }>): Unsubscribe {
    return sub(this._snapshotHandlers, handler)
  }

  onPatch(handler: Handler<PatchOperation[]>): Unsubscribe {
    return sub(this._patchHandlers, handler)
  }

  onStatusChange(handler: Handler<ConnectionStatus>): Unsubscribe {
    return sub(this._statusHandlers, handler)
  }

  // ── private ───────────────────────────────────────────────────────────────

  private _openSocket(): void {
    const ws = new WebSocket(this._url)
    this._ws = ws
    ws.binaryType = "arraybuffer"

    ws.onopen = () => {
      this._reconnectAttempts = 0
      this._setStatus("connected")
    }

    ws.onmessage = (event) => {
      if (event.data instanceof ArrayBuffer) {
        this._handleBinary(event.data)
      } else {
        try {
          const msg = JSON.parse(event.data as string)
          this._handleMessage(msg)
        } catch {
          // ignore malformed messages
        }
      }
    }

    ws.onclose = () => {
      if (this._reconnectAttempts < this._opts.maxReconnectAttempts) {
        this._setStatus("reconnecting")
        const delay = Math.min(1000 * 2 ** this._reconnectAttempts, 30_000)
        this._reconnectAttempts++
        this._reconnectTimer = setTimeout(() => this._openSocket(), delay)
      } else {
        this._setStatus("disconnected")
      }
    }

    ws.onerror = () => {
      this._setStatus("error")
    }
  }

  private _handleMessage(msg: Record<string, unknown>): void {
    const type = msg.type as string

    if (type === "snapshot") {
      const data = msg.data as Record<string, unknown>
      const version = msg.version as number
      this._state = structuredClone(data)
      this._snapshotHandlers.forEach((h) => h({ data, version }))
      return
    }

    if (type === "patch") {
      const patch = msg.patch as PatchOperation[]
      applyPatch(this._state, patch)
      this._patchHandlers.forEach((h) => h(patch))
      this._notifyPathListeners(patch)
      return
    }

    if (type === "command_ack") {
      const requestId = msg.requestId as string
      const pending = this._pendingRequests.get(requestId)
      if (pending) {
        clearTimeout(pending.timer)
        this._pendingRequests.delete(requestId)
        pending.resolve(msg.version as number)
      }
      return
    }

    if (type === "command_error") {
      const requestId = msg.requestId as string
      const pending = this._pendingRequests.get(requestId)
      if (pending) {
        clearTimeout(pending.timer)
        this._pendingRequests.delete(requestId)
        pending.reject(new Error(msg.error as string))
      }
      return
    }

    if (type === "stream_snapshot") {
      const id = msg.id as string
      const handle = this._getOrCreateStream(id)
      handle.handleSnapshot(msg as any)
      return
    }

    if (type === "stream_append") {
      const id = msg.id as string
      const handle = this._getOrCreateStream(id, "append")
      handle.handleAppend(msg as any)
      return
    }

    if (type === "stream_replace") {
      const id = msg.id as string
      const handle = this._getOrCreateStream(id, "replace")
      handle.handleReplace(msg as any)
      return
    }

    if (type === "stream_delta") {
      const id = msg.id as string
      const handle = this._getOrCreateStream(id, "int_delta")
      handle.handleDelta(msg as any)
      return
    }
  }

  private _handleBinary(buf: ArrayBuffer): void {
    const decoded = decodeBinaryFrame(buf)
    if (!decoded) return
    const handle = this._getOrCreateStream(decoded.id, "replace")
    handle.handleReplaceBinary(decoded.data, decoded.seq)
  }

  private _getOrCreateStream(id: string, mode: StreamMode = "replace"): StreamHandle {
    if (!this._streams.has(id)) {
      this._streams.set(id, new StreamHandle(id, mode, () => this._requestResync(id)))
    }
    return this._streams.get(id)!
  }

  private _requestResync(id: string): void {
    if (this._ws?.readyState === WebSocket.OPEN) {
      this._ws.send(JSON.stringify({ type: "stream_resync", id }))
    }
  }

  private _setStatus(status: ConnectionStatus): void {
    this.status = status
    this._statusHandlers.forEach((h) => h(status))
  }

  private _notifyPathListeners(patch: PatchOperation[]): void {
    for (const op of patch) {
      // Convert JSON Pointer (/pump/speed) to dot-path (pump.speed)
      const dotPath = op.path
        .split("/")
        .filter(Boolean)
        .join(".")

      for (const [listenPath, handlers] of this._pathListeners) {
        if (dotPath === listenPath || dotPath.startsWith(listenPath + ".")) {
          const value = this.get(listenPath)
          handlers.forEach((h) => h(value))
        }
      }
    }
  }
}

export function createSyncClient(url: string, options?: SyncClientOptions): SyncClient {
  const client = new SyncClient(url, options)
  client.connect()
  return client
}
