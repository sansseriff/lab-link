export type StreamMode = "append" | "replace" | "int_delta"

type Handler<T> = (data: T) => void
type Unsubscribe = () => void

function sub<T>(handlers: Set<Handler<T>>, handler: Handler<T>): Unsubscribe {
  handlers.add(handler)
  return () => handlers.delete(handler)
}

interface AppendSnapshotMsg {
  type: "stream_snapshot"
  id: string
  data: unknown[]
  seq: number
}

interface AppendMsg {
  type: "stream_append"
  id: string
  data: unknown[]
  seq: number
}

interface ReplaceMsg {
  type: "stream_replace"
  id: string
  data: number[]
  seq: number
}

interface DeltaMsg {
  type: "stream_delta"
  id: string
  deltas: [number, number][]
  seq: number
}

type StreamMsg = AppendSnapshotMsg | AppendMsg | ReplaceMsg | DeltaMsg

export class StreamHandle {
  readonly id: string
  readonly mode: StreamMode

  private _seq: number = -1
  private _requestResync: (() => void) | null = null

  // Append handlers
  private _appendHandlers = new Set<Handler<unknown[]>>()
  private _appendSnapshotHandlers = new Set<Handler<{ buffer: unknown[]; seq: number }>>()

  // Replace handlers
  private _replaceHandlers = new Set<Handler<number[]>>()
  private _replaceBinaryHandlers = new Set<Handler<Float32Array | Float64Array>>()
  private _replaceSnapshotHandlers = new Set<Handler<{ data: number[]; seq: number }>>()

  // Int-delta handlers
  private _deltaHandlers = new Set<Handler<[number, number][]>>()
  private _deltaSnapshotHandlers = new Set<Handler<{ bins: number[]; seq: number }>>()

  constructor(id: string, mode: StreamMode, requestResync: () => void) {
    this.id = id
    this.mode = mode
    this._requestResync = requestResync
  }

  // ── Append mode ──────────────────────────────────────────────────────────

  onAppend(handler: Handler<unknown[]>): Unsubscribe {
    return sub(this._appendHandlers, handler)
  }

  onSnapshot(handler: Handler<{ buffer: unknown[]; seq: number } | { bins: number[]; seq: number } | { data: number[]; seq: number }>): Unsubscribe {
    if (this.mode === "append") {
      return sub(this._appendSnapshotHandlers as any, handler as any)
    } else if (this.mode === "replace") {
      return sub(this._replaceSnapshotHandlers as any, handler as any)
    } else {
      return sub(this._deltaSnapshotHandlers as any, handler as any)
    }
  }

  // ── Replace mode ─────────────────────────────────────────────────────────

  onReplace(handler: Handler<number[]>): Unsubscribe {
    return sub(this._replaceHandlers, handler)
  }

  onReplaceBinary(handler: Handler<Float32Array | Float64Array>): Unsubscribe {
    return sub(this._replaceBinaryHandlers, handler)
  }

  // ── Int-delta mode ────────────────────────────────────────────────────────

  onDelta(handler: Handler<[number, number][]>): Unsubscribe {
    return sub(this._deltaHandlers, handler)
  }

  // ── Internal dispatch ─────────────────────────────────────────────────────

  handleSnapshot(msg: AppendSnapshotMsg | { type: "stream_snapshot"; id: string; data: unknown; seq: number }): void {
    this._seq = (msg as any).seq
    if (this.mode === "append") {
      const data = (msg as AppendSnapshotMsg).data
      this._appendSnapshotHandlers.forEach((h) => h({ buffer: data, seq: msg.seq }))
    } else if (this.mode === "replace") {
      const data = (msg as any).data as number[]
      this._replaceSnapshotHandlers.forEach((h) => h({ data, seq: msg.seq }))
    } else {
      const bins = (msg as any).data as number[]
      this._deltaSnapshotHandlers.forEach((h) => h({ bins, seq: msg.seq }))
    }
  }

  handleAppend(msg: AppendMsg): void {
    if (!this._checkSeq(msg.seq)) return
    this._appendHandlers.forEach((h) => h(msg.data))
  }

  handleReplace(msg: ReplaceMsg): void {
    if (!this._checkSeq(msg.seq)) return
    this._replaceHandlers.forEach((h) => h(msg.data))
  }

  handleReplaceBinary(data: Float32Array | Float64Array, seq: number): void {
    if (!this._checkSeq(seq)) return
    this._replaceBinaryHandlers.forEach((h) => h(data))
  }

  handleDelta(msg: DeltaMsg): void {
    if (!this._checkSeq(msg.seq)) return
    this._deltaHandlers.forEach((h) => h(msg.deltas))
  }

  private _checkSeq(seq: number): boolean {
    if (this._seq === -1) {
      // First message before snapshot — accept it
      this._seq = seq
      return true
    }
    if (seq !== this._seq + 1) {
      // Gap detected — request resync
      this._requestResync?.()
      return false
    }
    this._seq = seq
    return true
  }
}
