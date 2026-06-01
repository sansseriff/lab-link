import { beforeEach, describe, expect, test } from "bun:test"
import { SyncConnection, SyncCommandError } from "../src/core"
import { SyncNode, SyncRuntime } from "../src/model"

class MockWebSocket {
  static OPEN = 1
  static CLOSED = 3
  static instances: MockWebSocket[] = []

  readyState = MockWebSocket.OPEN
  binaryType = "blob"
  sent: string[] = []

  onopen: ((event: Event) => void) | null = null
  onmessage: ((event: MessageEvent) => void) | null = null
  onclose: ((event: CloseEvent) => void) | null = null
  onerror: ((event: Event) => void) | null = null

  constructor(readonly url: string) {
    MockWebSocket.instances.push(this)
    queueMicrotask(() => this.onopen?.(new Event("open")))
  }

  send(data: string): void {
    this.sent.push(data)
  }

  close(): void {
    this.readyState = MockWebSocket.CLOSED
    this.onclose?.(new CloseEvent("close"))
  }

  simulateMessage(data: unknown): void {
    this.onmessage?.(new MessageEvent("message", { data: JSON.stringify(data) }))
  }
}

function installMock(): void {
  MockWebSocket.instances = []
  ;(globalThis as any).WebSocket = MockWebSocket
}

function lastWs(): MockWebSocket {
  return MockWebSocket.instances[MockWebSocket.instances.length - 1]!
}

async function nextMicrotask(): Promise<void> {
  await new Promise((resolve) => queueMicrotask(resolve))
}

class ChannelNode extends SyncNode<{ bias_voltage: number; label: string }> {
  bias_voltage = 0
  label = ""
  editing = false
  applied: unknown[] = []

  override readonly fields = this.defineFields<this>({
    bias_voltage: {
      blockWhen: () => this.editing,
      onBlocked: "queueLatest",
      coerceRemote: (value) => Math.round(Number(value) * 100) / 100,
      validateRemote: (value) => typeof value === "number" && value >= -5 && value <= 5,
      onApplied: (value) => this.applied.push(value),
      setVia: "set_channel",
    },
    label: { writable: false },
  })

  applySnapshot(snapshot: { bias_voltage: number; label: string }): void {
    this.bias_voltage = snapshot.bias_voltage
    this.label = snapshot.label
  }

  setBiasVoltage(value: number): Promise<unknown> {
    return this.setSyncedField("bias_voltage", value)
  }
}

describe("SyncConnection", () => {
  beforeEach(() => installMock())

  test("handles snapshot, patch metadata, and command ack result", async () => {
    const connection = new SyncConnection<{ x: number }>("ws://test")
    const patches: unknown[] = []
    connection.onPatch((event) => patches.push(event))
    connection.connect()
    await nextMicrotask()

    const ws = lastWs()
    ws.simulateMessage({ type: "snapshot", data: { x: 1 }, version: 1 })
    expect(connection.get("/x")).toBe(1)

    const commandPromise = connection.sendCommand<{ rounded: number }>("set_x", { value: 2 })
    const sent = JSON.parse(ws.sent.at(-1)!)
    ws.simulateMessage({
      type: "patch",
      patch: [{ op: "replace", path: "/x", value: 2 }],
      version: 2,
      originClientId: "client-a",
      requestId: sent.requestId,
      command: "set_x",
    })
    ws.simulateMessage({
      type: "command_ack",
      command: "set_x",
      requestId: sent.requestId,
      version: 2,
      result: { rounded: 2 },
    })

    await expect(commandPromise).resolves.toMatchObject({
      version: 2,
      result: { rounded: 2 },
    })
    expect(connection.get("/x")).toBe(2)
    expect((patches[0] as any).meta).toMatchObject({
      version: 2,
      originClientId: "client-a",
      command: "set_x",
    })
  })

  test("rejects command promises with structured command errors", async () => {
    const connection = new SyncConnection("ws://test")
    const errors: SyncCommandError[] = []
    connection.onCommandError((error) => errors.push(error))
    connection.connect()
    await nextMicrotask()

    const ws = lastWs()
    const commandPromise = connection.sendCommand("set_channel")
    const sent = JSON.parse(ws.sent.at(-1)!)
    ws.simulateMessage({
      type: "command_error",
      command: "set_channel",
      requestId: sent.requestId,
      code: "hardware_timeout",
      message: "The voltage source did not respond before the timeout.",
      detail: "UDP timeout after 5.0 s",
      severity: "error",
      display: "banner",
      recoverable: true,
      path: "/data/0/vsource/channels/0",
      version: 4,
    })

    await expect(commandPromise).rejects.toThrow("The voltage source did not respond")
    expect(errors[0]).toBeInstanceOf(SyncCommandError)
    expect(errors[0]?.code).toBe("hardware_timeout")
    expect(errors[0]?.display).toBe("banner")
  })

  test("emits local not_connected failures through global command errors", async () => {
    const connection = new SyncConnection("ws://test")
    const errors: SyncCommandError[] = []
    connection.onCommandError((error) => errors.push(error))

    await expect(connection.sendCommand("set_channel")).rejects.toThrow("Not connected")
    expect(errors[0]?.code).toBe("not_connected")
  })

  test("emits local timeout failures through global command errors", async () => {
    const connection = new SyncConnection("ws://test", { commandTimeoutMs: 1 })
    const errors: SyncCommandError[] = []
    connection.onCommandError((error) => errors.push(error))
    connection.connect()
    await nextMicrotask()

    await expect(connection.sendCommand("set_channel")).rejects.toThrow("timed out")
    expect(errors[0]?.code).toBe("command_timeout")
  })
})

describe("SyncRuntime and SyncNode", () => {
  beforeEach(() => installMock())

  test("routes patches to the nearest registered node and applies field policy", async () => {
    const runtime = new SyncRuntime("ws://test")
    const channel = new ChannelNode(runtime, "/data/2/vsource/channels/7")
    runtime.connect()
    await nextMicrotask()

    lastWs().simulateMessage({
      type: "patch",
      patch: [{ op: "replace", path: "/data/2/vsource/channels/7/bias_voltage", value: 1.234 }],
      version: 45,
    })

    expect(channel.bias_voltage).toBe(1.23)
    expect(channel.applied).toEqual([1.23])
  })

  test("queues latest blocked remote value and flushes it later", async () => {
    const runtime = new SyncRuntime("ws://test")
    const channel = new ChannelNode(runtime, "/data/2/vsource/channels/7")
    channel.editing = true
    runtime.connect()
    await nextMicrotask()

    lastWs().simulateMessage({
      type: "patch",
      patch: [
        { op: "replace", path: "/data/2/vsource/channels/7/bias_voltage", value: 1 },
        { op: "replace", path: "/data/2/vsource/channels/7/bias_voltage", value: 2 },
      ],
      version: 46,
    })
    expect(channel.bias_voltage).toBe(0)

    channel.editing = false
    runtime.flushQueued(channel)
    expect(channel.bias_voltage).toBe(2)
  })

  test("setVia dispatches command with canonical path and value", async () => {
    const runtime = new SyncRuntime("ws://test")
    const channel = new ChannelNode(runtime, "/data/2/vsource/channels/7")
    runtime.connect()
    await nextMicrotask()

    const promise = channel.setBiasVoltage(1.25)
    const sent = JSON.parse(lastWs().sent.at(-1)!)
    expect(sent.command).toBe("set_channel")
    expect(sent.params).toEqual({
      path: "/data/2/vsource/channels/7/bias_voltage",
      value: 1.25,
    })

    lastWs().simulateMessage({
      type: "command_ack",
      command: "set_channel",
      requestId: sent.requestId,
      version: 47,
      result: { bias_voltage: 1.25 },
    })
    await expect(promise).resolves.toMatchObject({ version: 47 })
  })

  test("ignores undeclared fields instead of raw-mutating class instances", async () => {
    const runtime = new SyncRuntime("ws://test")
    const channel = new ChannelNode(runtime, "/data/2/vsource/channels/7") as ChannelNode & {
      serverOnly?: unknown
    }
    runtime.connect()
    await nextMicrotask()

    lastWs().simulateMessage({
      type: "patch",
      patch: [{ op: "add", path: "/data/2/vsource/channels/7/serverOnly", value: "raw" }],
      version: 48,
    })

    expect(channel.serverOnly).toBeUndefined()
  })
})
