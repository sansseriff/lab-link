import { describe, test, expect, beforeEach } from "bun:test"
import { SyncClient } from "../src/client"

// Minimal WebSocket mock
class MockWebSocket {
  static OPEN = 1
  static CLOSED = 3
  static instances: MockWebSocket[] = []

  readyState = MockWebSocket.OPEN
  binaryType: string = "blob"
  url: string
  sent: string[] = []

  onopen: ((e: Event) => void) | null = null
  onmessage: ((e: MessageEvent) => void) | null = null
  onclose: ((e: CloseEvent) => void) | null = null
  onerror: ((e: Event) => void) | null = null

  constructor(url: string) {
    this.url = url
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

function installMock() {
  MockWebSocket.instances = []
  ;(globalThis as any).WebSocket = MockWebSocket
}

function lastWs(): MockWebSocket {
  return MockWebSocket.instances[MockWebSocket.instances.length - 1]
}

describe("SyncClient", () => {
  beforeEach(() => {
    installMock()
  })

  test("get() returns undefined on empty state", () => {
    const client = new SyncClient("ws://test")
    expect(client.get("x")).toBeUndefined()
  })

  test("get() traverses dot-path", () => {
    const client = new SyncClient("ws://test") as any
    client._state = { pump: { speed: 1500 } }
    expect(client.get("pump.speed")).toBe(1500)
  })

  test("onSnapshot fires and sets state", async () => {
    const client = new SyncClient("ws://test")
    const snapshots: unknown[] = []
    client.onSnapshot((s) => snapshots.push(s))
    client.connect()

    await new Promise((r) => queueMicrotask(r as any))
    const ws = lastWs()
    ws.simulateMessage({ type: "snapshot", data: { x: 5 }, version: 1 })

    expect(snapshots.length).toBe(1)
    expect((snapshots[0] as any).data.x).toBe(5)
    expect(client.get("x")).toBe(5)
  })

  test("onPatch applies patch to state", async () => {
    const client = new SyncClient("ws://test") as any
    client._state = { x: 0 }
    client.connect()

    await new Promise((r) => queueMicrotask(r as any))
    const ws = lastWs()
    ws.simulateMessage({
      type: "patch",
      patch: [{ op: "replace", path: "/x", value: 99 }],
      version: 2,
    })

    expect(client.get("x")).toBe(99)
  })

  test("send() resolves on command_ack", async () => {
    const client = new SyncClient("ws://test")
    client.connect()

    await new Promise((r) => queueMicrotask(r as any))
    const ws = lastWs()

    const promise = client.send("set_x", { value: 5 })

    const sent = JSON.parse(ws.sent[ws.sent.length - 1])
    expect(sent.type).toBe("command")
    expect(sent.command).toBe("set_x")

    ws.simulateMessage({
      type: "command_ack",
      command: "set_x",
      requestId: sent.requestId,
      version: 3,
    })

    const version = await promise
    expect(version).toBe(3)
  })

  test("send() rejects on command_error", async () => {
    const client = new SyncClient("ws://test")
    client.connect()

    await new Promise((r) => queueMicrotask(r as any))
    const ws = lastWs()

    const promise = client.send("bad_cmd", {})
    const sent = JSON.parse(ws.sent[ws.sent.length - 1])

    ws.simulateMessage({
      type: "command_error",
      command: "bad_cmd",
      requestId: sent.requestId,
      error: "Unknown command",
    })

    await expect(promise).rejects.toThrow("Unknown command")
  })

  test("subscribe fires on matching patch path", async () => {
    const client = new SyncClient("ws://test") as any
    client._state = { pump: { speed: 0 } }
    client.connect()

    await new Promise((r) => queueMicrotask(r as any))
    const ws = lastWs()

    const values: unknown[] = []
    client.subscribe("pump.speed", (v: unknown) => values.push(v))

    ws.simulateMessage({
      type: "patch",
      patch: [{ op: "replace", path: "/pump/speed", value: 1500 }],
      version: 2,
    })

    expect(values.length).toBe(1)
    expect(values[0]).toBe(1500)
  })
})
