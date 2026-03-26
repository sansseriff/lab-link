import { describe, test, expect } from "bun:test"
import { StreamHandle } from "../src/stream-handle"
import { applyPatch } from "../src/apply-patch"

describe("StreamHandle", () => {
  test("append: onSnapshot fires on snapshot message", () => {
    const handle = new StreamHandle("temps", "append", () => {})
    const received: unknown[] = []
    handle.onSnapshot((s) => received.push(s))

    handle.handleSnapshot({ type: "stream_snapshot", id: "temps", data: [1, 2, 3], seq: 5 })
    expect(received.length).toBe(1)
    expect((received[0] as any).seq).toBe(5)
    expect((received[0] as any).buffer).toEqual([1, 2, 3])
  })

  test("append: onAppend fires on append message", () => {
    const handle = new StreamHandle("temps", "append", () => {})
    handle.handleSnapshot({ type: "stream_snapshot", id: "temps", data: [], seq: 0 })

    const points: unknown[][] = []
    handle.onAppend((p) => points.push(p))
    handle.handleAppend({ type: "stream_append", id: "temps", data: [42], seq: 1 })

    expect(points.length).toBe(1)
    expect(points[0]).toEqual([42])
  })

  test("seq gap triggers resync", () => {
    let resyncCalled = false
    const handle = new StreamHandle("temps", "append", () => {
      resyncCalled = true
    })
    handle.handleSnapshot({ type: "stream_snapshot", id: "temps", data: [], seq: 10 })
    handle.handleAppend({ type: "stream_append", id: "temps", data: [1], seq: 15 }) // gap!
    expect(resyncCalled).toBe(true)
  })

  test("replace: onReplace fires", () => {
    const handle = new StreamHandle("fft", "replace", () => {})
    handle.handleSnapshot({ type: "stream_snapshot", id: "fft", data: [], seq: 0 })

    const received: number[][] = []
    handle.onReplace((d) => received.push(d))
    handle.handleReplace({ type: "stream_replace", id: "fft", data: [0.1, 0.2], seq: 1 })

    expect(received.length).toBe(1)
    expect(received[0]).toEqual([0.1, 0.2])
  })

  test("int_delta: onDelta fires", () => {
    const handle = new StreamHandle("hist", "int_delta", () => {})
    handle.handleSnapshot({ type: "stream_snapshot", id: "hist", data: new Array(10).fill(0), seq: 0 })

    const deltas: [number, number][][] = []
    handle.onDelta((d) => deltas.push(d))
    handle.handleDelta({ type: "stream_delta", id: "hist", deltas: [[4, 3]], seq: 1 })

    expect(deltas.length).toBe(1)
    expect(deltas[0]).toEqual([[4, 3]])
  })
})

describe("applyPatch", () => {
  test("replace scalar", () => {
    const doc = { x: 0 }
    applyPatch(doc, [{ op: "replace", path: "/x", value: 5 }])
    expect(doc.x).toBe(5)
  })

  test("replace nested", () => {
    const doc: any = { pump: { speed: 0 } }
    applyPatch(doc, [{ op: "replace", path: "/pump/speed", value: 1500 }])
    expect(doc.pump.speed).toBe(1500)
  })

  test("add to object", () => {
    const doc: any = {}
    applyPatch(doc, [{ op: "add", path: "/y", value: 42 }])
    expect(doc.y).toBe(42)
  })

  test("remove key", () => {
    const doc: any = { a: 1, b: 2 }
    applyPatch(doc, [{ op: "remove", path: "/a" }])
    expect(doc.a).toBeUndefined()
    expect(doc.b).toBe(2)
  })

  test("add to array with -", () => {
    const doc: any = { items: [1, 2] }
    applyPatch(doc, [{ op: "add", path: "/items/-", value: 3 }])
    expect(doc.items).toEqual([1, 2, 3])
  })

  test("remove from array", () => {
    const doc: any = { items: [1, 2, 3] }
    applyPatch(doc, [{ op: "remove", path: "/items/1" }])
    expect(doc.items).toEqual([1, 3])
  })

  test("multiple ops applied in order", () => {
    const doc: any = { x: 0, y: 0 }
    applyPatch(doc, [
      { op: "replace", path: "/x", value: 1 },
      { op: "replace", path: "/y", value: 2 },
    ])
    expect(doc.x).toBe(1)
    expect(doc.y).toBe(2)
  })
})
