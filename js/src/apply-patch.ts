/**
 * Minimal RFC 6902 JSON Patch applier.
 * Supports: add, remove, replace — the only ops the server generates.
 */

export interface PatchOperation {
  op: "add" | "remove" | "replace" | "move" | "copy" | "test"
  path: string
  value?: unknown
  from?: string
}

function parsePath(path: string): string[] {
  if (path === "" || path === "/") return []
  return path
    .split("/")
    .slice(1)
    .map((s) => s.replace(/~1/g, "/").replace(/~0/g, "~"))
}

function getParent(
  doc: unknown,
  parts: string[],
): [unknown, string] {
  let current: unknown = doc
  for (let i = 0; i < parts.length - 1; i++) {
    if (current == null || typeof current !== "object") {
      throw new Error(`Cannot traverse path: ${parts.slice(0, i + 1).join("/")}`)
    }
    current = (current as Record<string, unknown>)[parts[i]!]
  }
  return [current, parts[parts.length - 1]!]
}

export function applyPatch(doc: unknown, ops: PatchOperation[]): unknown {
  let result = doc
  for (const op of ops) {
    result = applyOp(result, op)
  }
  return result
}

function applyOp(doc: unknown, op: PatchOperation): unknown {
  const parts = parsePath(op.path)

  if (op.op === "add" || op.op === "replace") {
    if (parts.length === 0) return op.value
    const [parent, key] = getParent(doc, parts)
    if (Array.isArray(parent)) {
      if (key === "-") {
        parent.push(op.value)
      } else {
        const idx = Number(key)
        if (op.op === "add") parent.splice(idx, 0, op.value)
        else parent[idx] = op.value
      }
    } else if (parent != null && typeof parent === "object") {
      ;(parent as Record<string, unknown>)[key] = op.value
    }
    return doc
  }

  if (op.op === "remove") {
    if (parts.length === 0) return undefined
    const [parent, key] = getParent(doc, parts)
    if (Array.isArray(parent)) {
      parent.splice(Number(key), 1)
    } else if (parent != null && typeof parent === "object") {
      delete (parent as Record<string, unknown>)[key]
    }
    return doc
  }

  // Unsupported ops silently pass through
  return doc
}
