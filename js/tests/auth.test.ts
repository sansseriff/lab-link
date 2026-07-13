import { afterEach, describe, expect, test } from "bun:test"
import { AuthClient, AuthError, authErrorMessage } from "../src/auth"

const originalFetch = globalThis.fetch

afterEach(() => {
  globalThis.fetch = originalFetch
})

describe("AuthClient", () => {
  test("logs in with credentials included and the configured prefix", async () => {
    let request: { input: string; init?: RequestInit } | undefined
    globalThis.fetch = (async (input, init) => {
      request = { input: String(input), init }
      return Response.json({ authorized: true })
    }) as typeof fetch

    const client = new AuthClient({ baseUrl: "https://instrument.test", prefix: "/lab" })
    await expect(client.login("TEST-PASS")).resolves.toEqual({ authorized: true })
    expect(request?.input).toBe("https://instrument.test/lab/auth/login")
    expect(request?.init?.credentials).toBe("include")
    expect(JSON.parse(String(request?.init?.body))).toEqual({ passphrase: "TEST-PASS" })
  })

  test("exchanges invitations without placing them in a query string", async () => {
    let body = ""
    globalThis.fetch = (async (_input, init) => {
      body = String(init?.body)
      return Response.json({ authorized: true })
    }) as typeof fetch

    await new AuthClient().exchangeInvite("one-time-secret")
    expect(JSON.parse(body)).toEqual({ invite: "one-time-secret" })
  })

  test("turns server errors into stable UI-neutral error codes", async () => {
    globalThis.fetch = (async () =>
      Response.json(
        { authorized: false, error: "invalid_or_expired_invite" },
        { status: 401 },
      )) as typeof fetch

    try {
      await new AuthClient().exchangeInvite("used")
      throw new Error("expected authentication to fail")
    } catch (error) {
      expect(error).toBeInstanceOf(AuthError)
      expect((error as AuthError).code).toBe("invalid_or_expired_invite")
      expect((error as AuthError).status).toBe(401)
    }
    expect(authErrorMessage("invalid_credentials")).toContain("passphrase")
  })
})
