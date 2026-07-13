import { afterEach, describe, expect, test } from "bun:test";
import { AuthClient, AuthError, authErrorMessage } from "../src/auth";

const originalFetch = globalThis.fetch;

afterEach(() => {
  globalThis.fetch = originalFetch;
});

describe("AuthClient", () => {
  test("logs in with credentials included and the configured prefix", async () => {
    let request: { input: string; init?: RequestInit } | undefined;
    globalThis.fetch = (async (input, init) => {
      request = { input: String(input), init };
      return Response.json({ authorized: true });
    }) as typeof fetch;

    const client = new AuthClient({
      baseUrl: "https://instrument.test",
      prefix: "/lab",
    });
    await expect(client.login("TEST-PASS")).resolves.toEqual({
      authorized: true,
    });
    expect(request?.input).toBe("https://instrument.test/lab/auth/login");
    expect(request?.init?.credentials).toBe("include");
    expect(JSON.parse(String(request?.init?.body))).toEqual({
      passphrase: "TEST-PASS",
      remember: false,
    });
  });

  test("exchanges invitations without placing them in a query string", async () => {
    let body = "";
    globalThis.fetch = (async (_input, init) => {
      body = String(init?.body);
      return Response.json({ authorized: true });
    }) as typeof fetch;

    await new AuthClient().exchangeInvite("one-time-secret");
    expect(JSON.parse(body)).toEqual({
      invite: "one-time-secret",
      remember: false,
    });
  });

  test("turns server errors into stable UI-neutral error codes", async () => {
    globalThis.fetch = (async () =>
      Response.json(
        { authorized: false, error: "invalid_or_expired_invite" },
        { status: 401 },
      )) as typeof fetch;

    try {
      await new AuthClient().exchangeInvite("used");
      throw new Error("expected authentication to fail");
    } catch (error) {
      expect(error).toBeInstanceOf(AuthError);
      expect((error as AuthError).code).toBe("invalid_or_expired_invite");
      expect((error as AuthError).status).toBe(401);
    }
    expect(authErrorMessage("invalid_credentials")).toContain("passphrase");
  });

  test("supports first-run setup and remembered device labels", async () => {
    let body: Record<string, unknown> = {};
    globalThis.fetch = (async (_input, init) => {
      body = JSON.parse(String(init?.body));
      return Response.json({ authorized: true, configured: true });
    }) as typeof fetch;

    const result = await new AuthClient().setup("a-long-lab-password", {
      remember: true,
      deviceName: "Control-room Mac",
    });
    expect(result.authorized).toBe(true);
    expect(body).toEqual({
      passphrase: "a-long-lab-password",
      remember: true,
      deviceName: "Control-room Mac",
    });
  });

  test("creates scoped API tokens and returns the plaintext once", async () => {
    let url = "";
    let body: Record<string, unknown> = {};
    globalThis.fetch = (async (input, init) => {
      url = String(input);
      body = JSON.parse(String(init?.body));
      return Response.json({
        id: "token-1",
        token: "ll_secret",
        label: "cooldown monitor",
        createdAt: "2026-01-01T00:00:00Z",
        expiresAt: null,
        capabilities: ["read_state"],
      });
    }) as typeof fetch;

    const credential = await new AuthClient().createApiToken(
      "cooldown monitor",
      ["read_state"],
    );
    expect(url).toBe("/sync/auth/tokens");
    expect(body).toEqual({
      label: "cooldown monitor",
      capabilities: ["read_state"],
    });
    expect(credential.token).toBe("ll_secret");
  });
});
