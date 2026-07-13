export interface AuthPrincipal {
  id: string;
  kind: "local" | "session" | "api_token";
  label: string;
  capabilities: string[];
  sessionId?: string | null;
}

export interface SessionInfo {
  id: string;
  label: string;
  createdAt: string;
  lastUsedAt: string;
  expiresAt: string;
  remembered: boolean;
  authMethod: string;
  capabilities: string[];
}

export interface AuthStatus {
  configured: boolean;
  authorized: boolean;
  principal?: AuthPrincipal | null;
  session?: SessionInfo;
  error?: string;
}

export interface SessionOptions {
  remember?: boolean;
  deviceName?: string;
}

export interface AccessInvite {
  id: string;
  token: string;
  expiresAt: string;
  status: "active" | "consumed" | "expired" | "revoked";
}

export interface ApiTokenInfo {
  id: string;
  label: string;
  createdAt: string;
  lastUsedAt?: string | null;
  expiresAt?: string | null;
  capabilities: string[];
}

export interface ApiTokenCredential extends ApiTokenInfo {
  /** Shown only in the creation response. */
  token: string;
}

export interface AuthClientOptions {
  /** Origin or application base URL. Defaults to the current origin. */
  baseUrl?: string;
  /** LabSync route prefix. Defaults to `/sync`. */
  prefix?: string;
}

export class AuthError extends Error {
  readonly code: string;
  readonly status: number;

  constructor(code: string, status: number, message?: string) {
    super(message || authErrorMessage(code));
    this.name = "AuthError";
    this.code = code;
    this.status = status;
  }
}

/** Headless client for lab-link authentication and access management. */
export class AuthClient {
  private readonly baseUrl: string;
  private readonly prefix: string;

  constructor(options: AuthClientOptions = {}) {
    this.baseUrl = options.baseUrl ?? "";
    this.prefix = `/${(options.prefix ?? "/sync").replace(/^\/+|\/+$/g, "")}`;
  }

  status(): Promise<AuthStatus> {
    return this.request("status", { method: "GET" });
  }

  setup(passphrase: string, options: SessionOptions = {}): Promise<AuthStatus> {
    return this.request("setup", {
      method: "POST",
      body: JSON.stringify({ passphrase, ...sessionPayload(options) }),
    });
  }

  login(passphrase: string, options: SessionOptions = {}): Promise<AuthStatus> {
    return this.request("login", {
      method: "POST",
      body: JSON.stringify({ passphrase, ...sessionPayload(options) }),
    });
  }

  exchangeInvite(
    invite: string,
    options: SessionOptions = {},
  ): Promise<AuthStatus> {
    return this.request("invite", {
      method: "POST",
      body: JSON.stringify({ invite, ...sessionPayload(options) }),
    });
  }

  logout(): Promise<AuthStatus> {
    return this.request("logout", { method: "POST" });
  }

  changePassphrase(
    passphrase: string,
    revokeSessions = true,
    revokeInvites = true,
  ): Promise<{ changed: boolean }> {
    return this.request("passphrase", {
      method: "POST",
      body: JSON.stringify({ passphrase, revokeSessions, revokeInvites }),
    });
  }

  async sessions(): Promise<SessionInfo[]> {
    return (
      await this.request<{ sessions: SessionInfo[] }>("sessions", {
        method: "GET",
      })
    ).sessions;
  }

  revokeSession(id: string): Promise<{ revoked: boolean }> {
    return this.request("sessions/revoke", {
      method: "POST",
      body: JSON.stringify({ id }),
    });
  }

  revokeAllSessions(): Promise<{ revoked: boolean }> {
    return this.request("sessions/revoke-all", { method: "POST" });
  }

  createInvite(ttl?: number): Promise<AccessInvite> {
    return this.request("invites", {
      method: "POST",
      body: JSON.stringify(ttl == null ? {} : { ttl }),
    });
  }

  revokeInvite(id: string): Promise<{ revoked: boolean }> {
    return this.request("invites/revoke", {
      method: "POST",
      body: JSON.stringify({ id }),
    });
  }

  async apiTokens(): Promise<ApiTokenInfo[]> {
    return (
      await this.request<{ tokens: ApiTokenInfo[] }>("tokens", {
        method: "GET",
      })
    ).tokens;
  }

  createApiToken(
    label: string,
    capabilities: string[],
    ttl?: number,
  ): Promise<ApiTokenCredential> {
    return this.request("tokens", {
      method: "POST",
      body: JSON.stringify({
        label,
        capabilities,
        ...(ttl == null ? {} : { ttl }),
      }),
    });
  }

  revokeApiToken(id: string): Promise<{ revoked: boolean }> {
    return this.request("tokens/revoke", {
      method: "POST",
      body: JSON.stringify({ id }),
    });
  }

  revokeAllApiTokens(): Promise<{ revoked: boolean }> {
    return this.request("tokens/revoke-all", { method: "POST" });
  }

  /**
   * Exchange an invite stored in `#invite=…`, then scrub it from browser
   * history. Returns false when the current URL contains no invitation.
   */
  async consumeInviteFragment(
    parameter = "invite",
    options: SessionOptions = {},
  ): Promise<boolean> {
    if (typeof window === "undefined") return false;
    const values = new URLSearchParams(window.location.hash.slice(1));
    const invite = values.get(parameter);
    if (!invite) return false;

    values.delete(parameter);
    const remaining = values.toString();
    const cleanUrl = `${window.location.pathname}${window.location.search}${remaining ? `#${remaining}` : ""}`;
    window.history.replaceState(null, "", cleanUrl);
    await this.exchangeInvite(invite, options);
    return true;
  }

  private async request<T>(path: string, init: RequestInit): Promise<T> {
    const response = await fetch(`${this.baseUrl}${this.prefix}/auth/${path}`, {
      ...init,
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        ...init.headers,
      },
    });
    const payload = (await response.json().catch(() => ({}))) as T & {
      error?: string;
      message?: string;
    };
    if (!response.ok) {
      throw new AuthError(
        payload.error ?? "authentication_failed",
        response.status,
        payload.message,
      );
    }
    return payload;
  }
}

function sessionPayload(options: SessionOptions): Record<string, unknown> {
  return {
    remember: options.remember ?? false,
    ...(options.deviceName ? { deviceName: options.deviceName } : {}),
  };
}

export function authErrorMessage(code: string): string {
  switch (code) {
    case "invalid_credentials":
      return "That passphrase was not accepted.";
    case "invalid_or_expired_invite":
      return "This access link has expired or has already been used.";
    case "rate_limited":
      return "Too many attempts. Wait a minute and try again.";
    case "origin_not_allowed":
      return "This page is not allowed to authenticate with the server.";
    case "setup_required":
      return "Remote access must be configured on the instrument first.";
    case "local_setup_required":
      return "Initial setup must be completed on the instrument computer.";
    case "weak_passphrase":
      return "Choose a passphrase containing at least 12 characters.";
    case "forbidden":
      return "This device is not permitted to manage remote access.";
    default:
      return "Authentication failed.";
  }
}
