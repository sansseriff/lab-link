export interface AuthStatus {
  authorized: boolean
  error?: string
}

export interface AuthClientOptions {
  /** Origin or application base URL. Defaults to the current origin. */
  baseUrl?: string
  /** LabSync route prefix. Defaults to `/sync`. */
  prefix?: string
}

export class AuthError extends Error {
  readonly code: string
  readonly status: number

  constructor(code: string, status: number) {
    super(authErrorMessage(code))
    this.name = "AuthError"
    this.code = code
    this.status = status
  }
}

/** Headless client for lab-link's passphrase and invitation endpoints. */
export class AuthClient {
  private readonly baseUrl: string
  private readonly prefix: string

  constructor(options: AuthClientOptions = {}) {
    this.baseUrl = options.baseUrl ?? ""
    this.prefix = `/${(options.prefix ?? "/sync").replace(/^\/+|\/+$/g, "")}`
  }

  status(): Promise<AuthStatus> {
    return this.request("status", { method: "GET" })
  }

  login(passphrase: string): Promise<AuthStatus> {
    return this.request("login", {
      method: "POST",
      body: JSON.stringify({ passphrase }),
    })
  }

  exchangeInvite(invite: string): Promise<AuthStatus> {
    return this.request("invite", {
      method: "POST",
      body: JSON.stringify({ invite }),
    })
  }

  logout(): Promise<AuthStatus> {
    return this.request("logout", { method: "POST" })
  }

  /**
   * Exchange an invite stored in `#invite=…`, then scrub it from browser
   * history. Returns false when the current URL contains no invitation.
   */
  async consumeInviteFragment(parameter = "invite"): Promise<boolean> {
    if (typeof window === "undefined") return false
    const values = new URLSearchParams(window.location.hash.slice(1))
    const invite = values.get(parameter)
    if (!invite) return false

    values.delete(parameter)
    const remaining = values.toString()
    const cleanUrl = `${window.location.pathname}${window.location.search}${remaining ? `#${remaining}` : ""}`
    window.history.replaceState(null, "", cleanUrl)
    await this.exchangeInvite(invite)
    return true
  }

  private async request(path: string, init: RequestInit): Promise<AuthStatus> {
    const response = await fetch(`${this.baseUrl}${this.prefix}/auth/${path}`, {
      ...init,
      credentials: "include",
      headers: {
        "Content-Type": "application/json",
        ...init.headers,
      },
    })
    const payload = (await response.json().catch(() => ({}))) as Partial<AuthStatus>
    if (!response.ok) {
      throw new AuthError(payload.error ?? "authentication_failed", response.status)
    }
    return { authorized: Boolean(payload.authorized) }
  }
}

export function authErrorMessage(code: string): string {
  switch (code) {
    case "invalid_credentials":
      return "That passphrase was not accepted."
    case "invalid_or_expired_invite":
      return "This access link has expired or has already been used."
    case "rate_limited":
      return "Too many attempts. Wait a minute and try again."
    case "origin_not_allowed":
      return "This page is not allowed to authenticate with the server."
    default:
      return "Authentication failed."
  }
}
