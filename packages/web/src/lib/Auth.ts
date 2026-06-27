import { createIsomorphicFn, createServerFn } from "@tanstack/react-start";
import { createServerOnlyFn } from "@tanstack/react-start";
import { getRequestHeader } from "@tanstack/react-start/server";

import { WebConfig } from "./WebConfig.ts";

export type DenoraAuthUser = {
  readonly id: string;
  readonly email: string;
  readonly emailVerified: boolean;
  readonly name: string | null;
  readonly image: string | null;
  readonly createdAt: string;
  readonly updatedAt: string;
};

export type DenoraAuthSession = {
  readonly session: {
    readonly id: string;
    readonly organizationId: string | null;
    readonly userId: string;
  };
  readonly user: DenoraAuthUser;
};

export type AuthClientResult<T> =
  | { readonly data: T; readonly error: null }
  | { readonly data: null; readonly error: { readonly message: string; readonly status?: number } };

const forwardedHeaderNames = ["authorization", "cookie", "x-denora-dev-user-id"] as const;

type ServerAuthHeadersGetter = () => Headers;

export const getServerAuthHeaders = createServerOnlyFn(() => {
  const headers = new Headers();

  for (const name of forwardedHeaderNames) {
    const value = getRequestHeader(name);
    if (value) {
      headers.set(name, value);
    }
  }

  return headers;
});

const getRequestAuthHeaders = createIsomorphicFn()
  .client(() => new Headers())
  .server(getServerAuthHeaders);

export async function withAuthForwardingHeaders(
  headers?: HeadersInit,
  options?: {
    readonly getServerAuthHeaders?: ServerAuthHeadersGetter;
  },
) {
  const mergedHeaders = new Headers(headers);
  const authHeaders = options?.getServerAuthHeaders?.() ?? getRequestAuthHeaders();

  authHeaders.forEach((value, key) => {
    if (!mergedHeaders.has(key)) {
      mergedHeaders.set(key, value);
    }
  });

  return mergedHeaders;
}

export function authUrl(path: string, search?: URLSearchParams): string {
  const url = new URL(path, WebConfig.requireApiUrl());
  if (search) {
    url.search = search.toString();
  }
  return url.toString();
}

export function currentReturnTo(fallback = "/app"): string {
  return typeof window === "undefined" ? fallback : `${window.location.origin}${fallback}`;
}

export function loginHref(
  input: {
    readonly redirect?: string | undefined;
    readonly screenHint?: "sign-in" | "sign-up" | undefined;
  } = {},
): string {
  const search = new URLSearchParams();
  search.set("redirect", input.redirect ?? currentReturnTo());
  if (input.screenHint) search.set("screen_hint", input.screenHint);
  return authUrl("/api/auth/login", search);
}

export function logoutHref(input: { readonly returnTo?: string | undefined } = {}): string {
  const search = new URLSearchParams();
  search.set("return_to", input.returnTo ?? currentReturnTo("/login"));
  return authUrl("/api/auth/logout", search);
}

export function signIn(
  input: {
    readonly redirect?: string | undefined;
    readonly screenHint?: "sign-in" | "sign-up" | undefined;
  } = {},
): void {
  window.location.assign(loginHref(input));
}

export function signOut(input: { readonly returnTo?: string | undefined } = {}): void {
  window.location.assign(logoutHref(input));
}

export async function fetchSession(): Promise<AuthClientResult<DenoraAuthSession>> {
  const response = await fetch(authUrl("/api/auth/session"), {
    credentials: "include",
    headers: await withAuthForwardingHeaders(),
  });

  if (response.status === 401) {
    return { data: null, error: { message: "Authentication required", status: 401 } };
  }

  if (!response.ok) {
    return {
      data: null,
      error: { message: "Failed to load auth session", status: response.status },
    };
  }

  return { data: (await response.json()) as DenoraAuthSession, error: null };
}

export const getSession = createServerFn({ method: "GET" }).handler(
  async (): Promise<DenoraAuthSession | null> => {
    const result = await fetchSession();

    if (result.error) {
      const status = result.error.status;

      if (status === 401) {
        return null;
      }

      throw new Error(`Failed to load session from server${status ? `: ${status}` : ""}`);
    }

    return result.data;
  },
);

export * as Auth from "./Auth.ts";
