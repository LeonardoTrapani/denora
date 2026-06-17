import { withAuthForwardingHeaders } from "./lib/request-auth-headers";
import { WebConfig } from "./lib/WebConfig.ts";

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

const authUrl = (path: string, search?: URLSearchParams) => {
  const url = new URL(path, WebConfig.requireApiUrl());
  if (search) {
    url.search = search.toString();
  }
  return url.toString();
};

const currentReturnTo = (fallback = "/app") =>
  typeof window === "undefined" ? fallback : `${window.location.origin}${fallback}`;

const getSession = async (): Promise<AuthClientResult<DenoraAuthSession>> => {
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
};

const signIn = (input: {
  readonly redirect?: string | undefined;
  readonly screenHint?: "sign-in" | "sign-up";
}) => {
  const search = new URLSearchParams();
  search.set("redirect", input.redirect ?? currentReturnTo());
  if (input.screenHint) search.set("screen_hint", input.screenHint);
  window.location.assign(authUrl("/api/auth/login", search));
};

const signOut = (input: { readonly returnTo?: string | undefined } = {}) => {
  const search = new URLSearchParams();
  search.set("return_to", input.returnTo ?? currentReturnTo("/login"));
  window.location.assign(authUrl("/api/auth/logout", search));
};

export const getAuthClient = () => ({
  getSession,
  signIn,
  signOut,
});
