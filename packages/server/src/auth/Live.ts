import {
  WorkOS,
  type AuthenticateWithSessionCookieSuccessResponse,
  type User,
} from "@workos-inc/node/worker";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as Result from "effect/Result";
import * as Url from "effect/unstable/http/Url";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Auth, AuthProviderError } from "./Auth.ts";
import { DenoraUser, Unauthorized } from "./User.ts";

const authBasePath = "/api/auth";
const callbackPath = `${authBasePath}/callback`;
const loginPath = `${authBasePath}/login`;
const logoutPath = `${authBasePath}/logout`;
const sessionPath = `${authBasePath}/session`;

const sessionCookieName = "denora_session";
const transactionCookieName = "denora_auth_transaction";
const sessionMaxAgeSeconds = 60 * 60 * 24 * 30;
const transactionMaxAgeSeconds = 60 * 10;

type AuthOptions = ServerConfig.Auth;

type AuthTransaction = {
  readonly codeVerifier: string;
  readonly returnTo: string;
  readonly state: string;
};

type AuthenticatedSession = {
  readonly organizationId: string | null;
  readonly sealedSession?: string | undefined;
  readonly sessionId: string;
  readonly user: DenoraUser;
};

type SessionState =
  | { readonly _tag: "Missing" }
  | { readonly _tag: "Unauthenticated" }
  | ({ readonly _tag: "Authenticated" } & AuthenticatedSession);

const encoder = new TextEncoder();

const toBase64Url = (bytes: Uint8Array) =>
  btoa(String.fromCharCode(...bytes))
    .replaceAll("+", "-")
    .replaceAll("/", "_")
    .replaceAll("=", "");

const fromBase64Url = (value: string) => {
  const base64 = value.replaceAll("-", "+").replaceAll("_", "/");
  const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
  return Uint8Array.from(atob(padded), (character) => character.charCodeAt(0));
};

const constantTimeEqual = (left: string, right: string) => {
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  let diff = leftBytes.length ^ rightBytes.length;
  const length = Math.max(leftBytes.length, rightBytes.length);

  for (let index = 0; index < length; index += 1) {
    diff |= (leftBytes[index] ?? 0) ^ (rightBytes[index] ?? 0);
  }

  return diff === 0;
};

const sign = async (payload: string, secret: string) => {
  const key = await crypto.subtle.importKey(
    "raw",
    encoder.encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const signature = await crypto.subtle.sign("HMAC", key, encoder.encode(payload));
  return toBase64Url(new Uint8Array(signature));
};

const sealTransaction = async (transaction: AuthTransaction, secret: string) => {
  const payload = toBase64Url(encoder.encode(JSON.stringify(transaction)));
  return `${payload}.${await sign(payload, secret)}`;
};

const unsealTransaction = async (
  value: string | undefined,
  secret: string,
): Promise<AuthTransaction | undefined> => {
  if (value === undefined) return undefined;

  const [payload, signature] = value.split(".");
  if (!payload || !signature) return undefined;

  const expectedSignature = await sign(payload, secret);
  if (!constantTimeEqual(signature, expectedSignature)) return undefined;

  try {
    const parsed = JSON.parse(
      new TextDecoder().decode(fromBase64Url(payload)),
    ) as Partial<AuthTransaction>;
    if (
      typeof parsed.codeVerifier !== "string" ||
      typeof parsed.returnTo !== "string" ||
      typeof parsed.state !== "string"
    ) {
      return undefined;
    }

    return {
      codeVerifier: parsed.codeVerifier,
      returnTo: parsed.returnTo,
      state: parsed.state,
    };
  } catch {
    return undefined;
  }
};

const parseCookies = (header: string | null) => {
  const cookies = new Map<string, string>();
  if (header === null) return cookies;

  for (const part of header.split(";")) {
    const [rawName, ...rawValue] = part.trim().split("=");
    if (!rawName || rawValue.length === 0) continue;
    cookies.set(rawName, decodeURIComponent(rawValue.join("=")));
  }

  return cookies;
};

const cookie = (
  name: string,
  value: string,
  options: {
    readonly baseURL: string;
    readonly cookieDomain?: string | undefined;
    readonly maxAgeSeconds?: number | undefined;
    readonly path?: string | undefined;
  },
) => {
  const parts = [
    `${name}=${encodeURIComponent(value)}`,
    `Path=${options.path ?? "/"}`,
    "HttpOnly",
    "SameSite=Lax",
  ];

  if (new URL(options.baseURL).protocol === "https:") {
    parts.push("Secure");
  }

  if (options.cookieDomain !== undefined) {
    parts.push(`Domain=${options.cookieDomain}`);
  }

  if (options.maxAgeSeconds !== undefined) {
    parts.push(`Max-Age=${options.maxAgeSeconds}`);
  }

  return parts.join("; ");
};

const clearCookie = (
  name: string,
  options: {
    readonly baseURL: string;
    readonly cookieDomain?: string | undefined;
    readonly path?: string;
  },
) => `${cookie(name, "", { ...options, maxAgeSeconds: 0 })}; Expires=Thu, 01 Jan 1970 00:00:00 GMT`;

const appendCookies = (headers: Headers, cookies: ReadonlyArray<string>) => {
  for (const value of cookies) {
    headers.append("Set-Cookie", value);
  }
};

const redirect = (location: string, cookies: ReadonlyArray<string> = []) => {
  const headers = new Headers({ Location: location });
  appendCookies(headers, cookies);
  return new Response(null, { headers, status: 302 });
};

const json = (body: unknown, init: ResponseInit = {}, cookies: ReadonlyArray<string> = []) => {
  const headers = new Headers();
  headers.set("Content-Type", "application/json");
  appendCookies(headers, cookies);
  return new Response(JSON.stringify(body), { ...init, headers });
};

const toDenoraUser = (user: User): DenoraUser =>
  new DenoraUser({
    id: user.id,
    email: user.email,
    emailVerified: user.emailVerified,
    name: user.name,
    image: user.profilePictureUrl,
    createdAt: user.createdAt,
    updatedAt: user.updatedAt,
  });

const toSessionBody = (session: AuthenticatedSession) => ({
  session: {
    id: session.sessionId,
    organizationId: session.organizationId,
    userId: session.user.id,
  },
  user: session.user,
});

const safePath = (value: string) => value.startsWith("/") && !value.startsWith("//");

const originFromString = (value: string): string | undefined => {
  const url = Url.fromString(value);
  return Result.isSuccess(url) ? url.success.origin : undefined;
};

const parseRequestUrl = (request: Request) =>
  Effect.fromResult(Url.fromString(request.url)).pipe(
    Effect.mapError((cause) => new AuthProviderError({ operation: "parseRequestUrl", cause })),
  );

const trustedOriginFromRequest = (
  request: Request,
  options: Pick<AuthOptions, "baseURL" | "webOrigins">,
) => {
  const candidates = [request.headers.get("origin"), request.headers.get("referer")];

  for (const candidate of candidates) {
    if (candidate === null) continue;
    const origin = originFromString(candidate);
    if (origin !== undefined && options.webOrigins.includes(origin)) return origin;
  }

  return (
    options.webOrigins[0] ??
    originFromString(request.url) ??
    originFromString(options.baseURL) ??
    options.baseURL
  );
};

const resolveReturnTo = (
  request: Request,
  rawValue: string | null,
  options: Pick<AuthOptions, "baseURL" | "webOrigins">,
  fallbackPath: string,
) => {
  const fallbackOrigin = trustedOriginFromRequest(request, options);
  const fallback = new URL(fallbackPath, fallbackOrigin).toString();

  if (rawValue === null || rawValue.length === 0) return fallback;

  if (safePath(rawValue)) return new URL(rawValue, fallbackOrigin).toString();

  try {
    const url = new URL(rawValue);
    const allowedOrigins = new Set([...options.webOrigins, new URL(options.baseURL).origin]);
    return allowedOrigins.has(url.origin) ? url.toString() : fallback;
  } catch {
    return fallback;
  }
};

const sessionCookie = (sealedSession: string, options: AuthOptions) =>
  cookie(sessionCookieName, sealedSession, {
    baseURL: options.baseURL,
    cookieDomain: options.cookieDomain,
    maxAgeSeconds: sessionMaxAgeSeconds,
  });

const clearSessionCookie = (options: AuthOptions) =>
  clearCookie(sessionCookieName, { baseURL: options.baseURL, cookieDomain: options.cookieDomain });

const transactionCookie = (sealedTransaction: string, options: AuthOptions) =>
  cookie(transactionCookieName, sealedTransaction, {
    baseURL: options.baseURL,
    cookieDomain: options.cookieDomain,
    maxAgeSeconds: transactionMaxAgeSeconds,
    path: callbackPath,
  });

const clearTransactionCookie = (options: AuthOptions) =>
  clearCookie(transactionCookieName, {
    baseURL: options.baseURL,
    cookieDomain: options.cookieDomain,
    path: callbackPath,
  });

const screenHint = (url: URL) => {
  const value = url.searchParams.get("screen_hint");
  return value === "sign-up" || value === "sign-in" ? value : undefined;
};

const makeWorkOS = (options: AuthOptions) =>
  new WorkOS(Redacted.value(options.apiKey), { clientId: options.clientId });

const logoutUrlFromSession = async (
  workos: WorkOS,
  sealedSession: string,
  cookiePassword: string,
  returnTo: string,
) => {
  const session = workos.userManagement.loadSealedSession({
    sessionData: sealedSession,
    cookiePassword,
  });

  const authenticated = await session.authenticate();
  if (authenticated.authenticated) {
    return workos.userManagement.getLogoutUrl({ sessionId: authenticated.sessionId, returnTo });
  }

  const refreshed = await session.refresh({ cookiePassword });
  if (refreshed.authenticated) {
    return workos.userManagement.getLogoutUrl({ sessionId: refreshed.sessionId, returnTo });
  }

  return returnTo;
};

export const layer = (options: AuthOptions): Layer.Layer<Auth.Service> =>
  Layer.effect(
    Auth.Service,
    Effect.sync(() => {
      const workos = makeWorkOS(options);
      const cookiePassword = Redacted.value(options.cookiePassword);

      const loadSession = Effect.fn("Auth.loadSession")(function* (request: Request) {
        const sealedSession = parseCookies(request.headers.get("cookie")).get(sessionCookieName);
        if (sealedSession === undefined) {
          return { _tag: "Missing" } satisfies SessionState;
        }

        return yield* Effect.tryPromise({
          try: async (): Promise<SessionState> => {
            const session = workos.userManagement.loadSealedSession({
              sessionData: sealedSession,
              cookiePassword,
            });

            const authenticated = await session.authenticate();
            if (authenticated.authenticated) {
              return toAuthenticatedSession(authenticated);
            }

            const refreshed = await session.refresh({ cookiePassword });
            if (refreshed.authenticated) {
              return toAuthenticatedSession(refreshed);
            }

            return { _tag: "Unauthenticated" };
          },
          catch: (cause) => new AuthProviderError({ operation: "loadSession", cause }),
        });
      });

      const handleLogin = Effect.fn("Auth.handleLogin")(function* (request: Request, url: URL) {
        const returnTo = resolveReturnTo(
          request,
          url.searchParams.get("redirect"),
          options,
          "/app",
        );
        const redirectUri = new URL(callbackPath, options.baseURL).toString();

        const hint = screenHint(url);
        const result = yield* Effect.tryPromise({
          try: () =>
            workos.userManagement.getAuthorizationUrlWithPKCE({
              clientId: options.clientId,
              provider: "authkit",
              redirectUri,
              ...(hint === undefined ? {} : { screenHint: hint }),
            }),
          catch: (cause) => new AuthProviderError({ operation: "getAuthorizationUrl", cause }),
        });

        const sealedTransaction = yield* Effect.tryPromise({
          try: () =>
            sealTransaction(
              { codeVerifier: result.codeVerifier, returnTo, state: result.state },
              cookiePassword,
            ),
          catch: (cause) => new AuthProviderError({ operation: "sealTransaction", cause }),
        });

        return redirect(result.url, [transactionCookie(sealedTransaction, options)]);
      });

      const handleCallback = Effect.fn("Auth.handleCallback")(function* (
        request: Request,
        url: URL,
      ) {
        const transaction = yield* Effect.tryPromise({
          try: () =>
            unsealTransaction(
              parseCookies(request.headers.get("cookie")).get(transactionCookieName),
              cookiePassword,
            ),
          catch: (cause) => new AuthProviderError({ operation: "unsealTransaction", cause }),
        });

        const callbackCookies = [clearTransactionCookie(options)];

        if (transaction === undefined || transaction.state !== url.searchParams.get("state")) {
          return json({ error: "InvalidAuthState" }, { status: 400 }, callbackCookies);
        }

        const error = url.searchParams.get("error");
        if (error !== null) {
          return redirect(
            new URL(`/login?error=${encodeURIComponent(error)}`, transaction.returnTo).toString(),
            callbackCookies,
          );
        }

        const code = url.searchParams.get("code");
        if (code === null) {
          return json({ error: "MissingAuthCode" }, { status: 400 }, callbackCookies);
        }

        const response = yield* Effect.tryPromise({
          try: () =>
            workos.userManagement.authenticateWithCode({
              clientId: options.clientId,
              code,
              codeVerifier: transaction.codeVerifier,
              session: {
                cookiePassword,
                sealSession: true,
              },
            }),
          catch: (cause) => new AuthProviderError({ operation: "authenticateWithCode", cause }),
        });

        if (response.sealedSession === undefined) {
          return yield* new AuthProviderError({
            operation: "authenticateWithCode",
            cause: new Error("WorkOS did not return a sealed session"),
          });
        }

        return redirect(transaction.returnTo, [
          sessionCookie(response.sealedSession, options),
          ...callbackCookies,
        ]);
      });

      const handleSession = Effect.fn("Auth.handleSession")(function* (request: Request) {
        const state = yield* loadSession(request);
        if (state._tag !== "Authenticated") {
          const cookies = state._tag === "Unauthenticated" ? [clearSessionCookie(options)] : [];
          return json({ session: null, user: null }, { status: 401 }, cookies);
        }

        return json(
          toSessionBody(state),
          { status: 200 },
          state.sealedSession === undefined ? [] : [sessionCookie(state.sealedSession, options)],
        );
      });

      const handleLogout = Effect.fn("Auth.handleLogout")(function* (request: Request, url: URL) {
        const returnTo = resolveReturnTo(
          request,
          url.searchParams.get("return_to") ?? url.searchParams.get("redirect"),
          options,
          "/login",
        );
        const sealedSession = parseCookies(request.headers.get("cookie")).get(sessionCookieName);
        const logoutUrl = yield* Effect.tryPromise({
          try: async () => {
            if (sealedSession === undefined) return returnTo;
            return await logoutUrlFromSession(workos, sealedSession, cookiePassword, returnTo);
          },
          catch: (cause) => new AuthProviderError({ operation: "logout", cause }),
        });

        return redirect(logoutUrl, [clearSessionCookie(options), clearTransactionCookie(options)]);
      });

      const handle = Effect.fn("Auth.handle")(function* (request: Request) {
        const url = yield* parseRequestUrl(request);

        if (request.method === "OPTIONS") return new Response(null, { status: 204 });
        if (request.method === "GET" && url.pathname === loginPath)
          return yield* handleLogin(request, url);
        if (request.method === "GET" && url.pathname === callbackPath) {
          return yield* handleCallback(request, url);
        }
        if (request.method === "GET" && url.pathname === sessionPath)
          return yield* handleSession(request);
        if (
          (request.method === "GET" || request.method === "POST") &&
          url.pathname === logoutPath
        ) {
          return yield* handleLogout(request, url);
        }

        return json({ error: "NotFound" }, { status: 404 });
      });

      const getSession = Effect.fn("Auth.getSession")(function* (request: Request) {
        const state = yield* loadSession(request);
        return state._tag === "Authenticated" ? Option.some(state.user) : Option.none<DenoraUser>();
      });

      const requireSession = Effect.fn("Auth.requireSession")(function* (request: Request) {
        const session = yield* getSession(request);
        if (Option.isNone(session)) {
          return yield* new Unauthorized({ message: "Authentication required" });
        }
        return session.value;
      });

      return Auth.Service.of({ handle, getSession, requireSession });
    }),
  );

const toAuthenticatedSession = (
  session: Omit<AuthenticateWithSessionCookieSuccessResponse, "accessToken"> & {
    readonly sealedSession?: string;
  },
): SessionState => ({
  _tag: "Authenticated",
  organizationId: session.organizationId ?? null,
  sealedSession: session.sealedSession,
  sessionId: session.sessionId,
  user: toDenoraUser(session.user),
});

export const layerFromConfig: Layer.Layer<Auth.Service, never, ServerConfig.Service> = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.Service;
    return layer(config.auth);
  }),
);

export * as AuthLive from "./Live.ts";
