import { Buffer } from "node:buffer";
import { createHmac, randomBytes, timingSafeEqual } from "node:crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as UrlParams from "effect/unstable/http/UrlParams";
import { Authorization } from "../../auth/Authorization.ts";
import { WorkOsAuth } from "../../auth/WorkOsAuth.ts";
import { DefaultWebOrigins, ServerConfig } from "../../config/ServerConfig.ts";

export const CsrfTokenTtlMs = 10 * 60 * 1000;
const CsrfHeaderName = "x-csrf-token";
const CsrfFormFieldName = "csrfToken";

export const firstSearchParam = (value: string | ReadonlyArray<string> | undefined) =>
  Array.isArray(value) ? (value[0] ?? null) : (value ?? null);

export const isAllowedAppReturnTo = (options: ServerConfig.Auth, candidate: string) => {
  if (!URL.canParse(candidate)) return false;

  const url = new URL(candidate);
  return options.appRedirectSchemes.some((scheme) => url.protocol === `${scheme}:`);
};

export const redirectToAllowedReturnTo = (options: ServerConfig.Auth, candidate: string | null) => {
  const fallback = options.webOrigins[0] ?? DefaultWebOrigins[0];
  if (!candidate) return fallback;

  if (URL.canParse(candidate)) {
    const url = new URL(candidate);
    if (options.webOrigins.includes(url.origin)) return url.toString();
    if (isAllowedAppReturnTo(options, candidate)) return url.toString();
    return fallback;
  }

  if (candidate.startsWith("/") && !candidate.startsWith("//")) {
    return new URL(candidate, fallback).toString();
  }

  return fallback;
};

export const withAuthResult = (
  options: ServerConfig.Auth,
  destination: string,
  result: Record<string, string>,
) => {
  if (!URL.canParse(destination)) return destination;

  const url = new URL(destination);
  if (isAllowedAppReturnTo(options, destination)) {
    const params = new URLSearchParams(url.hash.startsWith("#") ? url.hash.slice(1) : "");
    for (const [key, value] of Object.entries(result)) {
      params.set(key, value);
    }
    url.hash = params.toString();
    return url.toString();
  }

  for (const [key, value] of Object.entries(result)) {
    url.searchParams.set(key, value);
  }
  return url.toString();
};

export const withAuthError = (options: ServerConfig.Auth, destination: string, code: string) =>
  withAuthResult(options, destination, { authError: code });

export const withMobileSession = (
  options: ServerConfig.Auth,
  destination: string,
  sealedSession: string | undefined,
) => {
  if (!sealedSession || !isAllowedAppReturnTo(options, destination)) return destination;
  return withAuthResult(options, destination, {
    authStatus: "signed_in",
    session: sealedSession,
  });
};

const redirectWithAuthError = (options: ServerConfig.Auth, destination: string, code: string) =>
  Effect.succeed(HttpServerResponse.redirect(withAuthError(options, destination, code)));

const parseFormBody = (request: HttpServerRequest.HttpServerRequest) =>
  request.urlParamsBody.pipe(Effect.catch(() => Effect.succeed(UrlParams.empty)));

const firstUrlParam = (params: UrlParams.UrlParams, key: string) =>
  Option.getOrNull(UrlParams.getFirst(params, key));

const getCsrfSigningPayload = (
  issuedAt: string,
  nonce: string,
  sessionCookie: string | undefined,
) => `${issuedAt}.${nonce}.${sessionCookie ?? ""}`;

export const signCsrfToken = (
  secret: Redacted.Redacted<string>,
  issuedAt: string,
  nonce: string,
  sessionCookie: string | undefined,
) =>
  createHmac("sha256", Redacted.value(secret))
    .update(getCsrfSigningPayload(issuedAt, nonce, sessionCookie))
    .digest("base64url");

export const createCsrfToken = (options: ServerConfig.Auth, sessionCookie: string | undefined) => {
  const issuedAt = Date.now().toString(36);
  const nonce = randomBytes(16).toString("base64url");
  const signature = signCsrfToken(options.csrfSecret, issuedAt, nonce, sessionCookie);
  return `${issuedAt}.${nonce}.${signature}`;
};

const isEqualSignature = (actual: string, expected: string) => {
  const actualBytes = Buffer.from(actual, "base64url");
  const expectedBytes = Buffer.from(expected, "base64url");
  return (
    actualBytes.byteLength === expectedBytes.byteLength &&
    timingSafeEqual(actualBytes, expectedBytes)
  );
};

export const isValidCsrfToken = (
  options: ServerConfig.Auth,
  token: string | null,
  sessionCookie: string | undefined,
) => {
  if (!token) return false;

  const [issuedAt, nonce, signature, extra] = token.split(".");
  if (!issuedAt || !nonce || !signature || extra !== undefined) return false;

  const issuedAtMs = Number.parseInt(issuedAt, 36);
  if (!Number.isFinite(issuedAtMs)) return false;

  const age = Date.now() - issuedAtMs;
  if (age < 0 || age > CsrfTokenTtlMs) return false;

  try {
    return isEqualSignature(
      signature,
      signCsrfToken(options.csrfSecret, issuedAt, nonce, sessionCookie),
    );
  } catch {
    return false;
  }
};

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* WorkOsAuth.Service;
    const config = yield* ServerConfig.Service;
    const options = config.auth;

    yield* router.add("GET", "/auth/login", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const destination = redirectToAllowedReturnTo(options, firstSearchParam(params.returnTo));

        if (!URL.canParse(request.url)) {
          return HttpServerResponse.redirect(withAuthError(options, destination, "login_failed"));
        }

        return yield* Effect.gen(function* () {
          const requestUrl = new URL(request.url);
          const authorizationUrl = yield* auth.getAuthorizationUrl({
            redirectUri: new URL("/auth/callback", requestUrl.origin).toString(),
            returnTo: destination,
          });

          return HttpServerResponse.redirect(authorizationUrl);
        }).pipe(
          Effect.catchTag("WorkOsAuthError", () =>
            redirectWithAuthError(options, destination, "login_failed"),
          ),
        );
      }),
    );

    yield* router.add("GET", "/auth/callback", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const destination = redirectToAllowedReturnTo(options, firstSearchParam(params.state));
        const callbackCode = firstSearchParam(params.code);

        if (!callbackCode) {
          return HttpServerResponse.redirect(
            withAuthError(options, destination, "callback_missing_code"),
          );
        }

        return yield* Effect.gen(function* () {
          const forwardedFor = request.headers["x-forwarded-for"]?.split(",")[0]?.trim();
          const ipAddress =
            request.headers["cf-connecting-ip"] ??
            (forwardedFor && forwardedFor.length > 0 ? forwardedFor : undefined) ??
            Option.getOrUndefined(request.remoteAddress);
          const userAgent = request.headers["user-agent"];
          const metadata: WorkOsAuth.AuthRequestMetadata = {
            ...(ipAddress ? { ipAddress } : {}),
            ...(userAgent ? { userAgent } : {}),
          };

          const session = yield* auth.authenticateWithCode({
            code: callbackCode,
            metadata,
          });

          const response = HttpServerResponse.redirect(
            withMobileSession(options, destination, session.sealedSession),
          );
          if (!session.sealedSession) return response;
          return yield* Authorization.setSessionCookie(
            response,
            session.sealedSession,
            options,
          ).pipe(
            Effect.catchTag("CookieError", () =>
              Effect.succeed(
                HttpServerResponse.redirect(
                  withAuthError(options, destination, "session_cookie_failed"),
                ),
              ),
            ),
          );
        }).pipe(
          Effect.catchTags({
            WorkOsAuthError: () => redirectWithAuthError(options, destination, "callback_failed"),
            UserSyncError: () => redirectWithAuthError(options, destination, "user_sync_failed"),
          }),
        );
      }),
    );

    yield* router.add("GET", "/auth/csrf-token", (request) => {
      const csrfToken = createCsrfToken(options, request.cookies[Authorization.SessionCookieName]);
      return Effect.succeed(
        HttpServerResponse.text(JSON.stringify({ csrfToken }), {
          contentType: "application/json",
          headers: {
            "cache-control": "no-store",
          },
        }),
      );
    });

    yield* router.add("POST", "/auth/logout", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const body = yield* parseFormBody(request);
        const fallback = redirectToAllowedReturnTo(
          options,
          firstSearchParam(params.returnTo) ?? firstUrlParam(body, "returnTo"),
        );
        const cookie = request.cookies[Authorization.SessionCookieName];

        const csrfToken = request.headers[CsrfHeaderName] ?? firstUrlParam(body, CsrfFormFieldName);
        if (!isValidCsrfToken(options, csrfToken, cookie)) {
          return HttpServerResponse.text("Invalid CSRF token", { status: 403 });
        }

        const logoutUrl = cookie
          ? yield* auth
              .getLogoutUrl({ sealedSession: cookie, returnTo: fallback })
              .pipe(
                Effect.catchTag("WorkOsSessionError", () =>
                  Effect.succeed(withAuthError(options, fallback, "logout_failed")),
                ),
              )
          : fallback;

        const response = HttpServerResponse.redirect(logoutUrl);
        return yield* Authorization.clearSessionCookie(response, options).pipe(
          Effect.catchTag("CookieError", () => Effect.succeed(response)),
        );
      }),
    );
  }),
);

export * as AuthRoutes from "./Routes.ts";
