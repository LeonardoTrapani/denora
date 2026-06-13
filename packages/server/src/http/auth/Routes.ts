import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Authorization } from "../../auth/Authorization.ts";
import { WorkOsAuth } from "../../auth/WorkOsAuth.ts";
import { DefaultWebOrigin, ServerConfig } from "../../config/ServerConfig.ts";

const firstSearchParam = (value: string | ReadonlyArray<string> | undefined) =>
  Array.isArray(value) ? (value[0] ?? null) : (value ?? null);

const redirectToAllowedWebOrigin = (options: ServerConfig.Auth, candidate: string | null) => {
  const fallback = options.webOrigins[0] ?? DefaultWebOrigin;
  if (!candidate) return fallback;

  if (URL.canParse(candidate)) {
    const url = new URL(candidate);
    if (options.webOrigins.includes(url.origin)) return url.toString();
    return fallback;
  }

  if (candidate.startsWith("/") && !candidate.startsWith("//")) {
    return new URL(candidate, fallback).toString();
  }

  return fallback;
};

const redirectToFallback = (destination: string) =>
  Effect.succeed(HttpServerResponse.redirect(destination));

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* WorkOsAuth.Service;
    const config = yield* ServerConfig.Service;
    const options = config.auth;

    yield* router.add("GET", "/auth/login", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const destination = redirectToAllowedWebOrigin(options, firstSearchParam(params.returnTo));

        if (!URL.canParse(request.url)) return HttpServerResponse.redirect(destination);

        return yield* Effect.gen(function* () {
          const requestUrl = new URL(request.url);
          const authorizationUrl = yield* auth.getAuthorizationUrl({
            redirectUri: new URL("/auth/callback", requestUrl.origin).toString(),
            returnTo: destination,
          });

          return HttpServerResponse.redirect(authorizationUrl);
        }).pipe(Effect.catchTag("WorkOsAuthError", () => redirectToFallback(destination)));
      }),
    );

    yield* router.add("GET", "/auth/callback", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const destination = redirectToAllowedWebOrigin(options, firstSearchParam(params.state));
        const callbackCode = firstSearchParam(params.code);

        if (!callbackCode) return HttpServerResponse.redirect(destination);

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

          const response = HttpServerResponse.redirect(destination);
          if (!session.sealedSession) return response;
          return yield* Authorization.setSessionCookie(
            response,
            session.sealedSession,
            options,
          ).pipe(Effect.orDie);
        }).pipe(
          Effect.catchTags({
            WorkOsAuthError: () => redirectToFallback(destination),
            UserSyncError: () => redirectToFallback(destination),
          }),
        );
      }),
    );

    yield* router.add("POST", "/auth/logout", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const fallback = redirectToAllowedWebOrigin(options, firstSearchParam(params.returnTo));
        const cookie = request.cookies[Authorization.SessionCookieName];
        const logoutUrl = cookie
          ? yield* auth
              .getLogoutUrl({ sealedSession: cookie, returnTo: fallback })
              .pipe(Effect.catchTag("WorkOsSessionError", () => Effect.succeed(fallback)))
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
