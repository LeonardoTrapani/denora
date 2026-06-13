import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity";
import { DefaultWebOrigin, ServerConfig } from "../config/ServerConfig.ts";
import { WorkOsAuth } from "./WorkOsAuth.ts";
import { CurrentUser, Unauthorized } from "./User.ts";

export { CurrentUser, DenoraUser, Unauthorized } from "./User.ts";

export const SessionCookieName = WorkOsAuth.SessionCookieName;

export const sessionCookie = HttpApiSecurity.apiKey({
  in: "cookie",
  key: SessionCookieName,
});

export class Service extends HttpApiMiddleware.Service<
  Service,
  {
    provides: CurrentUser;
  }
>()("@denora/server/Authorization", {
  security: {
    session: sessionCookie,
  },
  error: Unauthorized,
}) {}

class AuthRequestError extends Schema.TaggedErrorClass<AuthRequestError>()("AuthRequestError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}

class SessionCookieError extends Schema.TaggedErrorClass<SessionCookieError>()(
  "SessionCookieError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

const redirectToAllowedWebOrigin = (options: ServerConfig.Auth, candidate: string | null) => {
  const fallback = options.webOrigins[0] ?? DefaultWebOrigin;
  if (!candidate) return fallback;

  try {
    const url = new URL(candidate);
    if (options.webOrigins.includes(url.origin)) return url.toString();
  } catch {
    if (candidate.startsWith("/")) return `${fallback}${candidate}`;
  }

  return fallback;
};

const setSessionCookie = Effect.fn("Authorization.setSessionCookie")(
  (response: HttpServerResponse.HttpServerResponse, value: string, options: ServerConfig.Auth) =>
    HttpServerResponse.setCookie(response, SessionCookieName, value, {
      domain: options.cookieDomain,
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "lax" as const,
    }).pipe(
      Effect.mapError((cause) => new SessionCookieError({ operation: "setSessionCookie", cause })),
    ),
);

const clearSessionCookie = Effect.fn("Authorization.clearSessionCookie")(
  (response: HttpServerResponse.HttpServerResponse, options: ServerConfig.Auth) =>
    HttpServerResponse.expireCookie(response, SessionCookieName, {
      domain: options.cookieDomain,
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "lax" as const,
    }).pipe(
      Effect.mapError(
        (cause) => new SessionCookieError({ operation: "clearSessionCookie", cause }),
      ),
    ),
);

export const layer: Layer.Layer<Service, never, WorkOsAuth.Service | ServerConfig.Service> =
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const auth = yield* WorkOsAuth.Service;
      const config = yield* ServerConfig.Service;
      const options = config.auth;

      return Service.of({
        session: (httpEffect, { credential }) =>
          Effect.gen(function* () {
            const session = yield* auth.authenticateSession(credential).pipe(
              Effect.catchTags({
                WorkOsSessionError: (error) =>
                  Effect.gen(function* () {
                    yield* Effect.logWarning(`Rejected WorkOS session after ${error._tag}`);
                    return yield* new Unauthorized({ message: "Missing or invalid session" });
                  }),
                UserSyncError: (error) =>
                  Effect.gen(function* () {
                    yield* Effect.logWarning(`Rejected WorkOS session after ${error._tag}`);
                    return yield* new Unauthorized({
                      message: "Unable to establish authenticated user",
                    });
                  }),
              }),
            );

            if (session.sealedSession) {
              yield* HttpEffect.appendPreResponseHandler((_, response) =>
                setSessionCookie(response, session.sealedSession!, options).pipe(
                  Effect.catchTag("SessionCookieError", (error) =>
                    Effect.map(
                      Effect.logWarning(
                        `Recovered WorkOS auth cookie mutation from ${error.operation}`,
                      ),
                      () => response,
                    ),
                  ),
                ),
              );
            }

            return yield* Effect.provideService(httpEffect, CurrentUser, session.user);
          }),
      });
    }),
  );

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* WorkOsAuth.Service;
    const config = yield* ServerConfig.Service;
    const options = config.auth;

    yield* router.add("GET", "/auth/login", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const returnTo = params.returnTo;
        const destination = redirectToAllowedWebOrigin(
          options,
          Array.isArray(returnTo) ? (returnTo[0] ?? null) : (returnTo ?? null),
        );

        return yield* Effect.gen(function* () {
          const parsedUrl = yield* Effect.try({
            try: () => new URL(request.url),
            catch: (cause) => new AuthRequestError({ operation: "parseLoginUrl", cause }),
          });

          const authorizationUrl = yield* auth.getAuthorizationUrl({
            redirectUri: new URL("/auth/callback", parsedUrl.origin).toString(),
            returnTo: destination,
          });

          return HttpServerResponse.redirect(authorizationUrl);
        }).pipe(
          Effect.catchTags({
            AuthRequestError: (error) =>
              Effect.map(Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`), () =>
                HttpServerResponse.redirect(destination),
              ),
            WorkOsAuthError: (error) =>
              Effect.map(Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`), () =>
                HttpServerResponse.redirect(destination),
              ),
          }),
        );
      }),
    );

    yield* router.add("GET", "/auth/callback", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const state = params.state;
        const code = params.code;
        const destination = redirectToAllowedWebOrigin(
          options,
          Array.isArray(state) ? (state[0] ?? null) : (state ?? null),
        );
        const callbackCode = Array.isArray(code) ? (code[0] ?? null) : (code ?? null);

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
          return yield* setSessionCookie(response, session.sealedSession, options);
        }).pipe(
          Effect.catchTags({
            WorkOsAuthError: (error) =>
              Effect.map(Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`), () =>
                HttpServerResponse.redirect(destination),
              ),
            UserSyncError: (error) =>
              Effect.map(Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`), () =>
                HttpServerResponse.redirect(destination),
              ),
            SessionCookieError: (error) =>
              Effect.map(Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`), () =>
                HttpServerResponse.redirect(destination),
              ),
          }),
        );
      }),
    );

    yield* router.add("POST", "/auth/logout", (request) =>
      Effect.gen(function* () {
        const params = yield* HttpServerRequest.ParsedSearchParams;
        const returnTo = params.returnTo;
        const fallback = redirectToAllowedWebOrigin(
          options,
          Array.isArray(returnTo) ? (returnTo[0] ?? null) : (returnTo ?? null),
        );
        const cookie = request.cookies[SessionCookieName];
        const logoutUrl = cookie
          ? yield* auth
              .getLogoutUrl({ sealedSession: cookie, returnTo: fallback })
              .pipe(
                Effect.catchTag("WorkOsSessionError", (error) =>
                  Effect.map(
                    Effect.logWarning(`Recovered WorkOS auth route from ${error._tag}`),
                    () => fallback,
                  ),
                ),
              )
          : fallback;

        const response = HttpServerResponse.redirect(logoutUrl);
        return yield* clearSessionCookie(response, options).pipe(
          Effect.catchTag("SessionCookieError", (error) =>
            Effect.map(
              Effect.logWarning(`Recovered WorkOS auth cookie mutation from ${error.operation}`),
              () => response,
            ),
          ),
        );
      }),
    );
  }),
);

export * as Authorization from "./Authorization.ts";
