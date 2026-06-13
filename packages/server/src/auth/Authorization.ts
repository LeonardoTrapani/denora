import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpEffect from "effect/unstable/http/HttpEffect";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity";
import { ServerConfig } from "../config/ServerConfig.ts";
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

export const setSessionCookie = Effect.fn("Authorization.setSessionCookie")(
  (response: HttpServerResponse.HttpServerResponse, value: string, options: ServerConfig.Auth) =>
    HttpServerResponse.setCookie(response, SessionCookieName, value, {
      domain: options.cookieDomain,
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "lax" as const,
    }),
);

export const clearSessionCookie = Effect.fn("Authorization.clearSessionCookie")(
  (response: HttpServerResponse.HttpServerResponse, options: ServerConfig.Auth) =>
    HttpServerResponse.expireCookie(response, SessionCookieName, {
      domain: options.cookieDomain,
      path: "/",
      httpOnly: true,
      secure: true,
      sameSite: "lax" as const,
    }),
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
                WorkOsSessionError: () =>
                  new Unauthorized({ message: "Missing or invalid session" }),
                UserSyncError: () =>
                  new Unauthorized({
                    message: "Unable to establish authenticated user",
                  }),
              }),
            );

            if (session.sealedSession) {
              yield* HttpEffect.appendPreResponseHandler((_, response) =>
                setSessionCookie(response, session.sealedSession!, options).pipe(
                  Effect.catchTag("CookieError", () => Effect.succeed(response)),
                ),
              );
            }

            return yield* Effect.provideService(httpEffect, CurrentUser, session.user);
          }),
      });
    }),
  );

export * as Authorization from "./Authorization.ts";
