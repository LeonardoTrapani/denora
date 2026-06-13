import type * as Alchemy from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity";
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
    requires: Alchemy.RuntimeContext;
  }
>()("@denora/server/Authorization", {
  security: {
    session: sessionCookie,
  },
  error: Unauthorized,
}) {}

export const layer: Layer.Layer<Service, never, WorkOsAuth.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const auth = yield* WorkOsAuth.Service;

    return Service.of({
      session: (httpEffect, { credential }) => auth.authenticateSession(httpEffect, credential),
    });
  }),
);

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* WorkOsAuth.Service;

    yield* router.add("GET", "/auth/login", auth.login);
    yield* router.add("GET", "/auth/callback", auth.callback);
    yield* router.add("POST", "/auth/logout", auth.logout);
  }),
);

export * as Authorization from "./Authorization.ts";
