import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import * as HttpApiSecurity from "effect/unstable/httpapi/HttpApiSecurity";
import { SessionCookieName } from "./Session.ts";
import { CurrentUser, Unauthorized } from "./User.ts";

export { CurrentUser, DenoraUser, Unauthorized } from "./User.ts";
export { SessionCookieName } from "./Session.ts";

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

export * as AuthorizationApi from "./AuthorizationApi.ts";
