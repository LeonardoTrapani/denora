import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import { CurrentUser, Unauthorized } from "./User.ts";

export { CurrentUser, DenoraUser, Unauthorized } from "./User.ts";

/**
 * Protects HTTP API endpoints: resolves the WorkOS AuthKit session from the
 * request and provides `CurrentUser`. No `security` scheme — the implementation
 * reads the whole request because the session is carried by an HTTP-only cookie.
 */
export class Service extends HttpApiMiddleware.Service<Service, { provides: CurrentUser }>()(
  "@denora/server/Authorization",
  {
    error: Unauthorized,
  },
) {}

export * as AuthorizationApi from "./AuthorizationApi.ts";
