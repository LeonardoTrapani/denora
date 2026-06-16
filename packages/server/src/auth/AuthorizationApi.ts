import * as HttpApiMiddleware from "effect/unstable/httpapi/HttpApiMiddleware";
import { CurrentUser, Unauthorized } from "./User.ts";

export { CurrentUser, DenoraUser, Unauthorized } from "./User.ts";

/**
 * Protects HTTP API endpoints: resolves the Better Auth session from the request
 * and provides `CurrentUser`. No `security` scheme — Better Auth owns its own
 * (signed) session cookie, so the implementation reads the whole request rather
 * than a single declared cookie.
 */
export class Service extends HttpApiMiddleware.Service<Service, { provides: CurrentUser }>()(
  "@denora/server/Authorization",
  {
    error: Unauthorized,
  },
) {}

export * as AuthorizationApi from "./AuthorizationApi.ts";
