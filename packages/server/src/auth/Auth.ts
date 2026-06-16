import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import type { DenoraUser, Unauthorized } from "./User.ts";

/**
 * Wraps unexpected Better Auth failures.
 */
export class AuthProviderError extends Schema.TaggedErrorClass<AuthProviderError>()(
  "AuthProviderError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

/**
 * Server-owned auth port. `handle` serves Better Auth's own HTTP endpoints;
 * `getSession`/`requireSession` resolve the current user for protected routes.
 */
export class Service extends Context.Service<
  Service,
  {
    readonly handle: (request: Request) => Effect.Effect<Response, AuthProviderError>;
    readonly getSession: (
      request: Request,
    ) => Effect.Effect<Option.Option<DenoraUser>, AuthProviderError>;
    readonly requireSession: (
      request: Request,
    ) => Effect.Effect<DenoraUser, Unauthorized | AuthProviderError>;
  }
>()("@denora/server/Auth") {}

export * as Auth from "./Auth.ts";
