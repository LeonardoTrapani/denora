import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { Auth } from "../../src/auth/Auth.ts";
import { DenoraUser, Unauthorized } from "../../src/auth/User.ts";

// A stand-in Auth.Service for HTTP tests: resolves the session straight from the
// request (typically a cookie) so the Authorization middleware and protected
// routes can be exercised without a real Better Auth instance or database.
export const layer = (resolve: (request: Request) => Option.Option<DenoraUser>) =>
  Layer.succeed(
    Auth.Service,
    Auth.Service.of({
      handle: () => Effect.succeed(new Response(null, { status: 204 })),
      getSession: (request) => Effect.succeed(resolve(request)),
      requireSession: (request) => {
        const user = resolve(request);
        return Option.isNone(user)
          ? Effect.fail(new Unauthorized({ message: "Authentication required" }))
          : Effect.succeed(user.value);
      },
    }),
  );
