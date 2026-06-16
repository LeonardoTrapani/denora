import { randomUUID } from "node:crypto";
import * as Effect from "effect/Effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Auth } from "../../auth/Auth.ts";
import { authBasePath } from "../../auth/BetterAuth.ts";

/**
 * Mounts Better Auth's own HTTP handler under `/api/auth/*`. Better Auth owns
 * every auth endpoint (sign-in/up, sign-out, get-session, callbacks, …), so we
 * delegate the whole prefix to it instead of defining routes by hand.
 */
export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* Auth.Service;

    yield* router.add("*", `${authBasePath}/*`, (request) =>
      Effect.gen(function* () {
        const startedAt = Date.now();
        yield* Effect.logInfo("better-auth request started", {
          method: request.method,
          url: request.url,
        });

        const webRequest = yield* HttpServerRequest.toWeb(request);
        const webResponse = yield* auth.handle(webRequest);

        yield* Effect.logInfo("better-auth request finished", {
          durationMs: Date.now() - startedAt,
          method: request.method,
          status: webResponse.status,
          url: request.url,
        });

        return HttpServerResponse.fromWeb(webResponse);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const traceId = randomUUID();
            yield* Effect.logError("better-auth request failed", {
              method: request.method,
              traceId,
              url: request.url,
              cause,
            });
            return HttpServerResponse.fromWeb(
              Response.json({ error: "InternalError", traceId }, { status: 500 }),
            );
          }),
        ),
      ),
    );
  }),
);

export * as AuthRoutes from "./Routes.ts";
