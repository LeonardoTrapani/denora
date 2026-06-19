import * as Effect from "effect/Effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { Auth } from "../../auth/Auth.ts";

export const authBasePath = "/api/auth";

/**
 * Mounts Denora's WorkOS AuthKit endpoints under `/api/auth/*`.
 */
export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* Auth.Service;

    yield* router.add("*", `${authBasePath}/*`, (request) =>
      Effect.gen(function* () {
        const startedAt = Date.now();
        const requestPath = pathOnly(request.url);
        yield* Effect.logInfo("auth request started", {
          method: request.method,
          path: requestPath,
        });

        const webRequest = yield* HttpServerRequest.toWeb(request);
        const webResponse = yield* auth.handle(webRequest);

        yield* Effect.logInfo("auth request finished", {
          durationMs: Date.now() - startedAt,
          method: request.method,
          status: webResponse.status,
          path: requestPath,
        });

        return HttpServerResponse.fromWeb(webResponse);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const traceId = crypto.randomUUID();
            yield* Effect.logError("auth request failed", {
              method: request.method,
              traceId,
              path: pathOnly(request.url),
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

const pathOnly = (url: string): string => {
  try {
    return new URL(url).pathname;
  } catch {
    return url.split("?", 1)[0] ?? url;
  }
};

export * as AuthRoutes from "./Routes.ts";
