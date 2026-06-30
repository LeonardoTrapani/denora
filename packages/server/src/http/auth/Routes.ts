import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
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
        const webRequest = yield* HttpServerRequest.toWeb(request);
        const webResponse = yield* auth.handle(webRequest);
        yield* Effect.annotateCurrentSpan({
          "http.response.status_code": webResponse.status,
        });

        return HttpServerResponse.fromWeb(webResponse);
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.gen(function* () {
            const traceId = crypto.randomUUID();
            yield* Effect.logError("auth request failed", {
              method: request.method,
              traceId,
              path: requestPathname(request),
              cause,
            });
            return HttpServerResponse.fromWeb(
              Response.json({ error: "InternalError", traceId }, { status: 500 }),
            );
          }),
        ),
        Effect.annotateSpans({
          "http.request.method": request.method,
          "http.route": `${authBasePath}/*`,
        }),
        Effect.withSpan("denora.http.auth"),
      ),
    );
  }),
);

const requestPathname = (request: HttpServerRequest.HttpServerRequest): string => {
  const url = HttpServerRequest.toURL(request);
  return Option.isNone(url) ? request.url : url.value.pathname;
};

export * as AuthRoutes from "./Routes.ts";
