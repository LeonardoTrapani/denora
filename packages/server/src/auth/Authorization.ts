import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import { Auth } from "./Auth.ts";
import { Service } from "./AuthorizationApi.ts";
import { CurrentUser, Unauthorized } from "./User.ts";

export { CurrentUser, DenoraUser, Service, Unauthorized } from "./AuthorizationApi.ts";

export const layer: Layer.Layer<Service, never, Auth.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const auth = yield* Auth.Service;

    return Service.of((httpEffect) =>
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const webRequest = yield* HttpServerRequest.toWeb(request).pipe(
          Effect.catch(() => new Unauthorized({ message: "Authentication required" })),
        );
        const user = yield* auth.requireSession(webRequest).pipe(
          Effect.catchTag("AuthProviderError", (error) =>
            Effect.gen(function* () {
              yield* Effect.logWarning("auth provider failed while resolving protected session", {
                operation: error.operation,
                cause: error.cause,
              });
              return yield* new Unauthorized({ message: "Authentication required" });
            }),
          ),
        );

        return yield* Effect.provideService(httpEffect, CurrentUser, user);
      }),
    );
  }),
);

export * as Authorization from "./Authorization.ts";
