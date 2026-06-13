import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { ServerConfig } from "../../config/ServerConfig.ts";
import { DenoraApi } from "../Api.ts";
import { Health } from "./Schema.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "System", (handlers) =>
  handlers.handle("health", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig.Service;

      yield* Effect.logInfo("System health check", {
        cookieDomainConfigured: config.auth.cookieDomain !== undefined,
        webOriginCount: config.auth.webOrigins.length,
      });

      return new Health({ status: "ok" });
    }),
  ),
);

export * as SystemHandlers from "./Handlers.ts";
