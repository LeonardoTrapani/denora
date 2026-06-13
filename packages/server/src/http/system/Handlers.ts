import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { DenoraApi } from "../Api.ts";
import { Health } from "./Schema.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "System", (handlers) =>
  handlers.handle("health", () =>
    Effect.gen(function* () {
      return new Health({ status: "ok" });
    }),
  ),
);

export * as SystemHandlers from "./Handlers.ts";
