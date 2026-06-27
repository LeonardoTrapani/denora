import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { DenoraApi } from "../Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "System", (handlers) =>
  handlers.handle("health", () => Effect.succeed({ status: "ok" })),
);

export * as SystemHandlers from "./Handlers.ts";
