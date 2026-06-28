import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { CloudflareAiGatewayModels } from "../../agent-loop/CloudflareAiGatewayModels.ts";
import { DenoraApi } from "../Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Ai", (handlers) =>
  handlers.handle("listAiModels", () =>
    Effect.succeed(CloudflareAiGatewayModels.catalogResponse()),
  ),
);

export * as AiHandlers from "./Handlers.ts";
