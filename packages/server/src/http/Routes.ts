import * as Layer from "effect/Layer";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Authorization } from "../auth/Authorization.ts";
import { Api } from "./Api.ts";
import * as AccountHandlers from "./account/Handlers.ts";
import * as AgentRunHandlers from "./agent-run/Handlers.ts";
import { AgentRunStreamRoutes } from "./agent-run/StreamRoutes.ts";
import { AuthRoutes } from "./auth/Routes.ts";
import * as SystemHandlers from "./system/Handlers.ts";

export const handlers = Layer.mergeAll(
  SystemHandlers.layer,
  AccountHandlers.layer,
  AgentRunHandlers.layer,
);

export const apiLayer = HttpApiBuilder.layer(Api.DenoraApi).pipe(Layer.provide(handlers));

const protectedLayer = Layer.mergeAll(apiLayer, AgentRunStreamRoutes.routes).pipe(
  Layer.provide(Authorization.layer),
);

export const layer = Layer.mergeAll(protectedLayer, AuthRoutes.routes);

export * as Routes from "./Routes.ts";
