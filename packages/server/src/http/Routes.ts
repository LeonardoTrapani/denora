import * as Layer from "effect/Layer";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Authorization } from "../auth/Authorization.ts";
import { Api } from "./Api.ts";
import * as AccountHandlers from "./account/Handlers.ts";
import { AuthRoutes } from "./auth/Routes.ts";
import * as SystemHandlers from "./system/Handlers.ts";

export const handlers = Layer.mergeAll(SystemHandlers.layer, AccountHandlers.layer).pipe(
  Layer.provide(Authorization.layer),
);

export const apiLayer = HttpApiBuilder.layer(Api.DenoraApi).pipe(Layer.provide(handlers));

export const layer = Layer.mergeAll(apiLayer, AuthRoutes.routes);

export * as Routes from "./Routes.ts";
