import * as Layer from "effect/Layer";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Authorization } from "../auth/Authorization.ts";
import { Api } from "./Api.ts";
import * as AccountHandlers from "./account/Handlers.ts";
import * as SystemHandlers from "./system/Handlers.ts";

export const handlers = Layer.mergeAll(SystemHandlers.layer, AccountHandlers.layer);

export const layer = Layer.mergeAll(HttpApiBuilder.layer(Api.DenoraApi), Authorization.routes).pipe(
  Layer.provide(handlers),
  Layer.provide(Authorization.layer),
);

export * as Routes from "./Routes.ts";
