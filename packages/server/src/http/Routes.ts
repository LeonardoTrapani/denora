import * as Layer from "effect/Layer";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Authorization } from "../auth/Authorization.ts";
import { Api } from "./Api.ts";
import * as AccountHandlers from "./account/Handlers.ts";
import * as SystemHandlers from "./system/Handlers.ts";

export type DbClient = Authorization.DbClient;

export const handlers = Layer.mergeAll(SystemHandlers.layer, AccountHandlers.layer);

export const layer = (db: DbClient) =>
  Layer.mergeAll(HttpApiBuilder.layer(Api.DenoraApi), Authorization.routes(db)).pipe(
    Layer.provide(handlers),
    Layer.provide(Authorization.layer(db)),
  );

export * as Routes from "./Routes.ts";
