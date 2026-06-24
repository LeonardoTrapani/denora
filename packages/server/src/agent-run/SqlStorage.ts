import type * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Layer from "effect/Layer";

export class Service extends Context.Service<Service, Cloudflare.SqlStorage>()(
  "@denora/server/SqlStorage",
) {}

export const layer = (sql: Cloudflare.SqlStorage): Layer.Layer<Service> =>
  Layer.succeed(Service, sql);

export * as SqlStorage from "./SqlStorage.ts";
