import * as Drizzle from "alchemy/Drizzle";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";

const makeClient = (connectionString: Effect.Effect<Redacted.Redacted<string>>) =>
  Drizzle.postgres(connectionString);

export type Client = Effect.Success<ReturnType<typeof makeClient>>;

export interface Interface {
  readonly client: Client;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/Db") {}

export const layer = (client: Client): Layer.Layer<Service> =>
  Layer.succeed(Service, Service.of({ client }));

export * as Db from "./Db.ts";
