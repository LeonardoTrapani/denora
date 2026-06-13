import type * as Alchemy from "alchemy";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Redacted from "effect/Redacted";
import { AlchemyDb } from "./AlchemyDb.ts";

const makeClient = (
  connectionString: Effect.Effect<Redacted.Redacted<string>, never, Alchemy.RuntimeContext>,
) => Drizzle.postgres(connectionString);

export type Client = Effect.Success<ReturnType<typeof makeClient>>;

export interface Interface {
  readonly client: Client;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/Db") {}

export const hyperdriveLayer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const hyperdrive = yield* Cloudflare.Hyperdrive.bind(AlchemyDb.DenoraHyperdrive);
    const client = yield* Drizzle.postgres(hyperdrive.connectionString);

    return Service.of({ client });
  }),
).pipe(Layer.provide(Cloudflare.HyperdriveBindingLive));

export * as Db from "./Db.ts";
