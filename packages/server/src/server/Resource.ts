import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Effect from "effect/Effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import { ServerConfig } from "../config/ServerConfig.ts";
import { AlchemyDb } from "../persistence/AlchemyDb.ts";
import { ServerLayers } from "./Layers.ts";

export class Resource extends Cloudflare.Worker<Resource>()(
  "Server",
  {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat"],
    },
  },
  Effect.gen(function* () {
    const config = yield* ServerConfig.load;
    const hyperdrive = yield* Cloudflare.Hyperdrive.bind(AlchemyDb.DenoraHyperdrive);
    const db = yield* Drizzle.postgres(hyperdrive.connectionString);

    return {
      fetch: ServerLayers.webHandlerLayer({ db, config }).pipe(HttpRouter.toHttpEffect),
    };
  }).pipe(Effect.provide(Cloudflare.HyperdriveBindingLive)),
) {}

export * as ServerResource from "./Resource.ts";
