import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as Layer from "effect/Layer";
import { ServerConfig } from "../config/ServerConfig.ts";
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

    return {
      fetch: ServerLayers.webHandlerLayer.pipe(
        Layer.provide(ServerConfig.layer(config)),
        HttpRouter.toHttpEffect,
      ),
    };
  }),
) {}

export * as ServerResource from "./Resource.ts";
