import * as Cloudflare from "alchemy/Cloudflare";
import { RuntimeContext } from "alchemy";
import * as Effect from "effect/Effect";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as Layer from "effect/Layer";
import { AgentThreadObject } from "../agent/ThreadObject.ts";
import { AgentThreads } from "../agent/Threads.ts";
import { AuthLive } from "../auth/Live.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Routes } from "../http/Routes.ts";

const corsLayer = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* ServerConfig.Service;

    return HttpRouter.cors({
      allowedOrigins: config.auth.webOrigins,
      allowedMethods: ["GET", "POST", "OPTIONS"],
      allowedHeaders: ["Content-Type", "Authorization", "X-CSRF-Token"],
      credentials: true,
    });
  }),
);

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
    const agentThreads = yield* AgentThreadObject.ThreadObject;
    const runtimeContext = yield* RuntimeContext;

    return {
      fetch: Routes.layer.pipe(
        Layer.provide(AgentThreads.layer(agentThreads, runtimeContext)),
        Layer.provide(AuthLive.layerFromConfig),
        Layer.provide([HttpPlatform.layer, Etag.layer]),
        Layer.provide(corsLayer),
        Layer.provide(ServerConfig.layer(config)),
        HttpRouter.toHttpEffect,
      ),
    };
  }),
) {}

export default Resource;

export * as ServerResource from "./Resource.ts";
