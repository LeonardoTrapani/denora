import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
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

export const webHandlerLayer = Routes.layer.pipe(
  Layer.provide(AuthLive.layerFromConfig),
  Layer.provide([HttpPlatform.layer, Etag.layer]),
  Layer.provide(corsLayer),
);

export * as ServerLayers from "./Layers.ts";
