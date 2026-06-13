import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import { WorkOsAuth } from "../auth/WorkOsAuth.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Routes } from "../http/Routes.ts";

export interface Options {
  readonly db: Routes.DbClient;
  readonly config: ServerConfig.Values;
}

const configMiddleware = (config: ServerConfig.Values) =>
  HttpRouter.middleware<{ provides: ServerConfig.Service }>()(
    Effect.succeed((httpEffect) => Effect.provideService(httpEffect, ServerConfig.Service, config)),
  ).layer;

export const webHandlerLayer = (options: Options) =>
  Routes.layer(options.db).pipe(
    Layer.provide(WorkOsAuth.layer(options.config.auth)),
    Layer.provide(configMiddleware(options.config)),
    Layer.provide([HttpPlatform.layer, Etag.layer]),
    Layer.provide(
      HttpRouter.cors({
        allowedOrigins: options.config.auth.webOrigins,
        allowedMethods: ["GET", "POST", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization"],
        credentials: true,
      }),
    ),
  );

export * as ServerLayers from "./Layers.ts";
