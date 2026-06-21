import * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { AuthLive } from "../auth/Live.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Routes } from "../http/Routes.ts";

export interface DeploymentConfig {
  readonly apiDomain?: string | undefined;
  readonly webDomain?: string | undefined;
}

export const Deployment = Context.Reference<DeploymentConfig>(
  "@denora/server/Resource/Deployment",
  {
    defaultValue: () => ({}),
  },
);

const origin = (domain: string) => `https://${domain}`;

const props = Effect.gen(function* () {
  const deployment = yield* Deployment;
  const env: Record<string, string> = {};

  if (deployment.apiDomain !== undefined) {
    env.WORKOS_REDIRECT_BASE_URL = origin(deployment.apiDomain);
  }

  if (deployment.webDomain !== undefined) {
    env.DENORA_WEB_ORIGINS = origin(deployment.webDomain);
  }

  const baseProps = {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat" as const],
    },
    env,
  };

  return Option.match(Option.fromUndefinedOr(deployment.apiDomain), {
    onNone: () => baseProps,
    onSome: (domain) => ({ ...baseProps, domain }),
  });
});

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
  props,
  Effect.gen(function* () {
    const config = yield* ServerConfig.load;

    return {
      fetch: Routes.layer.pipe(
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
