import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import {
  AgentConversationObject,
  AgentConversationObjectLive,
  AiGateway,
} from "../agent-run/AgentConversationObject.ts";
import { AuthLive } from "../auth/Live.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { ConversationPersistence } from "../conversation/ConversationPersistence.ts";
import { Conversations } from "../conversation/Conversations.ts";
import { Routes } from "../http/Routes.ts";
import { Telemetry } from "../observability/Telemetry.ts";
import { AlchemyDb } from "../persistence/AlchemyDb.ts";
import { Db } from "../persistence/Db.ts";

export interface ObservabilityConfig {
  readonly logsDataset: unknown;
  readonly logsDestinationName: string;
  readonly logsEndpoint: unknown;
  readonly metricsDataset: unknown;
  readonly metricsEndpoint: unknown;
  readonly token: unknown;
  readonly tracesDataset: unknown;
  readonly tracesDestinationName: string;
  readonly tracesEndpoint: unknown;
}

export interface DeploymentConfig {
  readonly apiDomain?: string | undefined;
  readonly observability?: ObservabilityConfig | undefined;
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
  const observability = deployment.observability;
  const env: Record<string, unknown> = {};

  if (deployment.apiDomain !== undefined) {
    env.WORKOS_REDIRECT_BASE_URL = origin(deployment.apiDomain);
  }

  if (deployment.webDomain !== undefined) {
    env.DENORA_COOKIE_DOMAIN = deployment.webDomain;
    env.DENORA_WEB_ORIGINS = origin(deployment.webDomain);
  }

  if (observability !== undefined) {
    env.AXIOM_INGEST_TOKEN = observability.token;
    env.AXIOM_OTEL_LOGS_DATASET = observability.logsDataset;
    env.AXIOM_OTEL_LOGS_ENDPOINT = observability.logsEndpoint;
    env.AXIOM_OTEL_METRICS_DATASET = observability.metricsDataset;
    env.AXIOM_OTEL_METRICS_ENDPOINT = observability.metricsEndpoint;
    env.AXIOM_OTEL_TRACES_DATASET = observability.tracesDataset;
    env.AXIOM_OTEL_TRACES_ENDPOINT = observability.tracesEndpoint;
  }

  const baseProps = {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat" as const],
    },
    env,
    logpush: observability !== undefined,
    observability: {
      enabled: true,
      headSamplingRate: 1,
      logs: {
        enabled: true,
        ...(observability === undefined
          ? {}
          : { destinations: [observability.logsDestinationName] }),
        invocationLogs: true,
        headSamplingRate: 1,
        persist: true,
      },
      traces: {
        enabled: true,
        ...(observability === undefined
          ? {}
          : { destinations: [observability.tracesDestinationName] }),
        headSamplingRate: 1,
        persist: true,
      },
    },
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
      allowedMethods: ["GET", "HEAD", "POST", "OPTIONS"],
      allowedHeaders: ["Content-Type", "Authorization", "X-CSRF-Token", "If-None-Match"],
      exposedHeaders: [
        "ETag",
        "Location",
        "Stream-Next-Offset",
        "Stream-Up-To-Date",
        "Stream-Closed",
        "Stream-Cursor",
      ],
      credentials: true,
    });
  }),
);

export class Resource extends Cloudflare.Worker<Resource, {}, AgentConversationObject>()(
  "Server",
  props,
) {}

export { AgentConversationObject };

export default Resource.make(
  Effect.gen(function* () {
    const config = yield* ServerConfig.load;
    yield* Cloudflare.AiGateway.bind(AiGateway);
    const hyperdrive = yield* Cloudflare.Hyperdrive.bind(AlchemyDb.DenoraHyperdrive);
    const db = yield* Drizzle.postgres(hyperdrive.connectionString);
    const conversationObjects = yield* AgentConversationObject;

    return {
      fetch: Routes.layer.pipe(
        Layer.provide(Conversations.layer(conversationObjects)),
        Layer.provide(ConversationPersistence.layer),
        Layer.provide(Db.layer(db)),
        Layer.provide(AuthLive.layerFromConfig),
        Layer.provide([HttpPlatform.layer, Etag.layer]),
        Layer.provide(corsLayer),
        Layer.provide(Telemetry.layer),
        Layer.provide(ServerConfig.layer(config)),
        HttpRouter.toHttpEffect,
      ),
    };
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        AgentConversationObjectLive,
        Cloudflare.AiGatewayBindingLive,
        Cloudflare.HyperdriveBindingLive,
      ),
    ),
  ),
);

export * as ServerResource from "./Resource.ts";
