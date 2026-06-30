import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Config from "effect/Config";
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
} from "../agent-run/AgentConversationObject.ts";
import { AgentRunPersistence } from "../agent-run/AgentRunPersistence.ts";
import { AgentRuns } from "../agent-run/AgentRuns.ts";
import { AuthLive } from "../auth/Live.ts";
import { CloudflareDynamicWorkerSandbox } from "../code-sandbox/CloudflareDynamicWorkerSandbox.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { ConversationPersistence } from "../conversation/ConversationPersistence.ts";
import { Conversations } from "../conversation/Conversations.ts";
import { Routes } from "../http/Routes.ts";
import { Telemetry } from "../observability/Telemetry.ts";
import { AlchemyDb } from "../persistence/AlchemyDb.ts";
import { Db } from "../persistence/Db.ts";

export interface AxiomOtelConfig {
  readonly logsDataset: unknown;
  readonly logsEndpoint: unknown;
  readonly metricsDataset: unknown;
  readonly metricsEndpoint: unknown;
  readonly token: unknown;
  readonly tracesDataset: unknown;
  readonly tracesEndpoint: unknown;
}

export interface ObservabilityConfig extends AxiomOtelConfig {
  readonly logsDestinationName?: string | undefined;
  readonly tracesDestinationName?: string | undefined;
}

export interface DeploymentConfig {
  readonly apiDomain?: string | undefined;
  readonly observability?: ObservabilityConfig | undefined;
  readonly stage?: string | undefined;
  readonly webDomain?: string | undefined;
}

export const Deployment = Context.Reference<DeploymentConfig>(
  "@denora/server/Resource/Deployment",
  {
    defaultValue: () => ({}),
  },
);

const origin = (domain: string) => `https://${domain}`;

const optionalString = (name: string) => Config.option(Config.string(name));

const telemetryEnv = Config.all({
  gitBranch: optionalString("DENORA_GIT_BRANCH"),
  gitSha: optionalString("DENORA_GIT_SHA"),
  serviceInstanceId: optionalString("DENORA_SERVICE_INSTANCE_ID"),
  serviceVersion: optionalString("DENORA_SERVICE_VERSION"),
  telemetrySource: optionalString("DENORA_TELEMETRY_SOURCE"),
});

const bindOptionalEnv = (
  env: Record<string, unknown>,
  name: string,
  value: Option.Option<string>,
): void => {
  const resolved = Option.getOrUndefined(value);
  if (resolved !== undefined && resolved.length > 0) env[name] = resolved;
};

const bindAxiomEnv = (env: Record<string, unknown>, observability: AxiomOtelConfig): void => {
  env.AXIOM_INGEST_TOKEN = observability.token;
  env.AXIOM_OTEL_LOGS_DATASET = observability.logsDataset;
  env.AXIOM_OTEL_LOGS_ENDPOINT = observability.logsEndpoint;
  env.AXIOM_OTEL_METRICS_DATASET = observability.metricsDataset;
  env.AXIOM_OTEL_METRICS_ENDPOINT = observability.metricsEndpoint;
  env.AXIOM_OTEL_TRACES_DATASET = observability.tracesDataset;
  env.AXIOM_OTEL_TRACES_ENDPOINT = observability.tracesEndpoint;
};

const props = Effect.gen(function* () {
  const deployment = yield* Deployment;
  const observability = deployment.observability;
  const stage = deployment.stage ?? "local";
  const env: Record<string, unknown> = {
    ALCHEMY_STAGE: stage,
  };

  if (deployment.apiDomain !== undefined) {
    env.WORKOS_REDIRECT_BASE_URL = origin(deployment.apiDomain);
  }

  if (deployment.webDomain !== undefined) {
    env.DENORA_COOKIE_DOMAIN = deployment.webDomain;
    env.DENORA_WEB_ORIGINS = origin(deployment.webDomain);
  }

  const telemetry = yield* telemetryEnv;

  env.DENORA_TELEMETRY_SOURCE = Option.getOrElse(telemetry.telemetrySource, () =>
    stage === "local" ? "local-dev" : "cloudflare-worker",
  );
  bindOptionalEnv(env, "DENORA_GIT_BRANCH", telemetry.gitBranch);
  bindOptionalEnv(env, "DENORA_GIT_SHA", telemetry.gitSha);
  bindOptionalEnv(env, "DENORA_SERVICE_INSTANCE_ID", telemetry.serviceInstanceId);
  bindOptionalEnv(env, "DENORA_SERVICE_VERSION", telemetry.serviceVersion);

  if (observability !== undefined) {
    bindAxiomEnv(env, observability);
  }

  const cloudflareLogsDestination = observability?.logsDestinationName;
  const cloudflareTracesDestination = observability?.tracesDestinationName;

  const baseProps = {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat" as const],
    },
    dev: {
      port: 1338,
      strictPort: true,
    },
    env,
    logpush: cloudflareLogsDestination !== undefined || cloudflareTracesDestination !== undefined,
    observability: {
      enabled: true,
      headSamplingRate: 1,
      logs: {
        enabled: true,
        ...(cloudflareLogsDestination === undefined
          ? {}
          : { destinations: [cloudflareLogsDestination] }),
        invocationLogs: true,
        headSamplingRate: 1,
        persist: true,
      },
      traces: {
        enabled: true,
        ...(cloudflareTracesDestination === undefined
          ? {}
          : { destinations: [cloudflareTracesDestination] }),
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
) {}

export { AgentConversationObject };

export default Resource.make(
  props,
  Effect.gen(function* () {
    const config = yield* ServerConfig.load;
    const hyperdriveConnection = yield* AlchemyDb.DenoraHyperdrive;
    const hyperdrive = yield* Cloudflare.Hyperdrive.Connect(hyperdriveConnection);
    const db = yield* Drizzle.postgres(hyperdrive.connectionString);
    const conversationObjects = yield* AgentConversationObject;
    const codeWorkerLoader = yield* Cloudflare.WorkerLoader("CODE_WORKER_LOADER");
    const codeSandboxDispatcherFactory = yield* Effect.promise(() =>
      import("../code-sandbox/CloudflareRpcToolDispatcher.ts")
        .then((module) => module.CloudflareRpcToolDispatcher.dispatcherFactory)
        .catch(() => undefined),
    );

    return {
      fetch: Routes.layer.pipe(
        Layer.provide(Conversations.layer(conversationObjects)),
        Layer.provide(AgentRuns.layer(conversationObjects)),
        Layer.provide(AgentRunPersistence.layer),
        Layer.provide(ConversationPersistence.layer),
        Layer.provide(Db.layer(db)),
        Layer.provide(
          CloudflareDynamicWorkerSandbox.layer({
            loader: codeWorkerLoader,
            ...(codeSandboxDispatcherFactory === undefined
              ? {}
              : { dispatcherFactory: codeSandboxDispatcherFactory }),
          }),
        ),
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
      Layer.mergeAll(AgentConversationObjectLive, Cloudflare.Hyperdrive.ConnectBinding),
    ),
  ),
);

export * as ServerResource from "./Resource.ts";
