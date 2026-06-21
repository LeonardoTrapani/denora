import * as Alchemy from "alchemy";
import * as AdoptPolicy from "alchemy/AdoptPolicy";
import * as Axiom from "alchemy/Axiom";
import type { Chart, LayoutCell } from "alchemy/Axiom";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import * as Neon from "alchemy/Neon";
import * as Output from "alchemy/Output";
import * as RemovalPolicy from "alchemy/RemovalPolicy";
import { AlchemyDb } from "@denora/server/persistence/AlchemyDb";
import { ServerResource } from "@denora/server/server/Resource";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";

const RootDomain = "denora.me";

const deployedStages = {
  dev: {
    webDomain: `dev.${RootDomain}`,
    apiDomain: `api.dev.${RootDomain}`,
  },
  staging: {
    webDomain: `staging.${RootDomain}`,
    apiDomain: `api.staging.${RootDomain}`,
  },
  prod: {
    webDomain: RootDomain,
    apiDomain: `api.${RootDomain}`,
  },
} as const;

type Deployment = (typeof deployedStages)[keyof typeof deployedStages];

interface ObservabilityDeployment {
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

const deploymentForStage = (stage: string) =>
  Option.fromUndefinedOr((deployedStages as Record<string, Deployment | undefined>)[stage]);

const ObservabilityDatasets = {
  logs: "denora-logs",
  metrics: "denora-metrics",
  traces: "denora-traces",
} as const;

const datasetQuery = (dataset: string, stage: string, query: string) =>
  `['${dataset}']\n| where ['deployment.environment'] == ${JSON.stringify(stage)}\n${query}`;

const dashboardFor = (
  stage: string,
  datasets: {
    readonly logs: string;
    readonly metrics: string;
    readonly traces: string;
  },
) => {
  const charts = [
    {
      id: "trace-volume",
      name: "Trace Volume",
      type: "TimeSeries",
      query: {
        apl: datasetQuery(datasets.traces, stage, "| summarize count() by bin_auto(_time)"),
      },
    },
    {
      id: "log-volume",
      name: "Log Volume",
      type: "TimeSeries",
      query: {
        apl: datasetQuery(datasets.logs, stage, "| summarize count() by bin_auto(_time)"),
      },
    },
    {
      id: "metric-volume",
      name: "Metric Volume",
      type: "TimeSeries",
      query: {
        apl: datasetQuery(datasets.metrics, stage, "| summarize count() by bin_auto(_time)"),
      },
    },
    {
      id: "recent-logs",
      name: "Recent Logs",
      type: "LogStream",
      query: {
        apl: datasetQuery(datasets.logs, stage, "| limit 100"),
      },
    },
  ] satisfies Chart[];

  const layout = [
    { i: "trace-volume", x: 0, y: 0, w: 6, h: 5 },
    { i: "log-volume", x: 6, y: 0, w: 6, h: 5 },
    { i: "metric-volume", x: 0, y: 5, w: 6, h: 5 },
    { i: "recent-logs", x: 6, y: 5, w: 6, h: 5 },
  ] satisfies LayoutCell[];

  return Axiom.Dashboard("DenoraOpsDashboard", {
    dashboard: {
      name: `Denora ${stage} Operations`,
      owner: "",
      description: "Stage-scoped application and Cloudflare Worker telemetry.",
      charts,
      layout,
      refreshTime: 60,
      schemaVersion: 2,
      timeWindowStart: "qr-now-1h",
      timeWindowEnd: "qr-now",
    },
  });
};

const observabilityForStage = (stage: string) =>
  Effect.gen(function* () {
    const prefix = `denora-${stage}`;
    const logsName = ObservabilityDatasets.logs;
    const metricsName = ObservabilityDatasets.metrics;
    const tracesName = ObservabilityDatasets.traces;
    const logsDestinationName = `${prefix}-cloudflare-logs-to-axiom`;
    const tracesDestinationName = `${prefix}-cloudflare-traces-to-axiom`;
    const logs = yield* Axiom.Dataset("AxiomLogs", {
      name: logsName,
      kind: "otel:logs:v1",
      description: "Denora OpenTelemetry logs",
      retentionDays: 30,
      useRetentionPeriod: true,
    }).pipe(AdoptPolicy.adopt(true), RemovalPolicy.retain(true));
    const traces = yield* Axiom.Dataset("AxiomTraces", {
      name: tracesName,
      kind: "otel:traces:v1",
      description: "Denora OpenTelemetry traces",
      retentionDays: 30,
      useRetentionPeriod: true,
    }).pipe(AdoptPolicy.adopt(true), RemovalPolicy.retain(true));
    const metrics = yield* Axiom.Dataset("AxiomMetrics", {
      name: metricsName,
      kind: "otel:metrics:v1",
      description: "Denora OpenTelemetry metrics",
      retentionDays: 30,
      useRetentionPeriod: true,
    }).pipe(AdoptPolicy.adopt(true), RemovalPolicy.retain(true));
    const ingestToken = yield* Axiom.ApiToken("AxiomIngestToken", {
      name: `${prefix}-otel-ingest`,
      description: `Denora ${stage} OpenTelemetry ingest token`,
      datasetCapabilities: Output.all(logs.name, traces.name, metrics.name).pipe(
        Output.map(([logsDataset, tracesDataset, metricsDataset]) => ({
          [logsDataset]: { ingest: ["create"] as const },
          [tracesDataset]: { ingest: ["create"] as const },
          [metricsDataset]: { ingest: ["create"] as const },
        })),
      ),
    });
    const axiomAuthorizationHeader = ingestToken.token.pipe(
      Output.map((token) => `Bearer ${Redacted.value(token)}`),
    );

    yield* Cloudflare.ObservabilityDestination("CloudflareTracesToAxiom", {
      name: tracesDestinationName,
      url: traces.otelTracesEndpoint,
      headers: {
        authorization: axiomAuthorizationHeader,
        "x-axiom-dataset": traces.name,
      },
      logpushDataset: "opentelemetry-traces",
      skipPreflightCheck: true,
    });
    yield* Cloudflare.ObservabilityDestination("CloudflareLogsToAxiom", {
      name: logsDestinationName,
      url: logs.otelLogsEndpoint,
      headers: {
        authorization: axiomAuthorizationHeader,
        "x-axiom-dataset": logs.name,
      },
      logpushDataset: "opentelemetry-logs",
      skipPreflightCheck: true,
    });
    // TODO: Restore once Cloudflare supports Workers OTEL metrics export.
    // The API schema accepts opentelemetry-metrics, but Cloudflare currently
    // returns 400 "Not so fast, curious explorer!". The Workers OTEL export
    // docs also say metrics export is not yet supported.
    // yield* Cloudflare.ObservabilityDestination("CloudflareMetricsToAxiom", {
    //   name: `${prefix}-cloudflare-metrics-to-axiom`,
    //   url: metrics.otelMetricsEndpoint,
    //   headers: {
    //     authorization: axiomAuthorizationHeader,
    //     "x-axiom-dataset": metrics.name,
    //   },
    //   logpushDataset: "opentelemetry-metrics",
    //   skipPreflightCheck: true,
    // });
    yield* dashboardFor(stage, {
      logs: logsName,
      metrics: metricsName,
      traces: tracesName,
    });

    return {
      logsDataset: logs.name,
      logsDestinationName,
      logsEndpoint: logs.otelLogsEndpoint,
      metricsDataset: metrics.name,
      metricsEndpoint: metrics.otelMetricsEndpoint,
      token: ingestToken.token,
      tracesDataset: traces.name,
      tracesDestinationName,
      tracesEndpoint: traces.otelTracesEndpoint,
    } satisfies ObservabilityDeployment;
  });

export default Alchemy.Stack(
  "Denora",
  {
    providers: Layer.mergeAll(Cloudflare.providers(), Drizzle.providers()).pipe(
      Layer.provideMerge(Neon.providers()),
      Layer.provideMerge(Axiom.providers()),
    ),
    state: Cloudflare.state(),
  },
  Effect.gen(function* () {
    const stage = yield* Alchemy.Stage;
    const deployment = deploymentForStage(stage);
    const zone = yield* Option.match(deployment, {
      onNone: () => Effect.void,
      onSome: () =>
        Cloudflare.Zone("denora-zone", { name: RootDomain }).pipe(AdoptPolicy.adopt(true)),
    });

    const { branch } = yield* AlchemyDb.DenoraDb;
    const hyperdrive = yield* AlchemyDb.DenoraHyperdrive;
    const observability = yield* Option.match(deployment, {
      onNone: () => Effect.void,
      onSome: () => observabilityForStage(stage),
    });

    const server = yield* ServerResource.Resource.pipe(
      Effect.provideService(
        ServerResource.Deployment,
        Option.match(deployment, {
          onNone: () => ({}),
          onSome: ({ apiDomain, webDomain }) => ({ apiDomain, observability, webDomain }),
        }),
      ),
    );

    const serverUrl = server.url.as<string>();

    const webProps = Option.match(deployment, {
      onNone: () => ({
        rootDir: "./packages/web",
        env: {
          VITE_API_URL: serverUrl,
        },
        compatibility: {
          flags: ["nodejs_compat" as const],
        },
      }),
      onSome: ({ apiDomain, webDomain }) => ({
        rootDir: "./packages/web",
        domain: webDomain,
        env: {
          VITE_API_URL: `https://${apiDomain}`,
        },
        compatibility: {
          flags: ["nodejs_compat" as const],
        },
      }),
    });

    const web = yield* Cloudflare.Vite("Web", webProps);
    const domains = Option.getOrUndefined(deployment);

    return {
      apiDomain: domains?.apiDomain,
      databaseName: branch.databaseName,
      hyperdriveId: hyperdrive.hyperdriveId,
      observability,
      stage,
      serverUrl,
      webDomain: domains?.webDomain,
      webUrl: web.url.as<string>(),
      zoneId: zone?.zoneId,
    };
  }),
);
