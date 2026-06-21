import * as Config from "effect/Config";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as OtlpLogger from "effect/unstable/observability/OtlpLogger";
import * as OtlpMetrics from "effect/unstable/observability/OtlpMetrics";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";
import * as OtlpTracer from "effect/unstable/observability/OtlpTracer";

const optionalString = (name: string) => Config.option(Config.string(name));

const load = Config.all({
  logsDataset: optionalString("AXIOM_OTEL_LOGS_DATASET"),
  logsEndpoint: optionalString("AXIOM_OTEL_LOGS_ENDPOINT"),
  metricsDataset: optionalString("AXIOM_OTEL_METRICS_DATASET"),
  metricsEndpoint: optionalString("AXIOM_OTEL_METRICS_ENDPOINT"),
  serviceVersion: Config.string("DENORA_SERVICE_VERSION").pipe(Config.withDefault("0.0.0")),
  stage: Config.string("ALCHEMY_STAGE").pipe(Config.withDefault("local")),
  token: Config.option(Config.redacted("AXIOM_INGEST_TOKEN")),
  tracesDataset: optionalString("AXIOM_OTEL_TRACES_DATASET"),
  tracesEndpoint: optionalString("AXIOM_OTEL_TRACES_ENDPOINT"),
});

const headers = (token: Redacted.Redacted<string>, dataset: string) => ({
  authorization: `Bearer ${Redacted.value(token)}`,
  "x-axiom-dataset": dataset,
});

export const layer: Layer.Layer<never, never, never> = Layer.unwrap(
  Effect.gen(function* () {
    const config = yield* load;
    const token = Option.getOrUndefined(config.token);
    const tracesEndpoint = Option.getOrUndefined(config.tracesEndpoint);
    const tracesDataset = Option.getOrUndefined(config.tracesDataset);
    const logsEndpoint = Option.getOrUndefined(config.logsEndpoint);
    const logsDataset = Option.getOrUndefined(config.logsDataset);
    const metricsEndpoint = Option.getOrUndefined(config.metricsEndpoint);
    const metricsDataset = Option.getOrUndefined(config.metricsDataset);

    if (
      token === undefined ||
      tracesEndpoint === undefined ||
      tracesDataset === undefined ||
      logsEndpoint === undefined ||
      logsDataset === undefined ||
      metricsEndpoint === undefined ||
      metricsDataset === undefined
    ) {
      return Layer.empty;
    }

    const resource = {
      serviceName: "denora-server",
      serviceVersion: config.serviceVersion,
      attributes: {
        "alchemy.stage": config.stage,
        "cloud.provider": "cloudflare",
        "deployment.environment": config.stage,
      },
    };

    return Layer.mergeAll(
      OtlpTracer.layer({
        url: tracesEndpoint,
        resource,
        headers: headers(token, tracesDataset),
        exportInterval: "5 seconds",
      }),
      OtlpLogger.layer({
        url: logsEndpoint,
        resource,
        headers: headers(token, logsDataset),
        exportInterval: "5 seconds",
        mergeWithExisting: true,
      }),
      OtlpMetrics.layer({
        url: metricsEndpoint,
        resource,
        headers: headers(token, metricsDataset),
        exportInterval: "10 seconds",
      }),
    ).pipe(Layer.provide(OtlpSerialization.layerProtobuf), Layer.provide(FetchHttpClient.layer));
  }).pipe(Effect.catch(() => Effect.succeed(Layer.empty))),
);

export * as Telemetry from "./Telemetry.ts";
