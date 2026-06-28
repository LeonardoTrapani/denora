import type { StreamFn } from "@earendil-works/pi-agent-core";
import { getModel, getModels, streamSimple, type Api, type Model } from "@earendil-works/pi-ai";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { ServerConfig } from "../config/ServerConfig.ts";

export const defaultProviderId = "openrouter";
export const defaultModelId = "openai/gpt-5.5";
export const defaultModelSpecifier = `${defaultProviderId}/${defaultModelId}`;

export const defaultModel = getModel(defaultProviderId, defaultModelId);
export const models: ReadonlyArray<Model<Api>> = getModels(defaultProviderId);

type ProviderStreamResult = Awaited<ReturnType<StreamFn>>;

export interface StreamInput {
  readonly model: Parameters<StreamFn>[0];
  readonly context: Parameters<StreamFn>[1];
  readonly options?: Parameters<StreamFn>[2];
}

export interface ProviderInterface {
  readonly defaultModel: Model<Api>;
  readonly models: ReadonlyArray<Model<Api>>;
  readonly stream: StreamFn;
}

export interface Interface {
  readonly defaultModel: Model<Api>;
  readonly models: ReadonlyArray<Model<Api>>;
  readonly stream: (input: StreamInput) => Effect.Effect<ProviderStreamResult>;
}

export interface ModelOptions {
  readonly apiKey?: NonNullable<Parameters<StreamFn>[2]>["apiKey"] | undefined;
  readonly defaultModel?: Model<Api> | undefined;
  readonly maxTokens?: NonNullable<Parameters<StreamFn>[2]>["maxTokens"] | undefined;
  readonly temperature?: NonNullable<Parameters<StreamFn>[2]>["temperature"] | undefined;
}

export class Provider extends Context.Service<Provider, ProviderInterface>()(
  "@denora/server/PiAgentProvider/Provider",
) {}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/PiAgentProvider",
) {}

export const providerLayer = (options: Partial<ProviderInterface> = {}) =>
  Layer.succeed(
    Provider,
    Provider.of({
      defaultModel: options.defaultModel ?? defaultModel,
      models: options.models ?? models,
      stream: options.stream ?? streamSimple,
    }),
  );

export const layer = (config: ModelOptions = {}) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const provider = yield* Provider;
      const configuredDefaultModel = config.defaultModel ?? provider.defaultModel;
      const configuredModels = provider.models;

      const stream: Interface["stream"] = ({ model, context, options }) =>
        Effect.promise(async () => {
          const apiKey = options?.apiKey ?? config.apiKey;
          const maxTokens = options?.maxTokens ?? config.maxTokens;
          const temperature = options?.temperature ?? config.temperature;
          const finalOptions = {
            ...options,
            ...(apiKey === undefined ? {} : { apiKey }),
            ...(maxTokens === undefined ? {} : { maxTokens }),
            ...(temperature === undefined ? {} : { temperature }),
          };

          return provider.stream(model ?? configuredDefaultModel, context, finalOptions);
        });

      return Service.of({ defaultModel: configuredDefaultModel, models: configuredModels, stream });
    }),
  );

export const layerFromConfig: Layer.Layer<Service, never, Provider | ServerConfig.Service> =
  Layer.unwrap(
    Effect.gen(function* () {
      const config = yield* ServerConfig.Service;
      return layer({
        apiKey: Redacted.value(config.model.openRouterApiKey),
        defaultModel,
      });
    }),
  );

export const defaultLayer = layerFromConfig.pipe(Layer.provide(providerLayer()));

export * as PiAgentProvider from "./PiAgentProvider.ts";
