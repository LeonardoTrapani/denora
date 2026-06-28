import type { Api, Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { PiAgentProvider } from "../../agent-loop/PiAgentProvider.ts";
import { DenoraApi } from "../Api.ts";
import type { AiModelCatalogItem, AiModelsResponse } from "./Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Ai", (handlers) =>
  handlers.handle("listAiModels", () => Effect.succeed(catalogResponse())),
);

const catalogResponse = (): AiModelsResponse => ({
  defaultModelId: PiAgentProvider.defaultModelSpecifier,
  providers: [
    {
      id: PiAgentProvider.defaultProviderId,
      name: "OpenRouter",
      models: PiAgentProvider.models.flatMap((model) =>
        isCatalogApi(model.api) ? [catalogItem(model)] : [],
      ),
    },
  ],
});

const catalogItem = (model: Model<Api>): AiModelCatalogItem => {
  const api = catalogApi(model.api);
  return {
    id: `${model.provider}/${model.id}`,
    name: model.name,
    displayProvider: { id: PiAgentProvider.defaultProviderId, name: "OpenRouter" },
    family: "default",
    default:
      model.provider === PiAgentProvider.defaultProviderId &&
      model.id === PiAgentProvider.defaultModelId,
    api,
    capabilities: {
      reasoning: model.reasoning,
      ...(model.reasoning ? { reasoningMode: "openai-compatible" as const } : {}),
      tools: true,
    },
    inputModalities: model.input,
    outputModalities: ["text"],
    contextWindow: model.contextWindow,
    maxTokens: model.maxTokens,
    cost: model.cost,
    lifecycle: "stable",
  };
};

const catalogApi = (api: Api): AiModelCatalogItem["api"] => {
  if (!isCatalogApi(api)) return "openai-completions";
  return api;
};

const isCatalogApi = (api: Api): api is AiModelCatalogItem["api"] =>
  api === "anthropic-messages" || api === "openai-responses" || api === "openai-completions";

export * as AiHandlers from "./Handlers.ts";
