import type { Api, Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { PiAgentProvider } from "../../agent-loop/PiAgentProvider.ts";
import { ConversationDomain } from "../../conversation/ConversationDomain.ts";
import { DenoraApi } from "../Api.ts";
import type { AiModelCatalogItem, AiModelsResponse, AiThinkingLevelItem } from "./Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Ai", (handlers) =>
  handlers.handle("listAiModels", () => Effect.succeed(catalogResponse())),
);

const defaultThinkingLevel = "medium" as const;

const catalogResponse = (): AiModelsResponse => ({
  defaultModelId: PiAgentProvider.defaultModelSpecifier,
  defaultThinkingLevel,
  thinkingLevels: ConversationDomain.thinkingLevels.map(thinkingLevelItem),
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

const thinkingLevelItem = (id: ConversationDomain.ThinkingLevel): AiThinkingLevelItem => ({
  id,
  name: thinkingLevelName(id),
  description: thinkingLevelDescription(id),
  default: id === defaultThinkingLevel,
});

const thinkingLevelName = (id: ConversationDomain.ThinkingLevel): string => {
  switch (id) {
    case "off":
      return "Off";
    case "minimal":
      return "Minimal";
    case "low":
      return "Low";
    case "medium":
      return "Medium";
    case "high":
      return "High";
    case "xhigh":
      return "Extra high";
  }
  return id;
};

const thinkingLevelDescription = (id: ConversationDomain.ThinkingLevel): string => {
  switch (id) {
    case "off":
      return "No explicit reasoning budget.";
    case "minimal":
      return "Fastest reasoning-capable responses.";
    case "low":
      return "Light reasoning for simple tasks.";
    case "medium":
      return "Balanced reasoning for everyday work.";
    case "high":
      return "More reasoning for hard tasks.";
    case "xhigh":
      return "Maximum reasoning for the hardest tasks.";
  }
  return "Reasoning budget.";
};

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
      thinkingLevels: thinkingLevelsForModel(model),
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

const thinkingLevelsForModel = (
  model: Model<Api>,
): ReadonlyArray<ConversationDomain.ThinkingLevel> =>
  model.reasoning ? ConversationDomain.thinkingLevels : ["off"];

const catalogApi = (api: Api): AiModelCatalogItem["api"] => {
  if (!isCatalogApi(api)) return "openai-completions";
  return api;
};

const isCatalogApi = (api: Api): api is AiModelCatalogItem["api"] =>
  api === "anthropic-messages" || api === "openai-responses" || api === "openai-completions";

export * as AiHandlers from "./Handlers.ts";
