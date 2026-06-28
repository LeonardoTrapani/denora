import type { Api, Model } from "@earendil-works/pi-ai";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { PiAgentProvider } from "../../agent-loop/PiAgentProvider.ts";
import { ConversationDomain } from "../../conversation/ConversationDomain.ts";
import { DenoraApi } from "../Api.ts";
import type {
  AiModelCatalogItem,
  AiModelProviderGroup,
  AiModelsResponse,
  AiThinkingLevelItem,
} from "./Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "Ai", (handlers) =>
  handlers.handle("listAiModels", () => Effect.succeed(catalogResponse())),
);

const defaultThinkingLevel = "medium" as const;

const catalogResponse = (): AiModelsResponse => {
  const models = PiAgentProvider.models.flatMap((model) =>
    isCatalogApi(model.api) ? [catalogItem(model)] : [],
  );
  return {
    defaultModelId: PiAgentProvider.defaultModelSpecifier,
    defaultThinkingLevel,
    thinkingLevels: ConversationDomain.thinkingLevels.map(thinkingLevelItem),
    providers: providerGroups(models),
  };
};

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
  const displayProvider = displayProviderForModel(model);
  return {
    id: `${model.provider}/${model.id}`,
    name: modelNameForDisplay(model.name, displayProvider.name),
    displayProvider,
    family: displayProvider.id,
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

const providerGroups = (
  models: ReadonlyArray<AiModelCatalogItem>,
): ReadonlyArray<AiModelProviderGroup> => {
  const groups = new Map<string, { id: string; name: string; models: Array<AiModelCatalogItem> }>();
  for (const model of models) {
    const group = groups.get(model.displayProvider.id);
    if (group === undefined) {
      groups.set(model.displayProvider.id, {
        id: model.displayProvider.id,
        name: model.displayProvider.name,
        models: [model],
      });
    } else {
      group.models.push(model);
    }
  }
  return [...groups.values()];
};

const displayProviderForModel = (model: Model<Api>): AiModelCatalogItem["displayProvider"] => {
  const nameProvider = providerNameFromModelName(model.name);
  const idProvider = providerNameFromModelId(model.id);
  const name = nameProvider ?? idProvider ?? providerNameFromModelId(model.provider) ?? "Unknown";
  return { id: providerId(name), name };
};

const providerNameFromModelName = (name: string): string | undefined => {
  const [provider] = name.split(":", 1);
  const trimmed = provider?.trim();
  return trimmed && trimmed !== name ? trimmed : undefined;
};

const providerNameFromModelId = (id: string): string | undefined => {
  const [provider] = id.split("/", 1);
  if (provider === undefined || provider.length === 0) return undefined;
  return providerDisplayNames[provider] ?? titleCaseProvider(provider);
};

const modelNameForDisplay = (name: string, providerName: string): string => {
  const prefix = `${providerName}:`;
  return name.startsWith(prefix) ? name.slice(prefix.length).trim() : name;
};

const providerId = (name: string): string =>
  name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");

const titleCaseProvider = (id: string): string =>
  id
    .split(/[-_]/g)
    .filter((part) => part.length > 0)
    .map((part) => part.slice(0, 1).toUpperCase() + part.slice(1))
    .join(" ");

const providerDisplayNames: Readonly<Record<string, string>> = {
  ai21: "AI21",
  alibaba: "Alibaba",
  anthropic: "Anthropic",
  cohere: "Cohere",
  deepseek: "DeepSeek",
  google: "Google",
  meta: "Meta",
  microsoft: "Microsoft",
  mistralai: "Mistral",
  moonshotai: "Moonshot AI",
  openai: "OpenAI",
  openrouter: "OpenRouter",
  perplexity: "Perplexity",
  qwen: "Qwen",
  xai: "xAI",
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
