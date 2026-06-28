import type { Api, Model, OpenAICompletionsCompat } from "@earendil-works/pi-ai";

export type RuntimeApi = "anthropic-messages" | "openai-responses" | "openai-completions";

export type RouteProvider = "anthropic" | "openai" | "workers-ai";

export interface RuntimeRoute {
  readonly provider: RouteProvider;
  readonly endpoint: string;
  readonly model: string;
}

export interface DisplayProvider {
  readonly id: string;
  readonly name: string;
}

export type CatalogLifecycle = "stable" | "preview" | "deprecated";

export type ReasoningMode =
  | "anthropic-manual"
  | "anthropic-adaptive"
  | "openai"
  | "openai-compatible";

export interface CatalogMetadata {
  /**
   * The provider/model id as Cloudflare's unified model catalog names it.
   *
   * Sources checked 2026-06-28:
   * - https://developers.cloudflare.com/ai-gateway/supported-models/
   * - https://developers.cloudflare.com/workers-ai/models/
   * - https://developers.cloudflare.com/api/resources/ai/subresources/models/methods/list/
   *
   * This checked-in registry intentionally includes only text-generation models
   * runnable by Denora's current Cloudflare AI Gateway adapters: native OpenAI
   * Responses, native Anthropic Messages, and Cloudflare-hosted Workers AI.
   * Other Cloudflare catalog text-generation entries such as Google Gemini,
   * xAI Grok, MiniMax, Alibaba Qwen, and DeepSeek native models require direct
   * provider-native or Cloudflare REST OpenAI-compatible adapters before they
   * can safely join the chat runtime registry.
   */
  readonly catalogId: string;
  readonly source: "cloudflare-ai-gateway";
  readonly routeProvider: RouteProvider;
  readonly displayProvider: DisplayProvider;
  readonly family: string;
  readonly reasoningMode?: ReasoningMode | undefined;
  readonly modalities: {
    readonly input: ReadonlyArray<"text" | "image">;
    readonly output: ReadonlyArray<"text">;
  };
  readonly lifecycle: CatalogLifecycle;
  readonly modelTask: "text-generation";
}

export interface RegistryEntry<TApi extends RuntimeApi = RuntimeApi> {
  readonly model: Model<TApi>;
  readonly route: RuntimeRoute;
  readonly catalog: CatalogMetadata;
}

export interface AiModelCatalogItem {
  readonly id: string;
  readonly name: string;
  readonly displayProvider: DisplayProvider;
  readonly family: string;
  readonly default: boolean;
  readonly api: RuntimeApi;
  readonly capabilities: {
    readonly reasoning: boolean;
    readonly reasoningMode?: ReasoningMode | undefined;
    readonly tools: boolean;
  };
  readonly inputModalities: ReadonlyArray<"text" | "image">;
  readonly outputModalities: ReadonlyArray<"text">;
  readonly contextWindow: number;
  readonly maxTokens: number;
  readonly cost: Model<Api>["cost"];
  readonly lifecycle: CatalogLifecycle;
}

export interface AiModelProviderGroup {
  readonly id: string;
  readonly name: string;
  readonly models: ReadonlyArray<AiModelCatalogItem>;
}

export interface AiModelCatalogResponse {
  readonly defaultModelId: string;
  readonly providers: ReadonlyArray<AiModelProviderGroup>;
}

export type RegistryModel = (typeof models)[keyof typeof models]["model"];

const baseUrl =
  "https://gateway.ai.cloudflare.com/v1/{CLOUDFLARE_ACCOUNT_ID}/{CLOUDFLARE_GATEWAY_ID}";

const workersAiCompat = {
  supportsStore: false,
  supportsDeveloperRole: false,
  supportsReasoningEffort: false,
  supportsUsageInStreaming: true,
  maxTokensField: "max_tokens",
  requiresToolResultName: false,
  requiresAssistantAfterToolResult: false,
  requiresThinkingAsText: false,
  requiresReasoningContentOnAssistantMessages: false,
  thinkingFormat: "openai",
  chatTemplateKwargs: {},
  openRouterRouting: {},
  vercelGatewayRouting: {},
  zaiToolStream: false,
  supportsStrictMode: false,
  sendSessionAffinityHeaders: true,
  supportsLongCacheRetention: false,
} as const satisfies OpenAICompletionsCompat;

const openAiReasoningLevelMap = { off: null } as const;
const anthropicAdaptiveLevelMap = {
  off: null,
  minimal: "low",
  low: "low",
  medium: "medium",
  high: "high",
  xhigh: "max",
} as const;
const anthropicAdaptiveXhighLevelMap = {
  ...anthropicAdaptiveLevelMap,
  xhigh: "xhigh",
} as const;
const anthropicManualLevelMap = { off: null } as const;

const providers = {
  anthropic: { id: "anthropic", name: "Anthropic" },
  openai: { id: "openai", name: "OpenAI" },
  moonshotai: { id: "moonshotai", name: "Moonshot AI" },
  zai: { id: "zai", name: "Z.ai / GLM" },
  meta: { id: "meta", name: "Meta" },
  qwen: { id: "qwen", name: "Qwen" },
  deepseek: { id: "deepseek", name: "DeepSeek" },
  google: { id: "google", name: "Google" },
  mistral: { id: "mistral", name: "Mistral AI" },
  nvidia: { id: "nvidia", name: "NVIDIA" },
  ibm: { id: "ibm", name: "IBM" },
  aisingapore: { id: "aisingapore", name: "AI Singapore" },
  nousresearch: { id: "nousresearch", name: "Nous Research" },
  microsoft: { id: "microsoft", name: "Microsoft" },
  defog: { id: "defog", name: "Defog" },
} as const satisfies Record<string, DisplayProvider>;

const anthropicModel = (input: {
  readonly id: string;
  readonly name: string;
  readonly inputCost: number;
  readonly outputCost: number;
  readonly cacheReadCost?: number | undefined;
  readonly cacheWriteCost?: number | undefined;
  readonly contextWindow?: number | undefined;
  readonly maxTokens?: number | undefined;
  readonly adaptive?: boolean | "xhigh" | undefined;
  readonly supportsTemperature?: boolean | undefined;
}): RegistryEntry<"anthropic-messages"> => ({
  model: {
    id: input.id,
    name: input.name,
    api: "anthropic-messages",
    provider: "cloudflare-ai-gateway",
    baseUrl: `${baseUrl}/anthropic`,
    compat: {
      sendSessionAffinityHeaders: true,
      ...(input.adaptive === undefined ? {} : { forceAdaptiveThinking: true }),
      ...(input.supportsTemperature === undefined
        ? {}
        : { supportsTemperature: input.supportsTemperature }),
    },
    reasoning: true,
    thinkingLevelMap:
      input.adaptive === "xhigh"
        ? anthropicAdaptiveXhighLevelMap
        : input.adaptive === true
          ? anthropicAdaptiveLevelMap
          : anthropicManualLevelMap,
    input: ["text", "image"],
    cost: {
      input: input.inputCost,
      output: input.outputCost,
      cacheRead: input.cacheReadCost ?? input.inputCost / 10,
      cacheWrite: input.cacheWriteCost ?? input.inputCost * 1.25,
    },
    contextWindow: input.contextWindow ?? 200_000,
    maxTokens: input.maxTokens ?? 64_000,
  },
  route: { provider: "anthropic", endpoint: "v1/messages", model: input.id },
  catalog: catalog({
    catalogId: `anthropic/${input.id}`,
    routeProvider: "anthropic",
    displayProvider: providers.anthropic,
    family: "claude",
    reasoningMode: input.adaptive === undefined ? "anthropic-manual" : "anthropic-adaptive",
    input: ["text", "image"],
  }),
});

const openAiResponsesModel = (input: {
  readonly id: string;
  readonly name: string;
  readonly reasoning?: boolean | undefined;
  readonly input?: ReadonlyArray<"text" | "image"> | undefined;
  readonly inputCost: number;
  readonly outputCost: number;
  readonly cacheReadCost?: number | undefined;
  readonly contextWindow?: number | undefined;
  readonly maxTokens?: number | undefined;
}): RegistryEntry<"openai-responses"> => ({
  model: {
    id: input.id,
    name: input.name,
    api: "openai-responses",
    provider: "cloudflare-ai-gateway",
    baseUrl: `${baseUrl}/openai`,
    compat: { supportsDeveloperRole: true, sendSessionIdHeader: true },
    reasoning: input.reasoning ?? true,
    ...(input.reasoning === false ? {} : { thinkingLevelMap: openAiReasoningLevelMap }),
    input: [...(input.input ?? ["text", "image"])],
    cost: {
      input: input.inputCost,
      output: input.outputCost,
      cacheRead: input.cacheReadCost ?? input.inputCost / 10,
      cacheWrite: 0,
    },
    contextWindow: input.contextWindow ?? 400_000,
    maxTokens: input.maxTokens ?? 128_000,
  },
  route: { provider: "openai", endpoint: "v1/responses", model: input.id },
  catalog: catalog({
    catalogId: `openai/${input.id}`,
    routeProvider: "openai",
    displayProvider: providers.openai,
    family: input.id.startsWith("o") ? "o-series" : "gpt",
    ...(input.reasoning === false ? {} : { reasoningMode: "openai" }),
    input: input.input ?? ["text", "image"],
  }),
});

const workersAiModel = (input: {
  readonly id: `@${"cf" | "hf"}/${string}`;
  readonly displayProvider: DisplayProvider;
  readonly family: string;
  readonly name?: string | undefined;
  readonly lifecycle?: CatalogLifecycle | undefined;
  readonly reasoning?: boolean | undefined;
  readonly input?: ReadonlyArray<"text" | "image"> | undefined;
  readonly inputCost?: number | undefined;
  readonly outputCost?: number | undefined;
  readonly cacheReadCost?: number | undefined;
  readonly contextWindow?: number | undefined;
  readonly maxTokens?: number | undefined;
}): RegistryEntry<"openai-completions"> => ({
  model: {
    id: `workers-ai/${input.id}`,
    name: input.name ?? modelNameFromCatalogId(input.id),
    api: "openai-completions",
    provider: "cloudflare-ai-gateway",
    baseUrl: `${baseUrl}/compat`,
    compat: workersAiCompat,
    reasoning: input.reasoning ?? false,
    input: [...(input.input ?? ["text"])],
    cost: {
      input: input.inputCost ?? 0,
      output: input.outputCost ?? 0,
      cacheRead: input.cacheReadCost ?? 0,
      cacheWrite: 0,
    },
    contextWindow: input.contextWindow ?? 128_000,
    maxTokens: input.maxTokens ?? 16_384,
  },
  route: { provider: "workers-ai", endpoint: input.id, model: input.id },
  catalog: catalog({
    catalogId: input.id,
    routeProvider: "workers-ai",
    displayProvider: input.displayProvider,
    family: input.family,
    ...(input.reasoning === true ? { reasoningMode: "openai-compatible" } : {}),
    input: input.input ?? ["text"],
    lifecycle: input.lifecycle,
  }),
});

const catalog = (input: {
  readonly catalogId: string;
  readonly routeProvider: RouteProvider;
  readonly displayProvider: DisplayProvider;
  readonly family: string;
  readonly reasoningMode?: ReasoningMode | undefined;
  readonly input: ReadonlyArray<"text" | "image">;
  readonly lifecycle?: CatalogLifecycle | undefined;
}): CatalogMetadata => ({
  source: "cloudflare-ai-gateway",
  catalogId: input.catalogId,
  routeProvider: input.routeProvider,
  displayProvider: input.displayProvider,
  family: input.family,
  ...(input.reasoningMode === undefined ? {} : { reasoningMode: input.reasoningMode }),
  modalities: { input: input.input, output: ["text"] },
  lifecycle: input.lifecycle ?? "stable",
  modelTask: "text-generation",
});

const modelNameFromCatalogId = (catalogId: string): string => {
  const slug = catalogId.split("/").at(-1) ?? catalogId;
  return slug
    .split("-")
    .map((part) => specialNameParts[part.toLowerCase()] ?? capitalize(part))
    .join(" ")
    .replace(/\bGpt\b/g, "GPT")
    .replace(/\bOss\b/g, "OSS")
    .replace(/\bGlm\b/g, "GLM")
    .replace(/\bQwq\b/g, "QwQ")
    .replace(/\bFp8\b/g, "FP8")
    .replace(/\bFp16\b/g, "FP16")
    .replace(/\bAwq\b/g, "AWQ")
    .replace(/\bLora\b/g, "LoRA")
    .replace(/\bHf\b/g, "HF")
    .replace(/\bIt\b/g, "IT")
    .replace(/\bAi\b/g, "AI");
};

const capitalize = (value: string): string =>
  value.length === 0 ? value : value[0]!.toUpperCase() + value.slice(1);

const specialNameParts: Record<string, string> = {
  gpt: "GPT",
  oss: "OSS",
  glm: "GLM",
  kimi: "Kimi",
  llama: "Llama",
  qwen: "Qwen",
  qwen3: "Qwen3",
  "qwen2.5": "Qwen2.5",
  qwq: "QwQ",
  deepseek: "DeepSeek",
  gemma: "Gemma",
  mistral: "Mistral",
  nemotron: "Nemotron",
  granite: "Granite",
  sea: "SEA",
  lion: "LION",
  fp8: "FP8",
  fp16: "FP16",
  awq: "AWQ",
  lora: "LoRA",
  hf: "HF",
  it: "IT",
  ai: "AI",
};

const model = <TApi extends RuntimeApi>(entry: RegistryEntry<TApi>): RegistryEntry<TApi> => entry;

export const models = {
  "claude-fable-5": model(
    anthropicModel({
      id: "claude-fable-5",
      name: "Claude Fable 5",
      inputCost: 3,
      outputCost: 15,
      contextWindow: 1_000_000,
      maxTokens: 128_000,
      adaptive: "xhigh",
      supportsTemperature: false,
    }),
  ),
  "claude-opus-4-8": model(
    anthropicModel({
      id: "claude-opus-4-8",
      name: "Claude Opus 4.8",
      inputCost: 15,
      outputCost: 75,
      contextWindow: 1_000_000,
      maxTokens: 128_000,
      adaptive: "xhigh",
      supportsTemperature: false,
    }),
  ),
  "claude-opus-4-7": model(
    anthropicModel({
      id: "claude-opus-4-7",
      name: "Claude Opus 4.7",
      inputCost: 15,
      outputCost: 75,
      contextWindow: 1_000_000,
      maxTokens: 128_000,
      adaptive: "xhigh",
      supportsTemperature: false,
    }),
  ),
  "claude-opus-4-6": model(
    anthropicModel({
      id: "claude-opus-4-6",
      name: "Claude Opus 4.6",
      inputCost: 15,
      outputCost: 75,
      contextWindow: 1_000_000,
      maxTokens: 128_000,
      adaptive: true,
    }),
  ),
  "claude-opus-4-5": model(
    anthropicModel({
      id: "claude-opus-4-5",
      name: "Claude Opus 4.5",
      inputCost: 5,
      outputCost: 25,
    }),
  ),
  "claude-sonnet-4-6": model(
    anthropicModel({
      id: "claude-sonnet-4-6",
      name: "Claude Sonnet 4.6",
      inputCost: 3,
      outputCost: 15,
      contextWindow: 1_000_000,
      maxTokens: 64_000,
      adaptive: true,
    }),
  ),
  "claude-sonnet-4-5": model(
    anthropicModel({
      id: "claude-sonnet-4-5",
      name: "Claude Sonnet 4.5",
      inputCost: 3,
      outputCost: 15,
      cacheReadCost: 0.3,
      cacheWriteCost: 3.75,
      maxTokens: 64_000,
    }),
  ),
  "claude-haiku-4-5": model(
    anthropicModel({
      id: "claude-haiku-4-5",
      name: "Claude Haiku 4.5",
      inputCost: 1,
      outputCost: 5,
    }),
  ),

  "gpt-5.5-pro": model(
    openAiResponsesModel({
      id: "gpt-5.5-pro",
      name: "GPT-5.5 Pro",
      inputCost: 15,
      outputCost: 120,
      contextWindow: 1_000_000,
    }),
  ),
  "gpt-5.5": model(
    openAiResponsesModel({
      id: "gpt-5.5",
      name: "GPT-5.5",
      inputCost: 2,
      outputCost: 16,
      contextWindow: 400_000,
    }),
  ),
  "gpt-5.4-pro": model(
    openAiResponsesModel({
      id: "gpt-5.4-pro",
      name: "GPT-5.4 Pro",
      inputCost: 15,
      outputCost: 120,
      contextWindow: 1_000_000,
    }),
  ),
  "gpt-5.4": model(
    openAiResponsesModel({
      id: "gpt-5.4",
      name: "GPT-5.4",
      inputCost: 2,
      outputCost: 16,
      contextWindow: 400_000,
    }),
  ),
  "gpt-5.4-mini": model(
    openAiResponsesModel({
      id: "gpt-5.4-mini",
      name: "GPT-5.4 Mini",
      inputCost: 0.4,
      outputCost: 3.2,
    }),
  ),
  "gpt-5.4-nano": model(
    openAiResponsesModel({
      id: "gpt-5.4-nano",
      name: "GPT-5.4 Nano",
      inputCost: 0.08,
      outputCost: 0.64,
    }),
  ),
  "gpt-5.1": model(
    openAiResponsesModel({
      id: "gpt-5.1",
      name: "GPT-5.1",
      inputCost: 1.25,
      outputCost: 10,
      cacheReadCost: 0.13,
    }),
  ),
  "gpt-5.1-chat": model(
    openAiResponsesModel({
      id: "gpt-5.1-chat",
      name: "GPT-5.1 Chat",
      inputCost: 1.25,
      outputCost: 10,
      cacheReadCost: 0.13,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "gpt-5": model(
    openAiResponsesModel({
      id: "gpt-5",
      name: "GPT-5",
      inputCost: 1.25,
      outputCost: 10,
      cacheReadCost: 0.13,
    }),
  ),
  "gpt-5-chat": model(
    openAiResponsesModel({
      id: "gpt-5-chat",
      name: "GPT-5 Chat",
      inputCost: 1.25,
      outputCost: 10,
      cacheReadCost: 0.13,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "gpt-5-mini": model(
    openAiResponsesModel({
      id: "gpt-5-mini",
      name: "GPT-5 Mini",
      inputCost: 0.25,
      outputCost: 2,
      cacheReadCost: 0.03,
    }),
  ),
  "gpt-5-nano": model(
    openAiResponsesModel({
      id: "gpt-5-nano",
      name: "GPT-5 Nano",
      inputCost: 0.05,
      outputCost: 0.4,
      cacheReadCost: 0.01,
    }),
  ),
  "gpt-4.1": model(
    openAiResponsesModel({
      id: "gpt-4.1",
      name: "GPT-4.1",
      reasoning: false,
      inputCost: 2,
      outputCost: 8,
      cacheReadCost: 0.5,
      contextWindow: 1_047_576,
      maxTokens: 32_768,
    }),
  ),
  "gpt-4.1-mini": model(
    openAiResponsesModel({
      id: "gpt-4.1-mini",
      name: "GPT-4.1 Mini",
      reasoning: false,
      inputCost: 0.4,
      outputCost: 1.6,
      cacheReadCost: 0.1,
      contextWindow: 1_047_576,
      maxTokens: 32_768,
    }),
  ),
  "gpt-4.1-nano": model(
    openAiResponsesModel({
      id: "gpt-4.1-nano",
      name: "GPT-4.1 Nano",
      reasoning: false,
      inputCost: 0.1,
      outputCost: 0.4,
      cacheReadCost: 0.03,
      contextWindow: 1_047_576,
      maxTokens: 32_768,
    }),
  ),
  "gpt-4o": model(
    openAiResponsesModel({
      id: "gpt-4o",
      name: "GPT-4o",
      reasoning: false,
      inputCost: 2.5,
      outputCost: 10,
      cacheReadCost: 1.25,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "gpt-4o-mini": model(
    openAiResponsesModel({
      id: "gpt-4o-mini",
      name: "GPT-4o Mini",
      reasoning: false,
      inputCost: 0.15,
      outputCost: 0.6,
      cacheReadCost: 0.08,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  o3: model(
    openAiResponsesModel({
      id: "o3",
      name: "o3",
      inputCost: 2,
      outputCost: 8,
      cacheReadCost: 0.5,
      contextWindow: 200_000,
      maxTokens: 100_000,
    }),
  ),
  "o3-mini": model(
    openAiResponsesModel({
      id: "o3-mini",
      name: "o3 Mini",
      inputCost: 1.1,
      outputCost: 4.4,
      cacheReadCost: 0.55,
      contextWindow: 200_000,
      maxTokens: 100_000,
    }),
  ),
  "o4-mini": model(
    openAiResponsesModel({
      id: "o4-mini",
      name: "o4 Mini",
      inputCost: 1.1,
      outputCost: 4.4,
      cacheReadCost: 0.28,
      contextWindow: 200_000,
      maxTokens: 100_000,
    }),
  ),

  "workers-ai/@cf/moonshotai/kimi-k2.7-code": model(
    workersAiModel({
      id: "@cf/moonshotai/kimi-k2.7-code",
      displayProvider: providers.moonshotai,
      family: "kimi",
      reasoning: true,
      input: ["text", "image"],
      inputCost: 0.95,
      outputCost: 4,
      cacheReadCost: 0.19,
      contextWindow: 262_144,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/moonshotai/kimi-k2.6": model(
    workersAiModel({
      id: "@cf/moonshotai/kimi-k2.6",
      displayProvider: providers.moonshotai,
      family: "kimi",
      reasoning: true,
      input: ["text", "image"],
      inputCost: 0.95,
      outputCost: 4,
      cacheReadCost: 0.16,
      contextWindow: 262_144,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/moonshotai/kimi-k2.5": model(
    workersAiModel({
      id: "@cf/moonshotai/kimi-k2.5",
      displayProvider: providers.moonshotai,
      family: "kimi",
      lifecycle: "deprecated",
      reasoning: true,
      input: ["text", "image"],
      inputCost: 0.6,
      outputCost: 3,
      cacheReadCost: 0.1,
      contextWindow: 256_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/zai-org/glm-5.2": model(
    workersAiModel({
      id: "@cf/zai-org/glm-5.2",
      displayProvider: providers.zai,
      family: "glm",
      reasoning: true,
      inputCost: 1.4,
      outputCost: 4.4,
      cacheReadCost: 0.26,
      contextWindow: 131_072,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/zai-org/glm-4.7-flash": model(
    workersAiModel({
      id: "@cf/zai-org/glm-4.7-flash",
      displayProvider: providers.zai,
      family: "glm",
      reasoning: true,
      inputCost: 0.06,
      outputCost: 0.4,
      contextWindow: 131_072,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/openai/gpt-oss-120b": model(
    workersAiModel({
      id: "@cf/openai/gpt-oss-120b",
      displayProvider: providers.openai,
      family: "gpt-oss",
      reasoning: true,
      inputCost: 0.35,
      outputCost: 0.75,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/openai/gpt-oss-20b": model(
    workersAiModel({
      id: "@cf/openai/gpt-oss-20b",
      displayProvider: providers.openai,
      family: "gpt-oss",
      reasoning: true,
      inputCost: 0.2,
      outputCost: 0.3,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/meta/llama-4-scout-17b-16e-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-4-scout-17b-16e-instruct",
      displayProvider: providers.meta,
      family: "llama",
      input: ["text", "image"],
      inputCost: 0.27,
      outputCost: 0.85,
      contextWindow: 131_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.3-70b-instruct-fp8-fast": model(
    workersAiModel({
      id: "@cf/meta/llama-3.3-70b-instruct-fp8-fast",
      displayProvider: providers.meta,
      family: "llama",
      inputCost: 0.29,
      outputCost: 2.25,
      contextWindow: 24_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.2-11b-vision-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3.2-11b-vision-instruct",
      displayProvider: providers.meta,
      family: "llama",
      input: ["text", "image"],
      inputCost: 0.049,
      outputCost: 0.68,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.2-3b-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3.2-3b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      inputCost: 0.051,
      outputCost: 0.34,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.2-1b-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3.2-1b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      inputCost: 0.027,
      outputCost: 0.2,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.1-70b-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3.1-70b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.1-8b-instruct-fast": model(
    workersAiModel({
      id: "@cf/meta/llama-3.1-8b-instruct-fast",
      displayProvider: providers.meta,
      family: "llama",
      inputCost: 0.05,
      outputCost: 0.08,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.1-8b-instruct-fp8": model(
    workersAiModel({
      id: "@cf/meta/llama-3.1-8b-instruct-fp8",
      displayProvider: providers.meta,
      family: "llama",
      inputCost: 0.15,
      outputCost: 0.29,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.1-8b-instruct-awq": model(
    workersAiModel({
      id: "@cf/meta/llama-3.1-8b-instruct-awq",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      inputCost: 0.12,
      outputCost: 0.27,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-3.1-8b-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3.1-8b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      inputCost: 0.28,
      outputCost: 0.83,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@hf/meta-llama/meta-llama-3-8b-instruct": model(
    workersAiModel({
      id: "@hf/meta-llama/meta-llama-3-8b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/meta/llama-3-8b-instruct-awq": model(
    workersAiModel({
      id: "@cf/meta/llama-3-8b-instruct-awq",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      inputCost: 0.12,
      outputCost: 0.27,
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/meta/llama-3-8b-instruct": model(
    workersAiModel({
      id: "@cf/meta/llama-3-8b-instruct",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      inputCost: 0.28,
      outputCost: 0.83,
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/meta/llama-guard-3-8b": model(
    workersAiModel({
      id: "@cf/meta/llama-guard-3-8b",
      displayProvider: providers.meta,
      family: "llama-guard",
      inputCost: 0.48,
      outputCost: 0.03,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/meta/llama-2-7b-chat-fp16": model(
    workersAiModel({
      id: "@cf/meta/llama-2-7b-chat-fp16",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      inputCost: 0.56,
      outputCost: 6.67,
      contextWindow: 4_096,
      maxTokens: 2_048,
    }),
  ),
  "workers-ai/@cf/meta/llama-2-7b-chat-int8": model(
    workersAiModel({
      id: "@cf/meta/llama-2-7b-chat-int8",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "deprecated",
      contextWindow: 4_096,
      maxTokens: 2_048,
    }),
  ),
  "workers-ai/@cf/meta-llama/llama-2-7b-chat-hf-lora": model(
    workersAiModel({
      id: "@cf/meta-llama/llama-2-7b-chat-hf-lora",
      displayProvider: providers.meta,
      family: "llama",
      lifecycle: "preview",
      contextWindow: 4_096,
      maxTokens: 2_048,
    }),
  ),
  "workers-ai/@cf/qwen/qwen3-30b-a3b-fp8": model(
    workersAiModel({
      id: "@cf/qwen/qwen3-30b-a3b-fp8",
      displayProvider: providers.qwen,
      family: "qwen",
      reasoning: true,
      inputCost: 0.051,
      outputCost: 0.34,
      contextWindow: 131_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/qwen/qwq-32b": model(
    workersAiModel({
      id: "@cf/qwen/qwq-32b",
      displayProvider: providers.qwen,
      family: "qwen",
      reasoning: true,
      inputCost: 0.66,
      outputCost: 1,
      contextWindow: 32_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/qwen/qwen2.5-coder-32b-instruct": model(
    workersAiModel({
      id: "@cf/qwen/qwen2.5-coder-32b-instruct",
      displayProvider: providers.qwen,
      family: "qwen",
      inputCost: 0.66,
      outputCost: 1,
      contextWindow: 32_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/deepseek-ai/deepseek-r1-distill-qwen-32b": model(
    workersAiModel({
      id: "@cf/deepseek-ai/deepseek-r1-distill-qwen-32b",
      displayProvider: providers.deepseek,
      family: "deepseek",
      reasoning: true,
      inputCost: 0.5,
      outputCost: 4.88,
      contextWindow: 32_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/google/gemma-4-26b-a4b-it": model(
    workersAiModel({
      id: "@cf/google/gemma-4-26b-a4b-it",
      displayProvider: providers.google,
      family: "gemma",
      reasoning: true,
      input: ["text", "image"],
      inputCost: 0.1,
      outputCost: 0.3,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/google/gemma-3-12b-it": model(
    workersAiModel({
      id: "@cf/google/gemma-3-12b-it",
      displayProvider: providers.google,
      family: "gemma",
      lifecycle: "deprecated",
      input: ["text", "image"],
      inputCost: 0.35,
      outputCost: 0.56,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/google/gemma-7b-it-lora": model(
    workersAiModel({
      id: "@cf/google/gemma-7b-it-lora",
      displayProvider: providers.google,
      family: "gemma",
      lifecycle: "preview",
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/google/gemma-2b-it-lora": model(
    workersAiModel({
      id: "@cf/google/gemma-2b-it-lora",
      displayProvider: providers.google,
      family: "gemma",
      lifecycle: "preview",
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@hf/google/gemma-7b-it": model(
    workersAiModel({
      id: "@hf/google/gemma-7b-it",
      displayProvider: providers.google,
      family: "gemma",
      lifecycle: "deprecated",
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/mistralai/mistral-small-3.1-24b-instruct": model(
    workersAiModel({
      id: "@cf/mistralai/mistral-small-3.1-24b-instruct",
      displayProvider: providers.mistral,
      family: "mistral",
      input: ["text", "image"],
      inputCost: 0.35,
      outputCost: 0.56,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@hf/mistral/mistral-7b-instruct-v0.2": model(
    workersAiModel({
      id: "@hf/mistral/mistral-7b-instruct-v0.2",
      displayProvider: providers.mistral,
      family: "mistral",
      lifecycle: "deprecated",
      contextWindow: 32_000,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/mistral/mistral-7b-instruct-v0.2-lora": model(
    workersAiModel({
      id: "@cf/mistral/mistral-7b-instruct-v0.2-lora",
      displayProvider: providers.mistral,
      family: "mistral",
      lifecycle: "preview",
      contextWindow: 32_000,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/mistral/mistral-7b-instruct-v0.1": model(
    workersAiModel({
      id: "@cf/mistral/mistral-7b-instruct-v0.1",
      displayProvider: providers.mistral,
      family: "mistral",
      lifecycle: "deprecated",
      inputCost: 0.11,
      outputCost: 0.19,
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/nvidia/nemotron-3-120b-a12b": model(
    workersAiModel({
      id: "@cf/nvidia/nemotron-3-120b-a12b",
      displayProvider: providers.nvidia,
      family: "nemotron",
      reasoning: true,
      inputCost: 0.5,
      outputCost: 1.5,
      contextWindow: 128_000,
      maxTokens: 16_384,
    }),
  ),
  "workers-ai/@cf/ibm-granite/granite-4.0-h-micro": model(
    workersAiModel({
      id: "@cf/ibm-granite/granite-4.0-h-micro",
      displayProvider: providers.ibm,
      family: "granite",
      inputCost: 0.017,
      outputCost: 0.11,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@cf/aisingapore/gemma-sea-lion-v4-27b-it": model(
    workersAiModel({
      id: "@cf/aisingapore/gemma-sea-lion-v4-27b-it",
      displayProvider: providers.aisingapore,
      family: "sea-lion",
      inputCost: 0.35,
      outputCost: 0.56,
      contextWindow: 128_000,
      maxTokens: 8_192,
    }),
  ),
  "workers-ai/@hf/nousresearch/hermes-2-pro-mistral-7b": model(
    workersAiModel({
      id: "@hf/nousresearch/hermes-2-pro-mistral-7b",
      displayProvider: providers.nousresearch,
      family: "hermes",
      lifecycle: "deprecated",
      contextWindow: 32_000,
      maxTokens: 4_096,
    }),
  ),
  "workers-ai/@cf/microsoft/phi-2": model(
    workersAiModel({
      id: "@cf/microsoft/phi-2",
      displayProvider: providers.microsoft,
      family: "phi",
      lifecycle: "deprecated",
      contextWindow: 2_048,
      maxTokens: 1_024,
    }),
  ),
  "workers-ai/@cf/defog/sqlcoder-7b-2": model(
    workersAiModel({
      id: "@cf/defog/sqlcoder-7b-2",
      displayProvider: providers.defog,
      family: "sqlcoder",
      lifecycle: "deprecated",
      contextWindow: 8_192,
      maxTokens: 4_096,
    }),
  ),
} as const satisfies Record<string, RegistryEntry>;

export const defaultModelId = "claude-sonnet-4-5";

export const defaultModel = models[defaultModelId].model;

export const registry = new Map<string, RegistryEntry>(Object.entries(models));

export const find = (modelId: string): RegistryEntry | undefined => registry.get(modelId);

export const isRegisteredModelId = (modelId: string): modelId is keyof typeof models =>
  find(modelId) !== undefined;

export const modelFor = (modelId: string): Model<Api> | undefined => find(modelId)?.model;

export const list = (): ReadonlyArray<RegistryEntry> => Array.from(registry.values());

export const modelsByFamily = (family: string): ReadonlyArray<RegistryEntry> =>
  list().filter((entry) => entry.catalog.family === family);

export const modelsByDisplayProvider = (providerId: string): ReadonlyArray<RegistryEntry> =>
  list().filter((entry) => entry.catalog.displayProvider.id === providerId);

export const catalogItemFor = (entry: RegistryEntry): AiModelCatalogItem => ({
  id: entry.model.id,
  name: entry.model.name,
  displayProvider: entry.catalog.displayProvider,
  family: entry.catalog.family,
  default: entry.model.id === defaultModelId,
  api: entry.model.api,
  capabilities: {
    reasoning: entry.model.reasoning,
    ...(entry.catalog.reasoningMode === undefined
      ? {}
      : { reasoningMode: entry.catalog.reasoningMode }),
    tools: true,
  },
  inputModalities: entry.catalog.modalities.input,
  outputModalities: entry.catalog.modalities.output,
  contextWindow: entry.model.contextWindow,
  maxTokens: entry.model.maxTokens,
  cost: entry.model.cost,
  lifecycle: entry.catalog.lifecycle,
});

export const listCatalogProviders = (): ReadonlyArray<AiModelProviderGroup> => {
  const grouped = new Map<string, { provider: DisplayProvider; models: AiModelCatalogItem[] }>();
  for (const entry of list()) {
    const item = catalogItemFor(entry);
    const provider = item.displayProvider;
    const existing = grouped.get(provider.id);
    if (existing === undefined) {
      grouped.set(provider.id, { provider, models: [item] });
    } else {
      existing.models.push(item);
    }
  }

  return Array.from(grouped.values())
    .map(({ provider, models }) => ({
      id: provider.id,
      name: provider.name,
      models: models.sort(compareCatalogItems),
    }))
    .sort(compareProviderGroups);
};

export const catalogResponse = (): AiModelCatalogResponse => ({
  defaultModelId,
  providers: listCatalogProviders(),
});

export const nonChatCatalogCategories = [
  "automatic-speech-recognition",
  "image-classification",
  "image-to-image",
  "image-to-text",
  "image-to-video",
  "music-generation",
  "object-detection",
  "summarization",
  "text-classification",
  "text-embeddings",
  "text-to-image",
  "text-to-speech",
  "text-to-video",
  "translation",
  "voice-activity-detection",
  "websocket",
] as const;

const compareProviderGroups = (a: AiModelProviderGroup, b: AiModelProviderGroup): number => {
  const aRank = providerSortRank[a.id] ?? 100;
  const bRank = providerSortRank[b.id] ?? 100;
  return aRank - bRank || a.name.localeCompare(b.name);
};

const compareCatalogItems = (a: AiModelCatalogItem, b: AiModelCatalogItem): number => {
  const lifecycleRank = lifecycleSortRank[a.lifecycle] - lifecycleSortRank[b.lifecycle];
  return lifecycleRank || a.name.localeCompare(b.name) || a.id.localeCompare(b.id);
};

const providerSortRank: Record<string, number> = {
  anthropic: 0,
  openai: 1,
  moonshotai: 2,
  zai: 3,
  meta: 4,
  qwen: 5,
  deepseek: 6,
  google: 7,
  mistral: 8,
};

const lifecycleSortRank: Record<CatalogLifecycle, number> = {
  stable: 0,
  preview: 1,
  deprecated: 2,
};

export * as CloudflareAiGatewayModels from "./CloudflareAiGatewayModels.ts";
