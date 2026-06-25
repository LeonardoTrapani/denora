import type { Api, Model } from "@earendil-works/pi-ai";

export type RuntimeApi = "anthropic-messages" | "openai-responses" | "openai-completions";

export interface RuntimeRoute {
  readonly provider: "anthropic" | "openai" | "workers-ai";
  readonly endpoint: string;
  readonly model: string;
}

export interface RegistryEntry<TApi extends RuntimeApi = RuntimeApi> {
  readonly model: Model<TApi>;
  readonly route: RuntimeRoute;
}

export type RegistryModel = (typeof models)[keyof typeof models]["model"];

const baseUrl =
  "https://gateway.ai.cloudflare.com/v1/{CLOUDFLARE_ACCOUNT_ID}/{CLOUDFLARE_GATEWAY_ID}";

export const models = {
  "claude-sonnet-4-5": {
    model: {
      id: "claude-sonnet-4-5",
      name: "Claude Sonnet 4.5",
      api: "anthropic-messages",
      provider: "cloudflare-ai-gateway",
      baseUrl: `${baseUrl}/anthropic`,
      compat: { sendSessionAffinityHeaders: true },
      reasoning: true,
      input: ["text", "image"],
      cost: { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
      contextWindow: 200_000,
      maxTokens: 64_000,
    } satisfies Model<"anthropic-messages">,
    route: { provider: "anthropic", endpoint: "v1/messages", model: "claude-sonnet-4-5" },
  },
  "gpt-5.1": {
    model: {
      id: "gpt-5.1",
      name: "GPT-5.1",
      api: "openai-responses",
      provider: "cloudflare-ai-gateway",
      baseUrl: `${baseUrl}/openai`,
      compat: { supportsDeveloperRole: true, sendSessionIdHeader: true },
      reasoning: true,
      thinkingLevelMap: { off: null },
      input: ["text", "image"],
      cost: { input: 1.25, output: 10, cacheRead: 0.13, cacheWrite: 0 },
      contextWindow: 400_000,
      maxTokens: 128_000,
    } satisfies Model<"openai-responses">,
    route: { provider: "openai", endpoint: "v1/responses", model: "gpt-5.1" },
  },
  "workers-ai/@cf/moonshotai/kimi-k2.6": {
    model: {
      id: "workers-ai/@cf/moonshotai/kimi-k2.6",
      name: "Kimi K2.6",
      api: "openai-completions",
      provider: "cloudflare-ai-gateway",
      baseUrl: `${baseUrl}/compat`,
      compat: {
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
      },
      reasoning: true,
      input: ["text", "image"],
      cost: { input: 0.95, output: 4, cacheRead: 0.16, cacheWrite: 0 },
      contextWindow: 256_000,
      maxTokens: 256_000,
    } satisfies Model<"openai-completions">,
    route: {
      provider: "workers-ai",
      endpoint: "@cf/moonshotai/kimi-k2.6",
      model: "@cf/moonshotai/kimi-k2.6",
    },
  },
} as const satisfies Record<string, RegistryEntry>;

export const defaultModelId = "claude-sonnet-4-5";

export const defaultModel = models[defaultModelId].model;

export const registry = new Map<string, RegistryEntry>(Object.entries(models));

export const find = (modelId: string): RegistryEntry | undefined => registry.get(modelId);

export const isRegisteredModelId = (modelId: string): modelId is keyof typeof models =>
  find(modelId) !== undefined;

export const modelFor = (modelId: string): Model<Api> | undefined => find(modelId)?.model;

export * as CloudflareAiGatewayModels from "./CloudflareAiGatewayModels.ts";
