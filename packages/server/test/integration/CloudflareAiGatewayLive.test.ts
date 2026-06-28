import type { AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessageEvent, Model, SimpleStreamOptions } from "@earendil-works/pi-ai";
import { Type } from "@earendil-works/pi-ai";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Test from "alchemy/Test/Vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as ManagedRuntime from "effect/ManagedRuntime";
import { assert, expect } from "vitest";
import { CloudflareAiGatewayModels } from "../../src/agent-loop/CloudflareAiGatewayModels.ts";
import { PiAgentModel } from "../../src/agent-loop/PiAgentModel.ts";
import { AgentRunSession, type RunEvent } from "../../src/agent-run/AgentRunSession.ts";

// Alchemy-effect live-test doctrine applied directly: opt-in only, bounded waits,
// test.provider, stack.destroy() before and after, and diagnostic entitlement
// handling by default. The test provisions an authenticated temporary AI Gateway
// instead of requiring caller-provided CLOUDFLARE_ACCOUNT_ID/CLOUDFLARE_GATEWAY_ID.
const LIVE_ENABLED = process.env.DENORA_LIVE_AI_GATEWAY === "1";
const STRICT_MODE = process.env.DENORA_LIVE_AI_GATEWAY_STRICT === "1";
const DEFAULT_TIMEOUT_MS = 60_000;
const TEST_TIMEOUT_MS = DEFAULT_TIMEOUT_MS + 5_000;
const GATEWAY_ID = `denora-live-ai-${crypto.randomUUID().slice(0, 8)}`;

const { test } = Test.make({ providers: Cloudflare.providers() });

interface LiveConfig {
  readonly accountId: string;
  readonly gatewayId: string;
  readonly token?: string | undefined;
  readonly baseUrl: string;
}

interface AiGatewayRunRequest {
  readonly provider: string;
  readonly endpoint: string;
  readonly headers?: Record<string, string | null | undefined> | undefined;
  readonly query: unknown;
  readonly config?: unknown;
}

interface AiGatewayRunOptions {
  readonly gateway?: { readonly id?: string | undefined } | undefined;
  readonly signal?: AbortSignal | undefined;
  readonly extraHeaders?: Record<string, string | null | undefined> | undefined;
}

type SmokeStatus = "done" | "provider-error" | "unexpected-error";

type LiveDiagnostic = SmokeResult | ToolSmokeResult;

interface SmokeResult {
  readonly modelId: string;
  readonly provider: string;
  readonly status: SmokeStatus;
  readonly eventCount: number;
  readonly textLength: number;
  readonly thinkingEvents: number;
  readonly errorMessage?: string | undefined;
  readonly stopReason?: string | undefined;
}

interface ToolSmokeResult extends SmokeResult {
  readonly toolStartCount: number;
  readonly toolResultCount: number;
  readonly echoedValue?: string | undefined;
}

const diagnostics: LiveDiagnostic[] = [];

const livePiAgentModelLayer = (config: LiveConfig): Layer.Layer<PiAgentModel.Service> =>
  PiAgentModel.layer().pipe(
    Layer.provide(
      Layer.succeed(
        PiAgentModel.AiGateway,
        PiAgentModel.AiGateway.of({
          id: PiAgentModel.AiGatewayId.make(config.gatewayId),
          gatewayRun: (request: AiGatewayRunRequest, options?: AiGatewayRunOptions) =>
            liveGatewayRun(config, request, options),
        }),
      ),
    ),
  );

const liveGatewayRun = (
  config: LiveConfig,
  request: AiGatewayRunRequest,
  options?: AiGatewayRunOptions,
): Promise<Response> => {
  const gatewayId = options?.gateway?.id ?? config.gatewayId;
  const url = `${config.baseUrl.replace(/\/+$/, "")}/v1/${encodeURIComponent(
    config.accountId,
  )}/${encodeURIComponent(gatewayId)}`;
  const headers: Record<string, string> = { "content-type": "application/json" };
  if (config.token !== undefined && config.token.length > 0) {
    headers["cf-aig-authorization"] = `Bearer ${config.token}`;
  }
  for (const [name, value] of Object.entries(options?.extraHeaders ?? {})) {
    if (value !== undefined && value !== null) headers[name] = value;
  }

  const init: RequestInit = {
    method: "POST",
    headers,
    body: JSON.stringify([request]),
  };
  if (options?.signal !== undefined) init.signal = options.signal;
  return fetch(url, init);
};

const collectEvents = async (
  stream: AsyncIterable<AssistantMessageEvent>,
): Promise<AssistantMessageEvent[]> => {
  const events: AssistantMessageEvent[] = [];
  for await (const event of stream) events.push(event);
  return events;
};

const smokeModel = (
  config: LiveConfig,
  entry: CloudflareAiGatewayModels.RegistryEntry,
  options: SimpleStreamOptions,
): Effect.Effect<SmokeResult> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort("live-ai-smoke-timeout"),
        DEFAULT_TIMEOUT_MS,
      );
      return { controller, timeout };
    }),
    ({ controller }) =>
      Effect.gen(function* () {
        const service = yield* PiAgentModel.Service;
        const stream = yield* service.stream({
          model: entry.model as Model<any>,
          context: {
            systemPrompt: "You are Denora's live AI Gateway smoke test. Reply tersely.",
            messages: [{ role: "user", content: "Reply with exactly: OK", timestamp: 1 }],
          },
          options: { ...options, signal: controller.signal },
        });
        const events = yield* Effect.promise(() => collectEvents(stream));
        return summarizeEvents(entry, events);
      }),
    ({ timeout }) => Effect.sync(() => clearTimeout(timeout)),
  ).pipe(
    Effect.catchCause((cause) =>
      Effect.succeed({
        modelId: entry.model.id,
        provider: entry.route.provider,
        status: "unexpected-error",
        eventCount: 0,
        textLength: 0,
        thinkingEvents: 0,
        errorMessage: causeMessage(cause),
      } satisfies SmokeResult),
    ),
    Effect.provide(livePiAgentModelLayer(config)),
  );

const causeMessage = (cause: Cause.Cause<unknown>): string => {
  const squashed = Cause.squash(cause);
  return squashed instanceof Error ? squashed.message : String(squashed);
};

const summarizeEvents = (
  entry: CloudflareAiGatewayModels.RegistryEntry,
  events: ReadonlyArray<AssistantMessageEvent>,
): SmokeResult => {
  const terminal = events.at(-1);
  const textLength = events.reduce(
    (sum, event) => sum + (event.type === "text_delta" ? event.delta.length : 0),
    0,
  );
  const thinkingEvents = events.filter((event) => event.type.startsWith("thinking_")).length;

  if (terminal?.type === "done") {
    return {
      modelId: entry.model.id,
      provider: entry.route.provider,
      status: "done",
      eventCount: events.length,
      textLength,
      thinkingEvents,
      stopReason: terminal.reason,
    };
  }
  if (terminal?.type === "error") {
    return {
      modelId: entry.model.id,
      provider: entry.route.provider,
      status: "provider-error",
      eventCount: events.length,
      textLength,
      thinkingEvents,
      stopReason: terminal.reason,
      errorMessage: terminal.error.errorMessage ?? "Provider returned an error event.",
    };
  }
  return {
    modelId: entry.model.id,
    provider: entry.route.provider,
    status: "unexpected-error",
    eventCount: events.length,
    textLength,
    thinkingEvents,
    errorMessage: `Stream ended without a terminal done/error event; last event was ${terminal?.type ?? "none"}.`,
  };
};

const recordAndAssertSmoke = (result: SmokeResult): void => {
  diagnostics.push(result);
  console.info(`[live-ai] ${result.modelId}: ${result.status}`, {
    provider: result.provider,
    eventCount: result.eventCount,
    textLength: result.textLength,
    thinkingEvents: result.thinkingEvents,
    stopReason: result.stopReason,
    errorMessage: result.errorMessage,
  });
  expect(["done", "provider-error"]).toContain(result.status);
  if (STRICT_MODE && result.status === "provider-error" && isModelOrProviderUnavailable(result)) {
    assert.fail(
      `${result.modelId} unavailable in strict live AI Gateway mode: ${result.errorMessage}`,
    );
  }
};

const isModelOrProviderUnavailable = (result: SmokeResult): boolean => {
  const message = result.errorMessage?.toLowerCase() ?? "";
  return [
    "model",
    "provider",
    "not found",
    "not available",
    "unavailable",
    "unsupported",
    "not enabled",
    "unknown model",
    "does not have access",
  ].some((needle) => message.includes(needle));
};

const registryCases = () =>
  CloudflareAiGatewayModels.list().map((entry) => ({
    name: entry.model.id,
    entry,
    options: catalogOptionsFor(entry),
  }));

const catalogOptionsFor = (
  entry: CloudflareAiGatewayModels.RegistryEntry,
): SimpleStreamOptions => ({
  maxTokens: entry.route.provider === "anthropic" ? 64 : 32,
  ...(entry.model.reasoning ? { reasoning: "minimal" as const } : {}),
});

const echoTool = {
  name: "echo_status",
  label: "Echo status",
  description: "Echoes the supplied status value back to the model.",
  parameters: Type.Object({ value: Type.String() }),
  execute: async (_toolCallId, rawParams) => {
    const params = rawParams as { readonly value: string };
    return {
      content: [{ type: "text", text: `echo:${params.value}` }],
      details: { value: params.value },
    };
  },
} satisfies AgentTool<any, { readonly value: string }>;

const toolSmokeModel = (
  config: LiveConfig,
  entry: CloudflareAiGatewayModels.RegistryEntry,
): Effect.Effect<ToolSmokeResult> =>
  Effect.acquireUseRelease(
    Effect.sync(() => {
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort("live-ai-tool-smoke-timeout"),
        DEFAULT_TIMEOUT_MS,
      );
      return { controller, timeout };
    }),
    ({ controller }) =>
      Effect.gen(function* () {
        const service = yield* PiAgentModel.Service;
        const runtime = ManagedRuntime.make(Layer.succeed(PiAgentModel.Service, service));
        const runEvents: RunEvent[] = [];
        const result = yield* AgentRunSession.execute({
          runId: `live_tool_${entry.model.id.replace(/[^a-zA-Z0-9_-]/g, "_")}`,
          input: {
            model: entry.model,
            thinkingLevel: entry.model.reasoning ? "minimal" : undefined,
            systemPrompt:
              "You are Denora's live AI Gateway tool smoke test. You must use available tools when instructed.",
            prompt:
              'Call the echo_status tool exactly once with {"value":"OK"}. After you receive the tool result, answer exactly: TOOL OK',
          },
          tools: [echoTool],
          streamFn: (model, context, options) =>
            runtime.runPromise(
              Effect.gen(function* () {
                const liveService = yield* PiAgentModel.Service;
                return yield* liveService.stream({
                  model,
                  context,
                  options: { ...options, signal: controller.signal },
                });
              }),
            ),
          onAgentEvent: (event) =>
            Effect.sync(() => {
              runEvents.push(event);
            }),
          signal: controller.signal,
        });
        return summarizeToolEvents(entry, runEvents, result.assistantText);
      }),
    ({ timeout }) => Effect.sync(() => clearTimeout(timeout)),
  ).pipe(
    Effect.catchCause((cause) =>
      Effect.succeed({
        modelId: entry.model.id,
        provider: entry.route.provider,
        status: "unexpected-error",
        eventCount: 0,
        textLength: 0,
        thinkingEvents: 0,
        toolStartCount: 0,
        toolResultCount: 0,
        errorMessage: causeMessage(cause),
      } satisfies ToolSmokeResult),
    ),
    Effect.provide(livePiAgentModelLayer(config)),
  );

const summarizeToolEvents = (
  entry: CloudflareAiGatewayModels.RegistryEntry,
  events: ReadonlyArray<RunEvent>,
  assistantText: string,
): ToolSmokeResult => {
  const toolStartCount = events.filter(
    (event) => eventType(event) === "tool_start" && eventField(event, "toolName") === "echo_status",
  ).length;
  const toolResultEvents = events.filter(
    (event) => eventType(event) === "tool" && eventField(event, "toolName") === "echo_status",
  );
  const errorTurn = events.find(
    (event) => eventType(event) === "turn" && eventField(event, "isError") === true,
  );
  const base = {
    modelId: entry.model.id,
    provider: entry.route.provider,
    eventCount: events.length,
    textLength: assistantText.length,
    thinkingEvents: events.filter((event) => eventType(event).startsWith("thinking_")).length,
    toolStartCount,
    toolResultCount: toolResultEvents.length,
    echoedValue: echoedToolValue(toolResultEvents),
  };

  if (errorTurn !== undefined) {
    return {
      ...base,
      status: "provider-error",
      errorMessage: errorMessageFromTurn(errorTurn) ?? "Provider returned an error turn.",
    };
  }
  if (toolResultEvents.length === 0) {
    return {
      ...base,
      status: "unexpected-error",
      errorMessage: "Model completed without calling echo_status.",
    };
  }
  return { ...base, status: "done", stopReason: "stop" };
};

const eventType = (event: RunEvent): string => String(eventField(event, "type") ?? "");

const eventField = (event: RunEvent, field: string): unknown =>
  (event as unknown as Record<string, unknown>)[field];

const errorMessageFromTurn = (event: RunEvent): string | undefined => {
  const response = eventField(event, "response");
  if (typeof response !== "object" || response === null) return undefined;
  const error = (response as Record<string, unknown>).error;
  if (typeof error !== "object" || error === null) return undefined;
  const message = (error as Record<string, unknown>).message;
  return typeof message === "string" ? message : undefined;
};

const echoedToolValue = (events: ReadonlyArray<RunEvent>): string | undefined => {
  for (const event of events) {
    const result = eventField(event, "result");
    if (typeof result !== "object" || result === null) continue;
    const details = (result as Record<string, unknown>).details;
    if (typeof details !== "object" || details === null) continue;
    const value = (details as Record<string, unknown>).value;
    if (typeof value === "string") return value;
  }
  return undefined;
};

const recordAndAssertToolSmoke = (result: ToolSmokeResult): void => {
  diagnostics.push(result);
  console.info(`[live-ai-tool] ${result.modelId}: ${result.status}`, {
    provider: result.provider,
    eventCount: result.eventCount,
    textLength: result.textLength,
    thinkingEvents: result.thinkingEvents,
    toolStartCount: result.toolStartCount,
    toolResultCount: result.toolResultCount,
    echoedValue: result.echoedValue,
    errorMessage: result.errorMessage,
  });
  expect(["done", "provider-error"]).toContain(result.status);
  if (result.status === "done") {
    expect(result.toolStartCount).toBeGreaterThan(0);
    expect(result.toolResultCount).toBeGreaterThan(0);
    expect(result.echoedValue).toBe("OK");
  }
  if (STRICT_MODE && result.status === "provider-error" && isModelOrProviderUnavailable(result)) {
    assert.fail(
      `${result.modelId} unavailable in strict live AI Gateway tool mode: ${result.errorMessage}`,
    );
  }
};

const deployedGatewayConfig = (gateway: {
  readonly accountId: string;
  readonly gatewayId: string;
}): LiveConfig => ({
  accountId: gateway.accountId,
  gatewayId: gateway.gatewayId,
  token: process.env.DENORA_AI_GATEWAY_TOKEN ?? process.env.CLOUDFLARE_API_TOKEN,
  baseUrl: process.env.DENORA_AI_GATEWAY_BASE_URL ?? "https://gateway.ai.cloudflare.com",
});

test.provider.skipIf(!LIVE_ENABLED)(
  "provisions a temporary AI Gateway and smokes every registry model plus echo tool use",
  (stack) =>
    Effect.gen(function* () {
      yield* stack.destroy();

      const gateway = yield* stack.deploy(
        Cloudflare.AiGateway("DenoraLiveAiGateway", {
          id: GATEWAY_ID,
          authentication: true,
          collectLogs: true,
        }),
      );
      const config = deployedGatewayConfig(gateway);

      for (const { entry, options } of registryCases()) {
        const result = yield* smokeModel(config, entry, options);
        recordAndAssertSmoke(result);
      }

      for (const { entry } of registryCases()) {
        const result = yield* toolSmokeModel(config, entry);
        recordAndAssertToolSmoke(result);
      }

      if (diagnostics.length > 0) {
        console.info("[live-ai] diagnostics", JSON.stringify(diagnostics, null, 2));
      }
    }).pipe(Effect.ensuring(stack.destroy().pipe(Effect.catch(() => Effect.void)))),
  { timeout: TEST_TIMEOUT_MS * Math.max(1, registryCases().length * 2) },
);
