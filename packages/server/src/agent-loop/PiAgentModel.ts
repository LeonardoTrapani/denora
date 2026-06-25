import {
  createAssistantMessageEventStream,
  parseStreamingJson,
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context as PiContext,
  type ImageContent,
  type Model,
  type OpenAICompletionsCompat,
  type SimpleStreamOptions,
  type StopReason,
  type TextContent,
  type ThinkingContent,
  type Tool,
  type ToolCall,
  type Usage,
} from "@earendil-works/pi-ai";
import { convertMessages } from "@earendil-works/pi-ai/openai-completions";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

type WorkersAiRunBinding = Effect.Success<Cloudflare.AiGatewayClient["raw"]>;
type AiGatewayRunBinding = Effect.Success<Cloudflare.AiGatewayClient["gateway"]>["run"];
type WorkersAIReasoningEffort = "low" | "medium" | "high";
type ProviderHeaders = Record<string, string | null>;
type StreamingTextBlock = TextContent;
type StreamingThinkingBlock = ThinkingContent;
type StreamingToolCallBlock = ToolCall & { partialArgs?: string; streamIndex?: number };
type StreamingBlock = StreamingTextBlock | StreamingThinkingBlock | StreamingToolCallBlock;
type SseChunkHandler = (data: string) => Effect.Effect<void, ModelStreamFailed>;
type SseReaderReadResult =
  | { readonly done: true; readonly value?: unknown }
  | { readonly done: false; readonly value: unknown };
type SseReader = {
  readonly read: () => PromiseLike<SseReaderReadResult>;
  readonly cancel: () => PromiseLike<void>;
  readonly releaseLock: () => void;
};
type SseReaderState = {
  readonly reader: SseReader;
  readonly decoder: TextDecoder;
  buffer: string;
  finished: boolean;
};
type ModelTurnError = InvalidPiModel | ModelCallFailed | ModelStreamFailed;

interface WorkersAiTool {
  readonly type: "function";
  readonly function: {
    readonly name: string;
    readonly description?: string | undefined;
    readonly parameters: unknown;
    readonly strict?: boolean | undefined;
  };
}

interface AnthropicTool {
  readonly name: string;
  readonly description?: string | undefined;
  readonly input_schema: {
    readonly type: "object";
    readonly properties: unknown;
    readonly required?: ReadonlyArray<string> | undefined;
  };
}

type AnthropicContentBlock =
  | { readonly type: "text"; readonly text: string }
  | {
      readonly type: "image";
      readonly source: {
        readonly type: "base64";
        readonly media_type: string;
        readonly data: string;
      };
    }
  | { readonly type: "thinking"; readonly thinking: string; readonly signature: string }
  | {
      readonly type: "tool_use";
      readonly id: string;
      readonly name: string;
      readonly input: unknown;
    }
  | {
      readonly type: "tool_result";
      readonly tool_use_id: string;
      readonly content: string | ReadonlyArray<AnthropicContentBlock>;
      readonly is_error: boolean;
    };

interface AnthropicMessage {
  readonly role: "user" | "assistant";
  readonly content: string | ReadonlyArray<AnthropicContentBlock>;
}

interface WorkersAiUsage {
  readonly prompt_tokens?: number | undefined;
  readonly completion_tokens?: number | undefined;
  readonly total_tokens?: number | undefined;
  readonly input_tokens?: number | undefined;
  readonly output_tokens?: number | undefined;
  readonly cache_read_input_tokens?: number | undefined;
  readonly cache_creation_input_tokens?: number | undefined;
  readonly cache_read_tokens?: number | undefined;
  readonly cache_write_tokens?: number | undefined;
  readonly prompt_cache_hit_tokens?: number | undefined;
  readonly prompt_tokens_details?:
    | {
        readonly cached_tokens?: number | undefined;
        readonly cache_write_tokens?: number | undefined;
      }
    | undefined;
}

const WORKERS_AI_COMPAT: Required<Omit<OpenAICompletionsCompat, "cacheControlFormat">> & {
  readonly cacheControlFormat: OpenAICompletionsCompat["cacheControlFormat"] | undefined;
} = {
  supportsStore: false,
  supportsDeveloperRole: false,
  supportsReasoningEffort: true,
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
  supportsStrictMode: true,
  cacheControlFormat: undefined,
  sendSessionAffinityHeaders: true,
  supportsLongCacheRetention: false,
};

export const AiGatewayId = Schema.String.pipe(Schema.brand("AiGatewayId"));
export type AiGatewayId = typeof AiGatewayId.Type;

export const WorkersAiModelId = Schema.String.pipe(Schema.brand("WorkersAiModelId"));
export type WorkersAiModelId = typeof WorkersAiModelId.Type;

export interface AiGatewayModelOptions {
  readonly defaultModelId?: string | undefined;
  readonly maxTokens?: number | undefined;
  readonly temperature?: number | undefined;
}

export interface StreamInput {
  readonly model: Model<Api>;
  readonly context: PiContext;
  readonly options?: SimpleStreamOptions | undefined;
}

/**
 * Provider adapter boundary: Denora sends Pi-shaped messages/tools to
 * Cloudflare AI Gateway and converts raw Workers AI streams directly back into
 * Pi AssistantMessageEventStream events.
 */
export interface Interface {
  readonly stream: (input: StreamInput) => Effect.Effect<AssistantMessageEventStream>;
}

export interface AiGatewayRuntime {
  readonly ai: WorkersAiRunBinding;
  readonly gatewayRun: AiGatewayRunBinding;
  readonly id: AiGatewayId;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/PiAgentModel") {}

export class AiGateway extends Context.Service<AiGateway, AiGatewayRuntime>()(
  "@denora/server/PiAgentModel/AiGateway",
) {}

export class InvalidPiModel extends Schema.TaggedErrorClass<InvalidPiModel>()("InvalidPiModel", {
  message: Schema.String,
  api: Schema.String,
}) {}

export class ModelCallFailed extends Schema.TaggedErrorClass<ModelCallFailed>()("ModelCallFailed", {
  message: Schema.String,
  cause: Schema.Defect(),
}) {}

export class ModelStreamFailed extends Schema.TaggedErrorClass<ModelStreamFailed>()(
  "ModelStreamFailed",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export const layer = (config: AiGatewayModelOptions = {}) =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const aiGateway = yield* AiGateway;
      const ai = aiGateway.ai;
      const gatewayRun = aiGateway.gatewayRun;
      const gatewayId = aiGateway.id;

      const stream = Effect.fn("PiAgentModel.stream")(function* ({
        model,
        context,
        options,
      }: StreamInput) {
        const requestedModel = WorkersAiModelId.make(config.defaultModelId ?? model.id);
        const eventStream = createAssistantMessageEventStream();
        const output = makeAssistantMessage({
          model,
          modelId: requestedModel,
          content: [],
          stopReason: "stop",
        });
        const maxTokens = options?.maxTokens ?? config.maxTokens;
        const temperature = options?.temperature ?? config.temperature;

        const turnOptions = {
          ...options,
          ...(maxTokens !== undefined ? { maxTokens } : {}),
          ...(temperature !== undefined ? { temperature } : {}),
        };

        const turn = isAnthropicRequest(model, requestedModel)
          ? runAnthropicTurn({
              gatewayRun,
              gatewayId,
              requestedModel,
              context,
              model,
              options: turnOptions,
              stream: eventStream,
              output,
            })
          : runWorkersAiTurn({
              ai,
              gatewayId,
              requestedModel,
              context,
              model,
              options: turnOptions,
              stream: eventStream,
              output,
            });

        yield* turn.pipe(
          Effect.catch((error) =>
            Effect.sync(() => emitTurnError({ output, stream: eventStream, options, error })),
          ),
          Effect.forkDetach,
        );

        return eventStream;
      });

      return Service.of({ stream });
    }),
  );

export const aiGatewayLayerFromClient = (client: Cloudflare.AiGatewayClient) =>
  Layer.effect(
    AiGateway,
    Effect.gen(function* () {
      const ai = yield* client.raw;
      const gateway = yield* client.gateway;
      const id = AiGatewayId.make(yield* client.id);
      return AiGateway.of({ ai, gatewayRun: gateway.run.bind(gateway) as AiGatewayRunBinding, id });
    }),
  );

const runAnthropicTurn = Effect.fn("PiAgentModel.runAnthropicTurn")(function* (input: {
  readonly gatewayRun: AiGatewayRunBinding;
  readonly gatewayId: AiGatewayId;
  readonly requestedModel: string;
  readonly context: PiContext;
  readonly model: Model<Api>;
  readonly options: SimpleStreamOptions | undefined;
  readonly stream: AssistantMessageEventStream;
  readonly output: AssistantMessage;
}): Effect.fn.Return<void, ModelTurnError> {
  const response = yield* callAnthropic({ ...input });
  return yield* processAnthropicResponse({ ...input, response }).pipe(
    Effect.catch((error) =>
      cancelResponseBody(response).pipe(Effect.flatMap(() => Effect.fail(error))),
    ),
  );
});

const processAnthropicResponse = Effect.fn("PiAgentModel.processAnthropicResponse")(
  function* (input: {
    readonly response: Response;
    readonly context: PiContext;
    readonly options: SimpleStreamOptions | undefined;
    readonly stream: AssistantMessageEventStream;
    readonly output: AssistantMessage;
  }): Effect.fn.Return<void, ModelStreamFailed> {
    yield* tryStreamSync(() => input.stream.push({ type: "start", partial: input.output }));
    const parser = new WorkersAiStreamParser({
      output: input.output,
      stream: input.stream,
      hasTools: (input.context.tools?.length ?? 0) > 0,
    });
    const body = input.response.body;
    if (body !== null) {
      yield* readSseChunks(body, (data) => parser.handleData(normalizeAnthropicStreamData(data)));
    }
    if (input.options?.signal?.aborted) {
      return yield* streamFailure(new Error("Request was aborted"));
    }
    yield* parser.finalize();
  },
);

const runWorkersAiTurn = Effect.fn("PiAgentModel.runWorkersAiTurn")(function* (input: {
  readonly ai: WorkersAiRunBinding;
  readonly gatewayId: AiGatewayId;
  readonly requestedModel: WorkersAiModelId;
  readonly context: PiContext;
  readonly model: Model<Api>;
  readonly options: SimpleStreamOptions | undefined;
  readonly stream: AssistantMessageEventStream;
  readonly output: AssistantMessage;
}): Effect.fn.Return<void, ModelTurnError> {
  const model = yield* requireOpenAiCompletionsModel(input.model);
  const response = yield* callWorkersAi({ ...input, model });
  return yield* processWorkersAiResponse({ ...input, model, response }).pipe(
    Effect.catch((error) =>
      cancelResponseBody(response).pipe(Effect.flatMap(() => Effect.fail(error))),
    ),
  );
});

const processWorkersAiResponse = Effect.fn("PiAgentModel.processWorkersAiResponse")(
  function* (input: {
    readonly response: Response;
    readonly model: Model<"openai-completions">;
    readonly context: PiContext;
    readonly options: SimpleStreamOptions | undefined;
    readonly stream: AssistantMessageEventStream;
    readonly output: AssistantMessage;
  }): Effect.fn.Return<void, ModelStreamFailed> {
    yield* tryStreamSync(() => input.stream.push({ type: "start", partial: input.output }));
    const parser = new WorkersAiStreamParser({
      output: input.output,
      stream: input.stream,
      hasTools: (input.context.tools?.length ?? 0) > 0,
    });
    const body = input.response.body;
    if (body !== null) yield* readSseChunks(body, (data) => parser.handleData(data));
    if (input.options?.signal?.aborted) {
      return yield* streamFailure(new Error("Request was aborted"));
    }
    yield* parser.finalize();
  },
);

const callWorkersAi = Effect.fn("PiAgentModel.callWorkersAi")(function* ({
  ai,
  gatewayId,
  requestedModel,
  context,
  model,
  options,
}: {
  readonly ai: WorkersAiRunBinding;
  readonly gatewayId: AiGatewayId;
  readonly requestedModel: WorkersAiModelId;
  readonly context: PiContext;
  readonly model: Model<"openai-completions">;
  readonly options: SimpleStreamOptions | undefined;
}): Effect.fn.Return<Response, ModelCallFailed> {
  const payload: Record<string, unknown> = {
    messages: convertMessages(model, context, WORKERS_AI_COMPAT),
    stream: true,
    stream_options: { include_usage: true },
  };
  if (context.tools && context.tools.length > 0) {
    payload.tools = context.tools.map(toWorkersAiTool);
    payload.tool_choice = "auto";
  }
  if (options?.maxTokens !== undefined) payload.max_tokens = options.maxTokens;
  if (options?.temperature !== undefined) payload.temperature = options.temperature;
  applyReasoningEffort(payload, model, options?.reasoning);

  const overridden = yield* Effect.tryPromise({
    try: () => Promise.resolve(options?.onPayload?.(payload, model)),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway payload hook failed.",
        cause,
      }),
  });
  const finalPayload = overridden === undefined ? payload : (overridden as Record<string, unknown>);
  const response = yield* Effect.tryPromise({
    try: () =>
      ai.run(
        requestedModel as never,
        finalPayload as never,
        {
          gateway: { id: gatewayId },
          returnRawResponse: true,
          ...(options?.signal ? { signal: options.signal } : {}),
          ...extraHeadersOption(options),
        } as never,
      ),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway model call failed.",
        cause,
      }),
  });
  if (!(response instanceof globalThis.Response)) {
    return yield* new ModelCallFailed({
      message: "Cloudflare AI Gateway did not return a raw Response.",
      cause: response,
    });
  }

  yield* Effect.tryPromise({
    try: () =>
      Promise.resolve(
        options?.onResponse?.(
          { status: response.status, headers: headersToRecord(response.headers) },
          model,
        ),
      ),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway response hook failed.",
        cause,
      }),
  });
  if (!response.ok) {
    const errorBody = yield* safeReadText(response);
    return yield* new ModelCallFailed({
      message:
        `Cloudflare AI Gateway returned ${response.status} ${response.statusText}` +
        (errorBody ? `: ${errorBody}` : ""),
      cause: response,
    });
  }
  return response;
});

const callAnthropic = Effect.fn("PiAgentModel.callAnthropic")(function* ({
  gatewayRun,
  gatewayId,
  requestedModel,
  context,
  model,
  options,
}: {
  readonly gatewayRun: AiGatewayRunBinding;
  readonly gatewayId: AiGatewayId;
  readonly requestedModel: string;
  readonly context: PiContext;
  readonly model: Model<Api>;
  readonly options: SimpleStreamOptions | undefined;
}): Effect.fn.Return<Response, ModelCallFailed> {
  const payload: Record<string, unknown> = {
    model: requestedModel,
    messages: toAnthropicMessages(context),
    max_tokens: options?.maxTokens ?? model.maxTokens,
    stream: true,
  };
  if (context.systemPrompt) payload.system = context.systemPrompt;
  if (options?.temperature !== undefined) payload.temperature = options.temperature;
  if (context.tools && context.tools.length > 0) payload.tools = context.tools.map(toAnthropicTool);

  const overridden = yield* Effect.tryPromise({
    try: () => Promise.resolve(options?.onPayload?.(payload, model)),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway Anthropic payload hook failed.",
        cause,
      }),
  });
  const finalPayload = overridden === undefined ? payload : overridden;
  const headers = mergeHeaders(
    {
      "content-type": "application/json",
      "anthropic-version": "2023-06-01",
    },
    options?.apiKey === undefined ? undefined : { "x-api-key": options.apiKey },
    options?.headers,
  );

  const response = yield* Effect.tryPromise({
    try: () =>
      gatewayRun(
        {
          provider: "anthropic",
          endpoint: "v1/messages",
          headers,
          query: finalPayload,
        } as never,
        {
          gateway: { id: gatewayId },
          ...(options?.signal ? { signal: options.signal } : {}),
          ...extraHeadersOption(options),
        } as never,
      ),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway Anthropic model call failed.",
        cause,
      }),
  });
  if (!(response instanceof globalThis.Response)) {
    return yield* new ModelCallFailed({
      message: "Cloudflare AI Gateway Anthropic call did not return a Response.",
      cause: response,
    });
  }

  yield* Effect.tryPromise({
    try: () =>
      Promise.resolve(
        options?.onResponse?.(
          { status: response.status, headers: headersToRecord(response.headers) },
          model,
        ),
      ),
    catch: (cause) =>
      new ModelCallFailed({
        message: errorMessage(cause) || "Cloudflare AI Gateway Anthropic response hook failed.",
        cause,
      }),
  });
  if (!response.ok) {
    const errorBody = yield* safeReadText(response);
    return yield* new ModelCallFailed({
      message:
        `Cloudflare AI Gateway returned ${response.status} ${response.statusText}` +
        (errorBody ? `: ${errorBody}` : ""),
      cause: response,
    });
  }
  return response;
});

class WorkersAiStreamParser {
  private textBlock: StreamingTextBlock | null = null;
  private thinkingBlock: StreamingThinkingBlock | null = null;
  private readonly toolCallBlocksByIndex = new Map<number, StreamingToolCallBlock>();
  private readonly toolCallBlocksById = new Map<string, StreamingToolCallBlock>();
  private usage: WorkersAiUsage | undefined;
  private finishReason: string | undefined;
  private receivedDone = false;
  private nativeToolBuffer = "";
  private nativeToolId: string | undefined;
  private readonly input: {
    readonly output: AssistantMessage;
    readonly stream: AssistantMessageEventStream;
    readonly hasTools: boolean;
  };

  constructor(input: {
    readonly output: AssistantMessage;
    readonly stream: AssistantMessageEventStream;
    readonly hasTools: boolean;
  }) {
    this.input = input;
  }

  handleData(data: string): Effect.Effect<void, ModelStreamFailed> {
    return tryStreamSync(() => {
      if (data.length === 0) return;
      if (data === "[DONE]") {
        this.receivedDone = true;
        return;
      }
      const chunk = parseChunk(data);
      this.updateMetadata(chunk);
      if (hasOpenAiDelta(chunk)) {
        this.handleOpenAiDelta(chunk);
        return;
      }
      this.handleNativeText(chunk);
      this.handleNativeToolCalls(chunk);
    });
  }

  finalize(): Effect.Effect<void, ModelStreamFailed> {
    return tryStreamSync(() => {
      this.closeToolCalls();
      this.closeThinkingBlock();
      this.closeTextBlock();
      this.flushNativeToolBuffer();

      const finalReason = this.finalReason();
      if (finalReason === undefined) {
        throw new Error("Stream ended without [DONE] or finish_reason");
      }

      const mapped = mapFinishReason(finalReason, hasToolCalls(this.input.output));
      this.input.output.stopReason = mapped.stopReason;
      if (mapped.errorMessage) this.input.output.errorMessage = mapped.errorMessage;
      if (this.usage !== undefined) this.input.output.usage = toUsage(this.usage);

      cleanupStreamingScratch(this.input.output);
      if (this.input.output.stopReason === "error") {
        this.input.stream.push({ type: "error", reason: "error", error: this.input.output });
      } else {
        this.input.stream.push({
          type: "done",
          reason: doneReason(this.input.output.stopReason),
          message: this.input.output,
        });
      }
      this.input.stream.end();
    });
  }

  private updateMetadata(chunk: Record<string, unknown>): void {
    const responseId = stringField(chunk, "id");
    if (!this.input.output.responseId && responseId) this.input.output.responseId = responseId;
    const responseModel = stringField(chunk, "model");
    if (
      !this.input.output.responseModel &&
      responseModel &&
      responseModel !== this.input.output.model
    ) {
      this.input.output.responseModel = responseModel;
    }
    this.updateUsage(chunk.usage);

    const choices = optionalArrayField(chunk, "choices");
    if (choices !== undefined) {
      const choice = objectAt(choices, 0);
      if (choice !== undefined) {
        this.updateUsage(choice.usage);
        const finish = nullableStringField(choice, "finish_reason");
        if (finish !== undefined && finish !== null) this.finishReason = finish;
      }
    }
    const finish = nullableStringField(chunk, "finish_reason");
    if (finish !== undefined && finish !== null) this.finishReason = finish;
  }

  private updateUsage(raw: unknown): void {
    const usage = optionalUsage(raw);
    if (usage === undefined) return;
    if (hasNonZeroUsage(usage) && this.usage !== undefined && hasNonZeroUsage(this.usage)) {
      this.usage = mergeUsage(this.usage, usage);
      return;
    }
    if (hasNonZeroUsage(usage) || this.usage === undefined || !hasNonZeroUsage(this.usage)) {
      this.usage = usage;
    }
  }

  private handleNativeText(chunk: Record<string, unknown>): void {
    if (!("response" in chunk)) return;
    const native = chunk.response;
    if (native === null || native === undefined || native === "") return;
    const text = typeof native === "object" ? JSON.stringify(native) : String(native);
    if (text.length === 0) return;
    if (this.input.hasTools) {
      this.nativeToolId ??= makeGeneratedToolCallId();
      this.nativeToolBuffer += text;
      return;
    }
    const block = this.ensureTextBlock();
    block.text += text;
    this.input.stream.push({
      type: "text_delta",
      contentIndex: this.contentIndex(block),
      delta: text,
      partial: this.input.output,
    });
  }

  private handleNativeToolCalls(chunk: Record<string, unknown>): void {
    const toolCalls = optionalArrayField(chunk, "tool_calls");
    if (toolCalls === undefined) return;
    this.closeThinkingBlock();
    this.handleToolDeltas(toolCalls);
  }

  private handleOpenAiDelta(chunk: Record<string, unknown>): void {
    const choices = optionalArrayField(chunk, "choices");
    if (choices === undefined) return;
    const choice = objectAt(choices, 0);
    if (choice === undefined) return;
    const delta = optionalObjectField(choice, "delta");
    if (delta === undefined) return;

    const reasoning = pickReasoning(delta);
    if (reasoning !== null) {
      const block = this.ensureThinkingBlock(reasoning.field);
      block.thinking += reasoning.text;
      this.input.stream.push({
        type: "thinking_delta",
        contentIndex: this.contentIndex(block),
        delta: reasoning.text,
        partial: this.input.output,
      });
    }

    const content = nullableStringField(delta, "content");
    if (content !== undefined && content !== null && content.length > 0) {
      this.closeThinkingBlock();
      const block = this.ensureTextBlock();
      block.text += content;
      this.input.stream.push({
        type: "text_delta",
        contentIndex: this.contentIndex(block),
        delta: content,
        partial: this.input.output,
      });
    }

    const toolCalls = optionalArrayField(delta, "tool_calls");
    if (toolCalls !== undefined) {
      this.closeThinkingBlock();
      this.handleToolDeltas(toolCalls);
    }
  }

  private handleToolDeltas(rawToolCalls: ReadonlyArray<unknown>): void {
    rawToolCalls.forEach((raw, fallbackIndex) => {
      const toolCall = requireObject(raw, "tool call");
      if (isNullFinalizationToolCall(toolCall)) return;
      const streamIndex = optionalNumberField(toolCall, "index") ?? fallbackIndex;
      const block = this.ensureToolCallBlock(toolCall, streamIndex);
      const delta = toolCallArgumentsDelta(toolCall);
      if (delta.length > 0) {
        block.partialArgs = (block.partialArgs ?? "") + delta;
        block.arguments = parseStreamingJson<Record<string, unknown>>(block.partialArgs);
      }
      this.input.stream.push({
        type: "toolcall_delta",
        contentIndex: this.contentIndex(block),
        delta,
        partial: this.input.output,
      });
    });
  }

  private ensureTextBlock(): StreamingTextBlock {
    if (this.textBlock === null) {
      this.textBlock = { type: "text", text: "" };
      this.blocks.push(this.textBlock);
      this.input.stream.push({
        type: "text_start",
        contentIndex: this.contentIndex(this.textBlock),
        partial: this.input.output,
      });
    }
    return this.textBlock;
  }

  private ensureThinkingBlock(thinkingSignature: string): StreamingThinkingBlock {
    if (this.thinkingBlock === null) {
      this.thinkingBlock = { type: "thinking", thinking: "", thinkingSignature };
      this.blocks.push(this.thinkingBlock);
      this.input.stream.push({
        type: "thinking_start",
        contentIndex: this.contentIndex(this.thinkingBlock),
        partial: this.input.output,
      });
    }
    return this.thinkingBlock;
  }

  private ensureToolCallBlock(
    toolCall: Record<string, unknown>,
    fallbackIndex: number,
  ): StreamingToolCallBlock {
    const streamIndex = optionalNumberField(toolCall, "index") ?? fallbackIndex;
    const id = stringField(toolCall, "id");
    let block = this.toolCallBlocksByIndex.get(streamIndex);
    if (!block && id) block = this.toolCallBlocksById.get(id);
    if (!block) {
      block = {
        type: "toolCall",
        id: id || makeGeneratedToolCallId(),
        name: toolCallName(toolCall),
        arguments: {},
        partialArgs: "",
        streamIndex,
      };
      this.toolCallBlocksByIndex.set(streamIndex, block);
      this.toolCallBlocksById.set(block.id, block);
      this.blocks.push(block);
      this.input.stream.push({
        type: "toolcall_start",
        contentIndex: this.contentIndex(block),
        partial: this.input.output,
      });
    }
    if (id && block.id !== id) {
      this.toolCallBlocksById.delete(block.id);
      block.id = id;
      this.toolCallBlocksById.set(id, block);
    }
    const name = toolCallName(toolCall);
    if (!block.name && name) block.name = name;
    return block;
  }

  private closeTextBlock(): void {
    if (this.textBlock === null) return;
    const block = this.textBlock;
    this.textBlock = null;
    this.input.stream.push({
      type: "text_end",
      contentIndex: this.contentIndex(block),
      content: block.text,
      partial: this.input.output,
    });
  }

  private closeThinkingBlock(): void {
    if (this.thinkingBlock === null) return;
    const block = this.thinkingBlock;
    this.thinkingBlock = null;
    this.input.stream.push({
      type: "thinking_end",
      contentIndex: this.contentIndex(block),
      content: block.thinking,
      partial: this.input.output,
    });
  }

  private closeToolCalls(): void {
    for (const block of this.toolCallBlocksByIndex.values()) {
      if (block.partialArgs === undefined) continue;
      block.arguments = parseStreamingJson<Record<string, unknown>>(block.partialArgs);
      delete block.partialArgs;
      delete block.streamIndex;
      this.input.stream.push({
        type: "toolcall_end",
        contentIndex: this.contentIndex(block),
        toolCall: block,
        partial: this.input.output,
      });
    }
  }

  private flushNativeToolBuffer(): void {
    if (this.nativeToolBuffer.length === 0) return;
    const decoded = decodeNativeToolBuffer(this.nativeToolBuffer);
    if (decoded === undefined) {
      const block = this.ensureTextBlock();
      block.text += this.nativeToolBuffer;
      this.input.stream.push({
        type: "text_delta",
        contentIndex: this.contentIndex(block),
        delta: this.nativeToolBuffer,
        partial: this.input.output,
      });
      this.closeTextBlock();
      this.nativeToolBuffer = "";
      this.nativeToolId = undefined;
      return;
    }

    const baseId = this.nativeToolId ?? makeGeneratedToolCallId();
    decoded.forEach((call, index) => {
      const id = index === 0 ? baseId : `${baseId}-${index}`;
      const block = this.ensureSyntheticToolCallBlock(id, call.name, this.blocks.length);
      block.partialArgs = call.args;
      block.arguments = parseStreamingJson<Record<string, unknown>>(call.args);
      this.input.stream.push({
        type: "toolcall_delta",
        contentIndex: this.contentIndex(block),
        delta: call.args,
        partial: this.input.output,
      });
      delete block.partialArgs;
      this.input.stream.push({
        type: "toolcall_end",
        contentIndex: this.contentIndex(block),
        toolCall: block,
        partial: this.input.output,
      });
    });
    this.nativeToolBuffer = "";
    this.nativeToolId = undefined;
  }

  private ensureSyntheticToolCallBlock(
    id: string,
    name: string,
    streamIndex: number,
  ): StreamingToolCallBlock {
    const block: StreamingToolCallBlock = {
      type: "toolCall",
      id,
      name,
      arguments: {},
      partialArgs: "",
      streamIndex,
    };
    this.blocks.push(block);
    this.input.stream.push({
      type: "toolcall_start",
      contentIndex: this.contentIndex(block),
      partial: this.input.output,
    });
    return block;
  }

  private finalReason(): string | undefined {
    if (this.finishReason !== undefined) return this.finishReason;
    if (this.receivedDone) return "stop";
    return undefined;
  }

  private get blocks(): StreamingBlock[] {
    return this.input.output.content as StreamingBlock[];
  }

  private contentIndex(block: StreamingBlock): number {
    return this.blocks.indexOf(block);
  }
}

const emitTurnError = ({
  output,
  stream,
  options,
  error,
}: {
  readonly output: AssistantMessage;
  readonly stream: AssistantMessageEventStream;
  readonly options: SimpleStreamOptions | undefined;
  readonly error: unknown;
}): void => {
  cleanupStreamingScratch(output);
  const reason = options?.signal?.aborted || isAbortError(error) ? "aborted" : "error";
  output.stopReason = reason;
  output.errorMessage = errorMessage(error);
  stream.push({ type: "error", reason, error: output });
  stream.end();
};

const requireOpenAiCompletionsModel = Effect.fn("PiAgentModel.requireOpenAiCompletionsModel")(
  function* (model: Model<Api>): Effect.fn.Return<Model<"openai-completions">, InvalidPiModel> {
    if (model.api === "openai-completions") return model as Model<"openai-completions">;
    return yield* new InvalidPiModel({
      api: model.api,
      message: `PiAgentModel requires an openai-completions Pi model for Workers AI payload conversion; received ${model.api}.`,
    });
  },
);

const toWorkersAiTool = (tool: Tool): WorkersAiTool => ({
  type: "function",
  function: {
    name: tool.name,
    description: tool.description,
    parameters: tool.parameters,
    strict: false,
  },
});

const toAnthropicTool = (tool: Tool): AnthropicTool => {
  const schema = tool.parameters as { readonly properties?: unknown; readonly required?: string[] };
  return {
    name: tool.name,
    description: tool.description,
    input_schema: {
      type: "object",
      properties: schema.properties ?? {},
      required: schema.required ?? [],
    },
  };
};

const toAnthropicMessages = (context: PiContext): AnthropicMessage[] => {
  const messages: AnthropicMessage[] = [];
  for (const message of context.messages) {
    switch (message.role) {
      case "user": {
        if (typeof message.content === "string") {
          if (message.content.trim().length > 0) {
            messages.push({ role: "user", content: message.content });
          }
        } else {
          const content = toAnthropicUserContent(message.content);
          if (content.length > 0) messages.push({ role: "user", content });
        }
        break;
      }
      case "assistant": {
        const content = message.content.flatMap((block): AnthropicContentBlock[] => {
          if (block.type === "text") {
            return block.text.trim().length === 0 ? [] : [{ type: "text", text: block.text }];
          }
          if (block.type === "thinking") {
            if (block.thinking.trim().length === 0) return [];
            return block.thinkingSignature
              ? [
                  {
                    type: "thinking",
                    thinking: block.thinking,
                    signature: block.thinkingSignature,
                  },
                ]
              : [{ type: "text", text: block.thinking }];
          }
          return [
            {
              type: "tool_use",
              id: normalizeAnthropicToolCallId(block.id),
              name: block.name,
              input: block.arguments ?? {},
            },
          ];
        });
        if (content.length > 0) messages.push({ role: "assistant", content });
        break;
      }
      case "toolResult":
        messages.push({
          role: "user",
          content: [
            {
              type: "tool_result",
              tool_use_id: normalizeAnthropicToolCallId(message.toolCallId),
              content: toAnthropicToolResultContent(message.content),
              is_error: message.isError,
            },
          ],
        });
        break;
    }
  }
  return messages;
};

const toAnthropicUserContent = (
  content: ReadonlyArray<TextContent | ImageContent>,
): AnthropicContentBlock[] =>
  content.flatMap((block): AnthropicContentBlock[] => {
    if (block.type === "text") {
      return block.text.trim().length === 0 ? [] : [{ type: "text", text: block.text }];
    }
    return [
      {
        type: "image",
        source: {
          type: "base64",
          media_type: block.mimeType,
          data: block.data,
        },
      },
    ];
  });

const toAnthropicToolResultContent = (
  content: ReadonlyArray<TextContent | ImageContent>,
): string | ReadonlyArray<AnthropicContentBlock> => {
  const blocks = toAnthropicUserContent(content);
  if (blocks.every((block) => block.type === "text")) {
    return blocks.map((block) => (block.type === "text" ? block.text : "")).join("\n");
  }
  return blocks;
};

const normalizeAnthropicToolCallId = (id: string): string =>
  id.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64);

const parseChunk = (data: string): Record<string, unknown> => {
  const parsed = JSON.parse(data) as unknown;
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    throw new Error("Workers AI stream chunk must be a JSON object.");
  }
  return parsed as Record<string, unknown>;
};

const normalizeAnthropicStreamData = (data: string): string => {
  if (data.length === 0 || data === "[DONE]") return data;
  const event = parseChunk(data);
  const type = stringField(event, "type");
  if (type === undefined) return data;

  switch (type) {
    case "message_start": {
      const message = optionalObjectField(event, "message");
      return JSON.stringify({
        id: message === undefined ? undefined : stringField(message, "id"),
        model: message === undefined ? undefined : stringField(message, "model"),
        usage: message?.usage,
      });
    }
    case "content_block_start": {
      const block = optionalObjectField(event, "content_block");
      if (block === undefined) return "";
      const index = optionalNumberField(event, "index") ?? 0;
      const blockType = stringField(block, "type");
      if (blockType === "tool_use") {
        const input = block.input;
        const args =
          input === undefined || input === null || JSON.stringify(input) === "{}"
            ? ""
            : JSON.stringify(input);
        return JSON.stringify({
          choices: [
            {
              delta: {
                tool_calls: [
                  {
                    index,
                    id: stringField(block, "id"),
                    function: { name: stringField(block, "name"), arguments: args },
                  },
                ],
              },
            },
          ],
        });
      }
      if (blockType === "text") {
        const text = stringField(block, "text");
        return text && text.length > 0
          ? JSON.stringify({ choices: [{ delta: { content: text } }] })
          : "";
      }
      if (blockType === "thinking") {
        const thinking = stringField(block, "thinking");
        return thinking && thinking.length > 0
          ? JSON.stringify({ choices: [{ delta: { reasoning_content: thinking } }] })
          : "";
      }
      return "";
    }
    case "content_block_delta": {
      const delta = optionalObjectField(event, "delta");
      if (delta === undefined) return "";
      const deltaType = stringField(delta, "type");
      if (deltaType === "text_delta") {
        return JSON.stringify({
          choices: [{ delta: { content: stringField(delta, "text") ?? "" } }],
        });
      }
      if (deltaType === "thinking_delta") {
        return JSON.stringify({
          choices: [{ delta: { reasoning_content: stringField(delta, "thinking") ?? "" } }],
        });
      }
      if (deltaType === "input_json_delta") {
        return JSON.stringify({
          choices: [
            {
              delta: {
                tool_calls: [
                  {
                    index: optionalNumberField(event, "index") ?? 0,
                    function: { arguments: stringField(delta, "partial_json") ?? "" },
                  },
                ],
              },
            },
          ],
        });
      }
      return "";
    }
    case "message_delta": {
      const delta = optionalObjectField(event, "delta");
      return JSON.stringify({
        finish_reason: delta === undefined ? undefined : nullableStringField(delta, "stop_reason"),
        usage: event.usage,
      });
    }
    case "message_stop":
      return "[DONE]";
    case "content_block_stop":
    case "ping":
      return "";
    default:
      return data;
  }
};

const optionalObjectField = (
  input: Record<string, unknown>,
  field: string,
): Record<string, unknown> | undefined => {
  const value = input[field];
  if (value === undefined || value === null) return undefined;
  return requireObject(value, field);
};

const optionalArrayField = (
  input: Record<string, unknown>,
  field: string,
): ReadonlyArray<unknown> | undefined => {
  const value = input[field];
  if (value === undefined || value === null) return undefined;
  if (!Array.isArray(value)) throw new Error(`Workers AI stream field ${field} must be an array.`);
  return value;
};

const objectAt = (
  values: ReadonlyArray<unknown>,
  index: number,
): Record<string, unknown> | undefined => {
  const value = values[index];
  if (value === undefined || value === null) return undefined;
  return requireObject(value, `array item ${index}`);
};

const requireObject = (value: unknown, label: string): Record<string, unknown> => {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`Workers AI stream ${label} must be a JSON object.`);
  }
  return value as Record<string, unknown>;
};

const nullableStringField = (
  input: Record<string, unknown>,
  field: string,
): string | null | undefined => {
  const value = input[field];
  if (value === undefined) return undefined;
  if (value === null) return null;
  if (typeof value !== "string") {
    throw new Error(`Workers AI stream field ${field} must be a string.`);
  }
  return value;
};

const stringField = (input: Record<string, unknown>, field: string): string | undefined => {
  const value = input[field];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "string") {
    throw new Error(`Workers AI stream field ${field} must be a string.`);
  }
  return value;
};

const optionalNumberField = (input: Record<string, unknown>, field: string): number | undefined => {
  const value = input[field];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new Error(`Workers AI stream field ${field} must be a finite number.`);
  }
  return value;
};

const optionalUsage = (value: unknown): WorkersAiUsage | undefined => {
  if (value === undefined || value === null) return undefined;
  const usage = requireObject(value, "usage");
  return usage as WorkersAiUsage;
};

const pickReasoning = (
  delta: Record<string, unknown>,
): { readonly field: string; readonly text: string } | null => {
  for (const field of ["reasoning_content", "reasoning", "reasoning_text"] as const) {
    const value = delta[field];
    if (value === undefined || value === null || value === "") continue;
    if (typeof value !== "string") {
      throw new Error(`Workers AI stream field ${field} must be a string.`);
    }
    return { field, text: value };
  }
  return null;
};

const hasOpenAiDelta = (chunk: Record<string, unknown>): boolean => {
  const choices = optionalArrayField(chunk, "choices");
  if (choices === undefined) return false;
  const choice = objectAt(choices, 0);
  if (choice === undefined) return false;
  return optionalObjectField(choice, "delta") !== undefined;
};

const isNullFinalizationToolCall = (toolCall: Record<string, unknown>): boolean => {
  const fn = optionalObjectField(toolCall, "function");
  const name = fn?.name ?? toolCall.name ?? null;
  const args = fn?.arguments ?? toolCall.arguments ?? null;
  const id = toolCall.id ?? null;
  return !id && !name && (!args || args === "");
};

const toolCallName = (toolCall: Record<string, unknown>): string => {
  const fn = optionalObjectField(toolCall, "function");
  const raw = fn?.name ?? toolCall.name;
  if (raw === undefined || raw === null) return "";
  if (typeof raw !== "string") throw new Error("Workers AI tool call name must be a string.");
  return raw;
};

const toolCallArgumentsDelta = (toolCall: Record<string, unknown>): string => {
  const fn = optionalObjectField(toolCall, "function");
  const raw = fn?.arguments ?? toolCall.arguments;
  if (raw === undefined || raw === null) return "";
  if (typeof raw === "string") return raw;
  if (typeof raw === "object") return JSON.stringify(raw);
  throw new Error("Workers AI tool call arguments must be a string or object.");
};

const decodeNativeToolBuffer = (
  buffer: string,
): ReadonlyArray<{ readonly name: string; readonly args: string }> | undefined => {
  const trimmed = buffer.trim();
  if (trimmed.length === 0 || (trimmed[0] !== "{" && trimmed[0] !== "[")) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch {
    return undefined;
  }
  const items = Array.isArray(parsed) ? parsed : [parsed];
  const calls = items.flatMap(
    (item): ReadonlyArray<{ readonly name: string; readonly args: string }> => {
      if (typeof item !== "object" || item === null || Array.isArray(item)) return [];
      const record = item as Record<string, unknown>;
      const fn =
        typeof record.function === "object" && record.function !== null
          ? (record.function as Record<string, unknown>)
          : undefined;
      const name = fn?.name ?? record.name;
      if (typeof name !== "string" || name.length === 0) return [];
      const rawArgs = fn?.arguments ?? record.arguments ?? record.parameters ?? {};
      return [{ name, args: typeof rawArgs === "string" ? rawArgs : JSON.stringify(rawArgs) }];
    },
  );
  return calls.length > 0 ? calls : undefined;
};

const readSseChunks = Effect.fn("PiAgentModel.readSseChunks")(function* (
  body: ReadableStream<Uint8Array>,
  onData: SseChunkHandler,
): Effect.fn.Return<void, ModelStreamFailed> {
  return yield* Effect.acquireUseRelease(
    Effect.try({
      try: (): SseReaderState => ({
        reader: body.getReader(),
        decoder: new TextDecoder(),
        buffer: "",
        finished: false,
      }),
      catch: modelStreamFailure,
    }),
    (state) => readSseReader(state, onData),
    (state) => releaseSseReader(state),
  );
});

const readSseReader = Effect.fn("PiAgentModel.readSseReader")(function* (
  state: SseReaderState,
  onData: SseChunkHandler,
): Effect.fn.Return<void, ModelStreamFailed> {
  while (true) {
    const { done, value } = yield* Effect.tryPromise({
      try: () => state.reader.read(),
      catch: modelStreamFailure,
    });
    if (done) {
      state.finished = true;
      state.buffer += state.decoder.decode();
      if (state.buffer.trim().length > 0) yield* readSseBlock(state.buffer, onData);
      return;
    }
    if (!(value instanceof Uint8Array)) {
      return yield* streamFailure(new Error("Workers AI stream reader returned non-byte data."));
    }
    state.buffer += state.decoder.decode(value, { stream: true });
    let boundary = findSseBoundary(state.buffer);
    while (boundary) {
      const block = state.buffer.slice(0, boundary.index);
      state.buffer = state.buffer.slice(boundary.index + boundary.width);
      yield* readSseBlock(block, onData);
      boundary = findSseBoundary(state.buffer);
    }
  }
});

const readSseBlock = Effect.fn("PiAgentModel.readSseBlock")(function* (
  block: string,
  onData: SseChunkHandler,
): Effect.fn.Return<void, ModelStreamFailed> {
  const data = parseSseData(block);
  if (data !== undefined) yield* onData(data);
});

const releaseSseReader = (state: SseReaderState): Effect.Effect<void> =>
  Effect.gen(function* () {
    if (!state.finished) {
      yield* Effect.tryPromise({
        try: async () => {
          await state.reader.cancel();
        },
        catch: () => undefined,
      }).pipe(Effect.orElseSucceed(() => undefined));
    }
    yield* Effect.try({
      try: () => state.reader.releaseLock(),
      catch: () => undefined,
    }).pipe(Effect.orElseSucceed(() => undefined));
  });

const findSseBoundary = (
  buffer: string,
): { readonly index: number; readonly width: number } | null => {
  const lf = buffer.indexOf("\n\n");
  const crlf = buffer.indexOf("\r\n\r\n");
  if (lf === -1 && crlf === -1) return null;
  if (lf === -1) return { index: crlf, width: 4 };
  if (crlf === -1) return { index: lf, width: 2 };
  return lf < crlf ? { index: lf, width: 2 } : { index: crlf, width: 4 };
};

const parseSseData = (block: string): string | undefined => {
  const dataLines: string[] = [];
  let start = 0;
  while (start <= block.length) {
    const newline = block.indexOf("\n", start);
    const end = newline === -1 ? block.length : newline;
    const lineEnd = end > start && block.charCodeAt(end - 1) === 13 ? end - 1 : end;
    const line = block.slice(start, lineEnd);
    if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
    if (newline === -1) break;
    start = newline + 1;
  }
  return dataLines.length === 0 ? undefined : dataLines.join("\n");
};

const applyReasoningEffort = (
  payload: Record<string, unknown>,
  model: Model<Api>,
  level: SimpleStreamOptions["reasoning"] | undefined,
): void => {
  if (!model.reasoning || level === undefined) return;
  payload.reasoning_effort = mapReasoningEffort(level);
};

const mapReasoningEffort = (
  level: NonNullable<SimpleStreamOptions["reasoning"]>,
): WorkersAIReasoningEffort => {
  switch (level) {
    case "minimal":
    case "low":
      return "low";
    case "medium":
      return "medium";
    case "high":
    case "xhigh":
      return "high";
  }
};

const isAnthropicRequest = (model: Model<Api>, requestedModel: string): boolean =>
  model.api === "anthropic-messages" ||
  model.provider === "anthropic" ||
  requestedModel.startsWith("claude-");

const mapFinishReason = (
  value: string,
  hasToolCalls: boolean,
): { readonly stopReason: StopReason; readonly errorMessage?: string | undefined } => {
  if (hasToolCalls) return { stopReason: "toolUse" };
  switch (value) {
    case "stop":
    case "eos":
    case "end":
    case "end_turn":
    case "stop_sequence":
      return { stopReason: "stop" };
    case "length":
    case "model_length":
    case "max_tokens":
      return { stopReason: "length" };
    case "tool_calls":
    case "tool-calls":
    case "function_call":
    case "tool_use":
      return { stopReason: "toolUse" };
    case "content_filter":
    case "content-filter":
      return { stopReason: "error", errorMessage: "Provider finish_reason: content_filter" };
    case "refusal":
      return { stopReason: "error", errorMessage: "Provider finish_reason: refusal" };
    case "error":
    case "network_error":
      return { stopReason: "error", errorMessage: `Provider finish_reason: ${value}` };
    default:
      return { stopReason: "error", errorMessage: `Provider finish_reason: ${value}` };
  }
};

const doneReason = (reason: StopReason): "stop" | "length" | "toolUse" =>
  reason === "length" || reason === "toolUse" ? reason : "stop";

const toUsage = (raw: WorkersAiUsage): Usage => {
  const promptTokens = numberOrZero(raw.prompt_tokens ?? raw.input_tokens);
  const output = numberOrZero(raw.completion_tokens ?? raw.output_tokens);
  const cacheRead = numberOrZero(
    raw.prompt_tokens_details?.cached_tokens ??
      raw.cache_read_input_tokens ??
      raw.cache_read_tokens ??
      raw.prompt_cache_hit_tokens,
  );
  const cacheWrite = numberOrZero(
    raw.prompt_tokens_details?.cache_write_tokens ??
      raw.cache_creation_input_tokens ??
      raw.cache_write_tokens,
  );
  const input = Math.max(0, promptTokens - cacheRead - cacheWrite);
  const totalTokens = numberOrZero(raw.total_tokens) || input + output + cacheRead + cacheWrite;
  return {
    input,
    output,
    cacheRead,
    cacheWrite,
    totalTokens,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
};

const hasNonZeroUsage = (usage: WorkersAiUsage): boolean =>
  numberOrZero(usage.prompt_tokens) > 0 ||
  numberOrZero(usage.completion_tokens) > 0 ||
  numberOrZero(usage.total_tokens) > 0 ||
  numberOrZero(usage.input_tokens) > 0 ||
  numberOrZero(usage.output_tokens) > 0 ||
  numberOrZero(usage.cache_read_input_tokens) > 0 ||
  numberOrZero(usage.cache_creation_input_tokens) > 0 ||
  numberOrZero(usage.cache_read_tokens) > 0 ||
  numberOrZero(usage.cache_write_tokens) > 0 ||
  numberOrZero(usage.prompt_cache_hit_tokens) > 0 ||
  numberOrZero(usage.prompt_tokens_details?.cached_tokens) > 0 ||
  numberOrZero(usage.prompt_tokens_details?.cache_write_tokens) > 0;

const mergeUsage = (previous: WorkersAiUsage, next: WorkersAiUsage): WorkersAiUsage => ({
  ...previous,
  ...next,
  prompt_tokens: next.prompt_tokens ?? previous.prompt_tokens,
  completion_tokens: next.completion_tokens ?? previous.completion_tokens,
  total_tokens: next.total_tokens ?? previous.total_tokens,
  input_tokens: next.input_tokens ?? previous.input_tokens,
  output_tokens: next.output_tokens ?? previous.output_tokens,
  cache_read_input_tokens: next.cache_read_input_tokens ?? previous.cache_read_input_tokens,
  cache_creation_input_tokens:
    next.cache_creation_input_tokens ?? previous.cache_creation_input_tokens,
  cache_read_tokens: next.cache_read_tokens ?? previous.cache_read_tokens,
  cache_write_tokens: next.cache_write_tokens ?? previous.cache_write_tokens,
  prompt_cache_hit_tokens: next.prompt_cache_hit_tokens ?? previous.prompt_cache_hit_tokens,
  prompt_tokens_details: {
    ...previous.prompt_tokens_details,
    ...next.prompt_tokens_details,
    cached_tokens:
      next.prompt_tokens_details?.cached_tokens ?? previous.prompt_tokens_details?.cached_tokens,
    cache_write_tokens:
      next.prompt_tokens_details?.cache_write_tokens ??
      previous.prompt_tokens_details?.cache_write_tokens,
  },
});

const makeAssistantMessage = (options: {
  readonly model: Model<Api>;
  readonly modelId: string;
  readonly content: AssistantMessage["content"];
  readonly stopReason: StopReason;
  readonly usage?: Usage | undefined;
  readonly errorMessage?: string | undefined;
}): AssistantMessage => {
  const message: AssistantMessage = {
    role: "assistant",
    content: options.content,
    api: options.model.api,
    provider: options.model.provider,
    model: options.modelId,
    usage: options.usage ?? emptyUsage,
    stopReason: options.stopReason,
    timestamp: Date.now(),
  };
  return options.errorMessage === undefined
    ? message
    : { ...message, errorMessage: options.errorMessage };
};

const extraHeadersOption = (
  options: SimpleStreamOptions | undefined,
): { readonly extraHeaders?: Record<string, string> } => {
  const extraHeaders = mergeHeaders(
    options?.sessionId === undefined ? undefined : { "x-session-affinity": options.sessionId },
    options?.headers,
  );
  return Object.keys(extraHeaders).length === 0 ? {} : { extraHeaders };
};

const mergeHeaders = (
  ...headers: ReadonlyArray<ProviderHeaders | undefined>
): Record<string, string> => {
  const out: Record<string, string> = {};
  for (const headerSet of headers) {
    if (headerSet === undefined) continue;
    for (const [key, value] of Object.entries(headerSet)) {
      if (value === null) delete out[key];
      else out[key] = String(value);
    }
  }
  return out;
};

const modelStreamFailure = (cause: unknown): ModelStreamFailed =>
  new ModelStreamFailed({
    message: errorMessage(cause) || "Workers AI stream processing failed.",
    cause,
  });

const streamFailure = (cause: unknown): Effect.Effect<never, ModelStreamFailed> =>
  Effect.fail(modelStreamFailure(cause));

const tryStreamSync = <A>(evaluate: () => A): Effect.Effect<A, ModelStreamFailed> =>
  Effect.try({
    try: evaluate,
    catch: modelStreamFailure,
  });

const cleanupStreamingScratch = (output: AssistantMessage): void => {
  for (const block of output.content as StreamingBlock[]) {
    if (block.type === "toolCall") {
      delete block.partialArgs;
      delete block.streamIndex;
    }
  }
};

const hasToolCalls = (output: AssistantMessage): boolean =>
  output.content.some((block) => block.type === "toolCall");

const isAbortError = (error: unknown): boolean => {
  if (error instanceof DOMException && error.name === "AbortError") return true;
  if (typeof error !== "object" || error === null) return false;
  const cause = (error as { readonly cause?: unknown }).cause;
  return cause !== undefined && cause !== error && isAbortError(cause);
};

const headersToRecord = (headers: Headers): Record<string, string> => {
  const out: Record<string, string> = {};
  headers.forEach((value, key) => {
    out[key] = value;
  });
  return out;
};

const safeReadText = (response: Response): Effect.Effect<string | undefined> =>
  Effect.tryPromise({
    try: () => response.text(),
    catch: () => undefined,
  }).pipe(Effect.orElseSucceed(() => undefined));

const cancelResponseBody = (response: Response): Effect.Effect<void> =>
  Effect.tryPromise({
    try: async () => {
      if (response.body && !response.body.locked) await response.body.cancel();
    },
    catch: () => undefined,
  }).pipe(Effect.orElseSucceed(() => undefined));

const errorMessage = (error: unknown): string => {
  if (error instanceof Error) return error.message;
  if (typeof error === "object" && error !== null) {
    const message = (error as { readonly message?: unknown }).message;
    if (typeof message === "string") return message;
    const reason = (error as { readonly reason?: { readonly description?: unknown } }).reason;
    if (typeof reason?.description === "string") return reason.description;
  }
  return String(error);
};

const makeGeneratedToolCallId = (): string => `call_${crypto.randomUUID()}`;

const numberOrZero = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0;

const emptyUsage: Usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

export * as PiAgentModel from "./PiAgentModel.ts";
