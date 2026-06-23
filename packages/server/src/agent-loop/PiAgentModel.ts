import {
  createAssistantMessageEventStream,
  parseStreamingJson,
  type Api,
  type AssistantMessage,
  type AssistantMessageEventStream,
  type Context as PiContext,
  type Message,
  type Model,
  type OpenAICompletionsCompat,
  type SimpleStreamOptions,
  type StopReason,
  type TextContent,
  type ThinkingContent,
  type Tool,
  type ToolCall,
  type ToolResultMessage,
  type Usage,
  type UserMessage,
} from "@earendil-works/pi-ai";
import { convertMessages } from "@earendil-works/pi-ai/openai-completions";
import * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

type WorkersAiMessage = ReturnType<typeof convertMessages>[number];

type WorkersAiTool = {
  readonly type: "function";
  readonly function: {
    readonly name: string;
    readonly description?: string;
    readonly parameters: unknown;
    readonly strict?: boolean;
  };
};

const FiniteNumber = Schema.Number.check(Schema.isFinite());

const WorkersAiUsage = Schema.Struct({
  prompt_tokens: Schema.optionalKey(FiniteNumber),
  completion_tokens: Schema.optionalKey(FiniteNumber),
  total_tokens: Schema.optionalKey(FiniteNumber),
  input_tokens: Schema.optionalKey(FiniteNumber),
  output_tokens: Schema.optionalKey(FiniteNumber),
  cache_read_tokens: Schema.optionalKey(FiniteNumber),
  cache_write_tokens: Schema.optionalKey(FiniteNumber),
  prompt_tokens_details: Schema.optionalKey(
    Schema.Struct({ cached_tokens: Schema.optionalKey(FiniteNumber) }),
  ),
});
type WorkersAiUsage = typeof WorkersAiUsage.Type;

const ChatCompletionToolCall = Schema.Struct({
  index: Schema.optionalKey(FiniteNumber),
  id: Schema.optionalKey(Schema.String),
  function: Schema.optionalKey(
    Schema.Struct({
      name: Schema.optionalKey(Schema.String),
      arguments: Schema.optionalKey(Schema.String),
    }),
  ),
});

const ChatCompletionDelta = Schema.Struct({
  content: Schema.optionalKey(Schema.NullOr(Schema.String)),
  reasoning_content: Schema.optionalKey(Schema.NullOr(Schema.String)),
  reasoning: Schema.optionalKey(Schema.NullOr(Schema.String)),
  tool_calls: Schema.optionalKey(Schema.Array(ChatCompletionToolCall)),
});
type ChatCompletionDelta = typeof ChatCompletionDelta.Type;

const ChatCompletionChoice = Schema.Struct({
  index: Schema.optionalKey(FiniteNumber),
  delta: Schema.optionalKey(ChatCompletionDelta),
  finish_reason: Schema.optionalKey(Schema.NullOr(Schema.String)),
  usage: Schema.optionalKey(WorkersAiUsage),
});

const ChatCompletionChunk = Schema.Struct({
  id: Schema.optionalKey(Schema.Unknown),
  model: Schema.optionalKey(Schema.Unknown),
  choices: Schema.optionalKey(Schema.Array(ChatCompletionChoice)),
  usage: Schema.optionalKey(WorkersAiUsage),
});
type ChatCompletionChunk = typeof ChatCompletionChunk.Type;

const ChatCompletionChunkFromJsonString = Schema.fromJsonString(ChatCompletionChunk);

type WorkersAIReasoningEffort = "low" | "medium" | "high";

type ProviderTextOrImageContent = Exclude<UserMessage["content"], string>[number];
type ProviderContentBlock =
  | ProviderTextOrImageContent
  | AssistantMessage["content"][number]
  | ToolResultMessage["content"][number];
type TurnUserContent =
  | { readonly type: "text"; readonly text: string; readonly textSignature?: string | undefined }
  | { readonly type: "image"; readonly data: string; readonly mimeType: string };
type TurnAssistantContent =
  | TurnUserContent
  | {
      readonly type: "thinking";
      readonly thinking: string;
      readonly thinkingSignature?: string | undefined;
      readonly redacted?: boolean | undefined;
    }
  | {
      readonly type: "toolCall";
      readonly id: string;
      readonly name: string;
      readonly arguments: Record<string, unknown>;
      readonly thoughtSignature?: string | undefined;
    };
type TurnToolResultContent = TurnUserContent;
type TurnContent = TurnUserContent | TurnAssistantContent | TurnToolResultContent;
type SignalMessageLike = {
  readonly role: "signal";
  readonly type: string;
  readonly tagName?: string | undefined;
  readonly content: string;
  readonly attributes?: Readonly<Record<string, unknown>> | undefined;
};
type TurnSourceMessage = Message | SignalMessageLike;
type TurnInputMessage =
  | { readonly role: "user"; readonly content: string | ReadonlyArray<TurnUserContent> }
  | { readonly role: "assistant"; readonly content: ReadonlyArray<TurnAssistantContent> }
  | {
      readonly role: "toolResult";
      readonly toolCallId: string;
      readonly toolName: string;
      readonly content: ReadonlyArray<TurnToolResultContent>;
      readonly isError: boolean;
    };
type StreamingTextBlock = TextContent;
type StreamingThinkingBlock = ThinkingContent;
type StreamingToolCallBlock = ToolCall & { partialArgs?: string; streamIndex?: number };
type StreamingBlock = StreamingTextBlock | StreamingThinkingBlock | StreamingToolCallBlock;
type SseChunkHandler = (chunk: ChatCompletionChunk) => Effect.Effect<void, ModelStreamFailed>;
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

type WorkersAiRunBinding = Effect.Success<Cloudflare.AiGatewayClient["raw"]>;

type ModelTurnError = InvalidPiModel | ModelCallFailed | ModelResponseFailed | ModelStreamFailed;

// Copied from Flue: event/turn payloads never carry raw image bytes.
export const IMAGE_DATA_OMITTED = "[image data omitted from event]";

export const AiGatewayId = Schema.String.pipe(Schema.brand("AiGatewayId"));
export type AiGatewayId = typeof AiGatewayId.Type;

export const WorkersAiModelId = Schema.String.pipe(Schema.brand("WorkersAiModelId"));
export type WorkersAiModelId = typeof WorkersAiModelId.Type;

const WORKERS_AI_COMPAT: Required<Omit<OpenAICompletionsCompat, "cacheControlFormat">> & {
  readonly cacheControlFormat: OpenAICompletionsCompat["cacheControlFormat"] | undefined;
} = {
  supportsStore: false,
  supportsDeveloperRole: false,
  supportsReasoningEffort: true,
  supportsUsageInStreaming: true,
  maxTokensField: "max_completion_tokens",
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

export interface Interface {
  readonly stream: (input: StreamInput) => Effect.Effect<AssistantMessageEventStream>;
}

export interface AiGatewayRuntime {
  readonly ai: WorkersAiRunBinding;
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

export class ModelResponseFailed extends Schema.TaggedErrorClass<ModelResponseFailed>()(
  "ModelResponseFailed",
  {
    message: Schema.String,
    cause: Schema.Defect(),
  },
) {}

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

        yield* runWorkersAiTurn({
          ai,
          gatewayId,
          requestedModel,
          context,
          model,
          options: {
            ...options,
            ...(maxTokens !== undefined ? { maxTokens } : {}),
            ...(temperature !== undefined ? { temperature } : {}),
          },
          stream: eventStream,
          output,
        }).pipe(
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
      const id = AiGatewayId.make(yield* client.id);
      return AiGateway.of({ ai, id });
    }),
  );

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
  for (const block of output.content as StreamingBlock[]) {
    if (block.type === "toolCall") {
      delete (block as StreamingToolCallBlock).partialArgs;
      delete (block as StreamingToolCallBlock).streamIndex;
    }
  }
  const reason = options?.signal?.aborted || isAbortError(error) ? "aborted" : "error";
  output.stopReason = reason;
  output.errorMessage = error instanceof Error ? error.message : String(error);
  stream.push({ type: "error", reason, error: output });
  stream.end();
};

const modelStreamFailure = (cause: unknown): ModelStreamFailed =>
  new ModelStreamFailed({
    message: cause instanceof Error ? cause.message : "Workers AI stream processing failed.",
    cause,
  });

const streamFailure = (cause: unknown): Effect.Effect<never, ModelStreamFailed> =>
  Effect.fail(modelStreamFailure(cause));

const tryStreamSync = <A>(evaluate: () => A): Effect.Effect<A, ModelStreamFailed> =>
  Effect.try({
    try: evaluate,
    catch: modelStreamFailure,
  });

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
  const body = response.body;
  if (!body) {
    return yield* new ModelResponseFailed({
      message: "Cloudflare AI Gateway returned empty response body.",
      cause: response,
    });
  }

  return yield* processWorkersAiResponse({ ...input, model, body }).pipe(
    Effect.catch((error) =>
      cancelResponseBody(response).pipe(Effect.flatMap(() => Effect.fail(error))),
    ),
  );
});

const processWorkersAiResponse = Effect.fn("PiAgentModel.processWorkersAiResponse")(
  function* (input: {
    readonly body: ReadableStream<Uint8Array>;
    readonly model: Model<"openai-completions">;
    readonly options: SimpleStreamOptions | undefined;
    readonly stream: AssistantMessageEventStream;
    readonly output: AssistantMessage;
  }): Effect.fn.Return<void, ModelStreamFailed> {
    yield* tryStreamSync(() => input.stream.push({ type: "start", partial: input.output }));

    let textBlock: StreamingTextBlock | null = null;
    let thinkingBlock: StreamingThinkingBlock | null = null;
    let hasFinishReason = false;
    const toolCallBlocksByIndex = new Map<number, StreamingToolCallBlock>();
    const toolCallBlocksById = new Map<string, StreamingToolCallBlock>();
    const blocks = input.output.content as StreamingBlock[];
    const indexOf = (block: StreamingBlock | null): number => (block ? blocks.indexOf(block) : -1);

    const finishBlock = (block: StreamingBlock): Effect.Effect<void, ModelStreamFailed> =>
      tryStreamSync(() => {
        const contentIndex = indexOf(block);
        if (contentIndex === -1) return;
        if (block.type === "text") {
          input.stream.push({
            type: "text_end",
            contentIndex,
            content: block.text,
            partial: input.output,
          });
        } else if (block.type === "thinking") {
          input.stream.push({
            type: "thinking_end",
            contentIndex,
            content: block.thinking,
            partial: input.output,
          });
        } else if (block.type === "toolCall") {
          block.arguments = parseStreamingJson<Record<string, unknown>>(block.partialArgs ?? "");
          delete block.partialArgs;
          delete block.streamIndex;
          input.stream.push({
            type: "toolcall_end",
            contentIndex,
            toolCall: block,
            partial: input.output,
          });
        }
      });

    const ensureTextBlock = (): StreamingTextBlock => {
      if (!textBlock) {
        textBlock = { type: "text", text: "" };
        blocks.push(textBlock);
        input.stream.push({
          type: "text_start",
          contentIndex: indexOf(textBlock),
          partial: input.output,
        });
      }
      return textBlock;
    };

    const ensureThinkingBlock = (thinkingSignature: string): StreamingThinkingBlock => {
      if (!thinkingBlock) {
        thinkingBlock = { type: "thinking", thinking: "", thinkingSignature };
        blocks.push(thinkingBlock);
        input.stream.push({
          type: "thinking_start",
          contentIndex: indexOf(thinkingBlock),
          partial: input.output,
        });
      }
      return thinkingBlock;
    };

    const ensureToolCallBlock = (toolCall: {
      readonly index?: number;
      readonly id?: string;
      readonly function?: { readonly name?: string; readonly arguments?: string };
    }): StreamingToolCallBlock => {
      const streamIndex = typeof toolCall.index === "number" ? toolCall.index : undefined;
      let block = streamIndex !== undefined ? toolCallBlocksByIndex.get(streamIndex) : undefined;
      if (!block && toolCall.id) {
        block = toolCallBlocksById.get(toolCall.id);
      }
      if (!block) {
        const newBlock: StreamingToolCallBlock = {
          type: "toolCall",
          id: toolCall.id ?? "",
          name: toolCall.function?.name ?? "",
          arguments: {},
          partialArgs: "",
        };
        if (streamIndex !== undefined) {
          newBlock.streamIndex = streamIndex;
        }
        block = newBlock;
        if (streamIndex !== undefined) {
          toolCallBlocksByIndex.set(streamIndex, block);
        }
        if (toolCall.id) {
          toolCallBlocksById.set(toolCall.id, block);
        }
        blocks.push(block);
        input.stream.push({
          type: "toolcall_start",
          contentIndex: indexOf(block),
          partial: input.output,
        });
      }
      if (streamIndex !== undefined && block.streamIndex === undefined) {
        block.streamIndex = streamIndex;
        toolCallBlocksByIndex.set(streamIndex, block);
      }
      if (toolCall.id) {
        toolCallBlocksById.set(toolCall.id, block);
      }
      return block;
    };

    const applyChunk = (chunk: ChatCompletionChunk): Effect.Effect<void, ModelStreamFailed> =>
      tryStreamSync(() => {
        const responseId = stringOrEmpty(chunk.id);
        if (!input.output.responseId && responseId.length > 0) {
          input.output.responseId = responseId;
        }
        const responseModel = stringOrEmpty(chunk.model);
        if (
          !input.output.responseModel &&
          responseModel.length > 0 &&
          responseModel !== input.output.model
        ) {
          input.output.responseModel = responseModel;
        }
        if (chunk.usage) {
          input.output.usage = toUsage(chunk.usage);
        }

        const choice = Array.isArray(chunk.choices) ? chunk.choices[0] : undefined;
        if (!choice) return;
        if (!chunk.usage && choice.usage) {
          input.output.usage = toUsage(choice.usage);
        }
        if (choice.finish_reason) {
          const mapped = mapFinishReason(choice.finish_reason);
          input.output.stopReason = mapped.stopReason;
          if (mapped.errorMessage) input.output.errorMessage = mapped.errorMessage;
          hasFinishReason = true;
        }

        const delta = choice.delta;
        if (!delta) return;

        if (delta.content !== null && delta.content !== undefined && delta.content.length > 0) {
          const block = ensureTextBlock();
          block.text += delta.content;
          input.stream.push({
            type: "text_delta",
            contentIndex: indexOf(block),
            delta: delta.content,
            partial: input.output,
          });
        }

        const reasoningDelta = pickReasoning(delta);
        if (reasoningDelta) {
          const block = ensureThinkingBlock(reasoningDelta.field);
          block.thinking += reasoningDelta.text;
          input.stream.push({
            type: "thinking_delta",
            contentIndex: indexOf(block),
            delta: reasoningDelta.text,
            partial: input.output,
          });
        }

        if (delta.tool_calls) {
          for (const toolCall of delta.tool_calls) {
            const block = ensureToolCallBlock(toolCall);
            if (!block.id && toolCall.id) {
              block.id = toolCall.id;
              toolCallBlocksById.set(toolCall.id, block);
            }
            if (!block.name && toolCall.function?.name) {
              block.name = toolCall.function.name;
            }
            let toolDelta = "";
            if (toolCall.function?.arguments) {
              toolDelta = toolCall.function.arguments;
              block.partialArgs = (block.partialArgs ?? "") + toolDelta;
              block.arguments = parseStreamingJson<Record<string, unknown>>(block.partialArgs);
            }
            input.stream.push({
              type: "toolcall_delta",
              contentIndex: indexOf(block),
              delta: toolDelta,
              partial: input.output,
            });
          }
        }
      });

    yield* readSseChunks(input.body, applyChunk);

    for (const block of blocks) {
      yield* finishBlock(block);
    }

    if (input.options?.signal?.aborted) {
      return yield* streamFailure(new Error("Request was aborted"));
    }
    if (input.output.stopReason === "error") {
      return yield* streamFailure(
        new Error(input.output.errorMessage ?? "Provider returned an error stop reason"),
      );
    }
    if (!hasFinishReason) {
      return yield* streamFailure(new Error("Stream ended without finish_reason"));
    }
    yield* tryStreamSync(() => {
      input.stream.push({
        type: "done",
        reason: doneReason(input.output.stopReason),
        message: input.output,
      });
      input.stream.end();
    });
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
    messages: toWorkersAiMessages(model, context),
    stream: true,
    stream_options: { include_usage: true },
  };
  if (context.tools && context.tools.length > 0) {
    payload.tools = context.tools.map(toWorkersAiTool);
  }
  if (options?.maxTokens) {
    payload.max_completion_tokens = options.maxTokens;
  }
  if (options?.temperature !== undefined) {
    payload.temperature = options.temperature;
  }
  applyReasoningEffort(payload, model, options?.reasoning);

  const overridden = yield* Effect.tryPromise({
    try: () => Promise.resolve(options?.onPayload?.(payload, model)),
    catch: (cause) =>
      new ModelCallFailed({
        message:
          cause instanceof Error ? cause.message : "Cloudflare AI Gateway payload hook failed.",
        cause,
      }),
  });
  const finalPayload = overridden === undefined ? payload : (overridden as Record<string, unknown>);
  const extraHeaders: Record<string, string> = {};
  if (options?.sessionId) {
    extraHeaders["x-session-affinity"] = options.sessionId;
  }
  if (options?.headers) {
    Object.assign(extraHeaders, options.headers);
  }

  const response = yield* Effect.tryPromise({
    try: () =>
      ai.run(requestedModel, finalPayload, {
        gateway: { id: gatewayId },
        returnRawResponse: true,
        ...(options?.signal ? { signal: options.signal } : {}),
        ...(Object.keys(extraHeaders).length > 0 ? { extraHeaders } : {}),
      }),
    catch: (cause) =>
      new ModelCallFailed({
        message:
          cause instanceof Error ? cause.message : "Cloudflare AI Gateway model call failed.",
        cause,
      }),
  });
  if (!(response instanceof Response)) {
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
        message:
          cause instanceof Error ? cause.message : "Cloudflare AI Gateway response hook failed.",
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

const toWorkersAiMessages = (
  model: Model<"openai-completions">,
  context: PiContext,
): ReadonlyArray<WorkersAiMessage> => convertMessages(model, context, WORKERS_AI_COMPAT);

const requireOpenAiCompletionsModel = Effect.fn("PiAgentModel.requireOpenAiCompletionsModel")(
  function* (model: Model<Api>): Effect.fn.Return<Model<"openai-completions">, InvalidPiModel> {
    if (model.api === "openai-completions") {
      // `convertMessages` is typed for `Model<'openai-completions'>` but only
      // reads provider/id/reasoning, which our validated model has. This
      // mirrors Flue's Workers AI provider boundary.
      return model as unknown as Model<"openai-completions">;
    }
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

const mapFinishReason = (
  value: string,
): { readonly stopReason: StopReason; readonly errorMessage?: string | undefined } => {
  switch (value) {
    case "stop":
    case "eos":
    case "end":
      return { stopReason: "stop" };
    case "length":
      return { stopReason: "length" };
    case "tool_calls":
    case "tool-calls":
    case "function_call":
      return { stopReason: "toolUse" };
    case "content_filter":
      return {
        stopReason: "error",
        errorMessage: "Provider stopped generation: content filter",
      };
    default:
      return {
        stopReason: "error",
        errorMessage: `Provider finish_reason: ${value}`,
      };
  }
};

const doneReason = (reason: StopReason): "stop" | "length" | "toolUse" =>
  reason === "length" || reason === "toolUse" ? reason : "stop";

const toUsage = (raw: WorkersAiUsage): Usage => {
  const cacheRead = numberOrZero(raw.prompt_tokens_details?.cached_tokens ?? raw.cache_read_tokens);
  const promptTokens = numberOrZero(raw.prompt_tokens ?? raw.input_tokens);
  const output = numberOrZero(raw.completion_tokens ?? raw.output_tokens);
  const input = Math.max(0, promptTokens - cacheRead);
  const totalTokens = numberOrZero(raw.total_tokens) || input + output + cacheRead;
  return {
    input,
    output,
    cacheRead,
    cacheWrite: numberOrZero(raw.cache_write_tokens),
    totalTokens,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  };
};

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

export const toTurnMessage = (message: TurnSourceMessage): TurnInputMessage => {
  if (message.role === "signal") {
    return {
      role: "user",
      content: renderSignalMessage(message),
    };
  }
  if (message.role === "user") {
    return {
      role: "user",
      content:
        typeof message.content === "string"
          ? message.content
          : (message.content.map(toTurnContent) as TurnUserContent[]),
    };
  }
  if (message.role === "assistant") {
    return {
      role: "assistant",
      content: message.content.map(toTurnContent) as TurnAssistantContent[],
    };
  }
  if (message.role === "toolResult") {
    return {
      role: "toolResult",
      toolCallId: message.toolCallId,
      toolName: message.toolName,
      content: message.content.map(toTurnContent) as TurnToolResultContent[],
      isError: message.isError,
    };
  }
  throw new Error(
    `[denora] Unsupported message role in turn context: ${(message as { readonly role?: unknown }).role}`,
  );
};

export const toTurnContent = (block: ProviderContentBlock): TurnContent => {
  if (block.type === "text") {
    return { type: "text", text: block.text, textSignature: block.textSignature };
  }
  if (block.type === "image") {
    return { type: "image", data: IMAGE_DATA_OMITTED, mimeType: block.mimeType };
  }
  if (block.type === "thinking") {
    return {
      type: "thinking",
      thinking: block.thinking,
      thinkingSignature: block.thinkingSignature,
      redacted: block.redacted,
    };
  }
  return {
    type: "toolCall",
    id: block.id,
    name: block.name,
    arguments: block.arguments,
    thoughtSignature: block.thoughtSignature,
  };
};

const renderSignalMessage = (message: SignalMessageLike): string => {
  const tagName = message.tagName ?? "signal";
  const attributes = [["type", message.type], ...Object.entries(message.attributes ?? {})]
    .map(([name, value]) => ` ${escapeXmlAttribute(name)}="${escapeXmlAttribute(value)}"`)
    .join("");
  return `<${tagName}${attributes}>\n${escapeXmlText(message.content)}\n</${tagName}>`;
};

const escapeXmlText = (value: unknown): string =>
  String(value).replaceAll("&", "&amp;").replaceAll("<", "&lt;").replaceAll(">", "&gt;");

const escapeXmlAttribute = (value: unknown): string =>
  escapeXmlText(value).replaceAll('"', "&quot;");

const readSseChunks = Effect.fn("PiAgentModel.readSseChunks")(function* (
  body: ReadableStream<Uint8Array>,
  onChunk: SseChunkHandler,
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
    (state) => readSseReader(state, onChunk),
    (state) => releaseSseReader(state),
  );
});

const readSseReader = Effect.fn("PiAgentModel.readSseReader")(function* (
  state: SseReaderState,
  onChunk: SseChunkHandler,
): Effect.fn.Return<void, ModelStreamFailed> {
  while (true) {
    const { done, value } = yield* Effect.tryPromise({
      try: () => state.reader.read(),
      catch: modelStreamFailure,
    });
    if (done) {
      state.finished = true;
      state.buffer += state.decoder.decode();
      if (state.buffer.trim().length > 0) {
        yield* readSseBlock(state.buffer, onChunk);
      }
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
      yield* readSseBlock(block, onChunk);
      boundary = findSseBoundary(state.buffer);
    }
  }
});

const readSseBlock = Effect.fn("PiAgentModel.readSseBlock")(function* (
  block: string,
  onChunk: SseChunkHandler,
): Effect.fn.Return<void, ModelStreamFailed> {
  const chunks = yield* parseSseEvents(block);
  for (const chunk of chunks) {
    yield* onChunk(chunk);
  }
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

const parseSseEvents = (
  block: string,
): Effect.Effect<ReadonlyArray<ChatCompletionChunk>, ModelStreamFailed> =>
  tryStreamSync(() => {
    const dataLines: Array<string> = [];
    let start = 0;
    while (start <= block.length) {
      const newline = block.indexOf("\n", start);
      const end = newline === -1 ? block.length : newline;
      const lineEnd = end > start && block.charCodeAt(end - 1) === 13 ? end - 1 : end;
      const line = block.slice(start, lineEnd);
      if (line.startsWith("data:")) {
        dataLines.push(line.slice(5).trimStart());
      }
      if (newline === -1) break;
      start = newline + 1;
    }
    if (dataLines.length === 0) return [];
    const data = dataLines.join("\n");
    if (data === "" || data === "[DONE]") return [];
    return [Schema.decodeUnknownSync(ChatCompletionChunkFromJsonString)(data)];
  });

const pickReasoning = (
  delta: ChatCompletionDelta,
): { readonly field: string; readonly text: string } | null => {
  for (const field of ["reasoning_content", "reasoning"] as const) {
    const value = delta[field];
    if (typeof value === "string" && value.length > 0) return { field, text: value };
  }
  return null;
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

const emptyUsage: Usage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

const stringOrEmpty = (value: unknown): string => (typeof value === "string" ? value : "");

const numberOrZero = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) ? value : 0;

export * as PiAgentModel from "./PiAgentModel.ts";
