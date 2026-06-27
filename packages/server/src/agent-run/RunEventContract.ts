import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, ToolResultMessage, UserMessage } from "@earendil-works/pi-ai";
import * as Schema from "effect/Schema";

export interface RunEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

const PublicEventIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const PublicRunEventRest = Schema.Record(Schema.String, Schema.Unknown);
const NonNegativeDuration = Schema.Number.check(Schema.isGreaterThanOrEqualTo(0));

export const LlmTextContent = Schema.Struct({
  type: Schema.Literal("text"),
  text: Schema.String,
  textSignature: Schema.optionalKey(Schema.String),
}).pipe(Schema.annotate({ identifier: "LlmTextContent" }));
export type LlmTextContent = typeof LlmTextContent.Type;

export const LlmImageContent = Schema.Struct({
  type: Schema.Literal("image"),
  data: Schema.String,
  mimeType: Schema.String,
}).pipe(Schema.annotate({ identifier: "LlmImageContent" }));
export type LlmImageContent = typeof LlmImageContent.Type;

export const LlmThinkingContent = Schema.Struct({
  type: Schema.Literal("thinking"),
  thinking: Schema.String,
  thinkingSignature: Schema.optionalKey(Schema.String),
  redacted: Schema.optionalKey(Schema.Boolean),
}).pipe(Schema.annotate({ identifier: "LlmThinkingContent" }));
export type LlmThinkingContent = typeof LlmThinkingContent.Type;

export const LlmToolCall = Schema.Struct({
  type: Schema.Literal("toolCall"),
  id: Schema.String,
  name: Schema.String,
  arguments: Schema.Record(Schema.String, Schema.Unknown),
  thoughtSignature: Schema.optionalKey(Schema.String),
}).pipe(Schema.annotate({ identifier: "LlmToolCall" }));
export type LlmToolCall = typeof LlmToolCall.Type;

export const LlmUserMessage = Schema.Struct({
  role: Schema.Literal("user"),
  content: Schema.Union([
    Schema.String,
    Schema.Array(Schema.Union([LlmTextContent, LlmImageContent])),
  ]),
}).pipe(Schema.annotate({ identifier: "LlmUserMessage" }));
export type LlmUserMessage = typeof LlmUserMessage.Type;

export const LlmAssistantMessage = Schema.Struct({
  role: Schema.Literal("assistant"),
  content: Schema.Array(Schema.Union([LlmTextContent, LlmThinkingContent, LlmToolCall])),
}).pipe(Schema.annotate({ identifier: "LlmAssistantMessage" }));
export type LlmAssistantMessage = typeof LlmAssistantMessage.Type;

export const LlmToolResultMessage = Schema.Struct({
  role: Schema.Literal("toolResult"),
  toolCallId: Schema.String,
  toolName: Schema.String,
  content: Schema.Array(Schema.Union([LlmTextContent, LlmImageContent])),
  isError: Schema.Boolean,
}).pipe(Schema.annotate({ identifier: "LlmToolResultMessage" }));
export type LlmToolResultMessage = typeof LlmToolResultMessage.Type;

export const LlmMessage = Schema.Union([
  LlmUserMessage,
  LlmAssistantMessage,
  LlmToolResultMessage,
]).pipe(Schema.annotate({ identifier: "LlmMessage" }));
export type LlmMessage = typeof LlmMessage.Type;

export const LlmTurnPurpose = Schema.Literals(["agent", "compaction", "compaction_prefix"]);
export type LlmTurnPurpose = typeof LlmTurnPurpose.Type;

export const OperationKind = Schema.Literals(["prompt", "skill", "task", "shell", "compact"]);
export type OperationKind = typeof OperationKind.Type;

export const FlueSerializedError = Schema.Struct({
  name: Schema.optionalKey(Schema.String),
  message: Schema.String,
  type: Schema.optionalKey(Schema.String),
  details: Schema.optionalKey(Schema.String),
  dev: Schema.optionalKey(Schema.String),
  meta: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
}).pipe(Schema.annotate({ identifier: "FlueSerializedError" }));
export type FlueSerializedError = typeof FlueSerializedError.Type;

const publicRunEventEnvelope = {
  v: Schema.Literal(3),
  runId: Schema.String,
  eventIndex: PublicEventIndex,
  timestamp: Schema.String,
};

const attachedAgentEventEnvelope = {
  v: Schema.Literal(3),
  instanceId: Schema.String,
  eventIndex: PublicEventIndex,
  timestamp: Schema.String,
  runId: Schema.optionalKey(Schema.Never),
  dispatchId: Schema.optionalKey(Schema.String),
  submissionId: Schema.optionalKey(Schema.String),
  messageId: Schema.optionalKey(Schema.Never),
  agentName: Schema.optionalKey(Schema.String),
  conversationId: Schema.optionalKey(Schema.String),
  session: Schema.optionalKey(Schema.String),
  parentSession: Schema.optionalKey(Schema.String),
  taskId: Schema.optionalKey(Schema.String),
  harness: Schema.optionalKey(Schema.String),
  operationId: Schema.optionalKey(Schema.String),
  turnId: Schema.optionalKey(Schema.String),
};

const PublicRunStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicRunEventEnvelope,
    type: Schema.Literal("run_start"),
    workflowName: Schema.String,
    startedAt: Schema.String,
    input: Schema.Unknown,
  }),
  [PublicRunEventRest],
);

const PublicRunEndEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicRunEventEnvelope,
    type: Schema.Literal("run_end"),
    isError: Schema.Boolean,
    durationMs: NonNegativeDuration,
    result: Schema.Unknown,
    error: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const PublicRunAgentEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicRunEventEnvelope,
    type: Schema.Literals([
      "agent_start",
      "agent_end",
      "turn_start",
      "turn_messages",
      "message_start",
      "message_end",
      "text_delta",
      "thinking_start",
      "thinking_delta",
      "thinking_end",
      "tool_start",
      "tool",
      "turn",
    ]),
  }),
  [PublicRunEventRest],
);

export const PublicRunEvent = Schema.Union([
  PublicRunStartEvent,
  PublicRunEndEvent,
  PublicRunAgentEvent,
]);

const AttachedAgentStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("agent_start"),
  }),
  [PublicRunEventRest],
);

const AttachedAgentEndEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("agent_end"),
    messages: Schema.Array(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const AttachedTurnStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("turn_start"),
    turnId: Schema.String,
    purpose: LlmTurnPurpose,
  }),
  [PublicRunEventRest],
);

const AttachedTurnMessagesEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("turn_messages"),
    turnId: Schema.String,
    purpose: LlmTurnPurpose,
    message: Schema.Unknown,
    toolResults: Schema.Array(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const AttachedMessageEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literals(["message_start", "message_end"]),
    message: LlmMessage,
    turnId: Schema.String,
  }),
  [PublicRunEventRest],
);

const AttachedTextDeltaEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("text_delta"),
    text: Schema.String,
  }),
  [PublicRunEventRest],
);

const AttachedThinkingStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("thinking_start"),
    contentIndex: Schema.optionalKey(Schema.Number),
  }),
  [PublicRunEventRest],
);

const AttachedThinkingDeltaEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("thinking_delta"),
    contentIndex: Schema.optionalKey(Schema.Number),
    delta: Schema.String,
  }),
  [PublicRunEventRest],
);

const AttachedThinkingEndEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("thinking_end"),
    contentIndex: Schema.optionalKey(Schema.Number),
    content: Schema.String,
  }),
  [PublicRunEventRest],
);

const AttachedToolStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("tool_start"),
    toolName: Schema.String,
    toolCallId: Schema.String,
    args: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const AttachedToolResultEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("tool"),
    toolName: Schema.String,
    toolCallId: Schema.String,
    isError: Schema.Boolean,
    result: Schema.optionalKey(Schema.Unknown),
    durationMs: NonNegativeDuration,
  }),
  [PublicRunEventRest],
);

const AttachedTurnEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("turn"),
    turnId: Schema.String,
    purpose: LlmTurnPurpose,
    durationMs: NonNegativeDuration,
    request: Schema.Unknown,
    response: Schema.Unknown,
    isError: Schema.Boolean,
  }),
  [PublicRunEventRest],
);

const AttachedTaskStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("task_start"),
    taskId: Schema.String,
    prompt: Schema.String,
    agent: Schema.optionalKey(Schema.String),
    cwd: Schema.optionalKey(Schema.String),
  }),
  [PublicRunEventRest],
);

const AttachedTaskEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("task"),
    taskId: Schema.String,
    agent: Schema.optionalKey(Schema.String),
    isError: Schema.Boolean,
    result: Schema.optionalKey(Schema.Unknown),
    durationMs: NonNegativeDuration,
  }),
  [PublicRunEventRest],
);

const AttachedCompactionStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("compaction_start"),
    reason: Schema.Literals(["threshold", "overflow", "manual"]),
    estimatedTokens: Schema.Number,
  }),
  [PublicRunEventRest],
);

const AttachedCompactionEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("compaction"),
    messagesBefore: Schema.Number,
    messagesAfter: Schema.Number,
    durationMs: NonNegativeDuration,
    isError: Schema.Boolean,
    error: Schema.optionalKey(Schema.Unknown),
    usage: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const AttachedOperationStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("operation_start"),
    operationId: Schema.String,
    operationKind: OperationKind,
  }),
  [PublicRunEventRest],
);

const AttachedOperationEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("operation"),
    operationId: Schema.String,
    operationKind: OperationKind,
    durationMs: NonNegativeDuration,
    isError: Schema.Boolean,
    error: Schema.optionalKey(Schema.Unknown),
    result: Schema.optionalKey(Schema.Unknown),
    usage: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const AttachedLogEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("log"),
    level: Schema.Literals(["info", "warn", "error"]),
    message: Schema.String,
    attributes: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
  }),
  [PublicRunEventRest],
);

const AttachedSubmissionSettledEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("submission_settled"),
    submissionId: Schema.String,
    outcome: Schema.Literals(["completed", "failed"]),
    result: Schema.optionalKey(Schema.Unknown),
    error: Schema.optionalKey(FlueSerializedError),
  }),
  [PublicRunEventRest],
);

const AttachedIdleEvent = Schema.StructWithRest(
  Schema.Struct({
    ...attachedAgentEventEnvelope,
    type: Schema.Literal("idle"),
  }),
  [PublicRunEventRest],
);

export const AttachedAgentEvent = Schema.Union([
  AttachedAgentStartEvent,
  AttachedAgentEndEvent,
  AttachedTurnStartEvent,
  AttachedTurnMessagesEvent,
  AttachedMessageEvent,
  AttachedTextDeltaEvent,
  AttachedThinkingStartEvent,
  AttachedThinkingDeltaEvent,
  AttachedThinkingEndEvent,
  AttachedToolStartEvent,
  AttachedToolResultEvent,
  AttachedTurnEvent,
  AttachedTaskStartEvent,
  AttachedTaskEvent,
  AttachedCompactionStartEvent,
  AttachedCompactionEvent,
  AttachedOperationStartEvent,
  AttachedOperationEvent,
  AttachedLogEvent,
  AttachedSubmissionSettledEvent,
  AttachedIdleEvent,
]).pipe(Schema.annotate({ identifier: "AttachedAgentEvent" }));
export type AttachedAgentEvent = typeof AttachedAgentEvent.Type;

export const PublicConversationEvent = AttachedAgentEvent;
export type PublicConversationEvent = AttachedAgentEvent;

export const PublicStreamEvent = Schema.Union([PublicRunEvent, AttachedAgentEvent]);

type ProviderTextOrImageContent = Exclude<UserMessage["content"], string>[number];
type ProviderContentBlock =
  | ProviderTextOrImageContent
  | AssistantMessage["content"][number]
  | ToolResultMessage["content"][number];
type TurnUserContent = LlmTextContent | LlmImageContent;
type TurnAssistantContent = LlmTextContent | LlmImageContent | LlmThinkingContent | LlmToolCall;
type TurnToolResultContent = TurnUserContent;
type TurnContent = TurnUserContent | TurnAssistantContent | TurnToolResultContent;
type SignalMessageLike = {
  readonly role: "signal";
  readonly type: string;
  readonly tagName?: string | undefined;
  readonly content: string;
  readonly attributes?: Readonly<Record<string, unknown>> | undefined;
};
type TurnSourceMessage = AgentMessage | SignalMessageLike;
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

export const IMAGE_DATA_OMITTED = "[image data omitted from event]";

const STREAM_EXCLUDED_EVENT_TYPES: ReadonlySet<string> = new Set(["turn_request"]);

const BUFFERED_RUN_EVENT_TYPES: ReadonlySet<string> = new Set([
  "text_delta",
  "thinking_start",
  "thinking_delta",
  "thinking_end",
]);

export const isStreamExcludedRunEvent = (event: RunEvent): boolean =>
  STREAM_EXCLUDED_EVENT_TYPES.has(event.type);

export const isBufferedRunEvent = (event: RunEvent): boolean =>
  BUFFERED_RUN_EVENT_TYPES.has(event.type);

export const redactRunEventImages = (event: RunEvent): RunEvent => {
  switch (event.type) {
    case "run_start": {
      const input = redactUnknownImages(event.input);
      return input === event.input ? event : { ...event, input };
    }
    case "message_start":
    case "message_end": {
      const message = redactMessageImages(event.message as AgentMessage | undefined);
      return message === event.message ? event : { ...event, message };
    }
    case "turn_messages": {
      const message = redactMessageImages(event.message as AgentMessage | undefined);
      const toolResults = redactEachMessageImages(event.toolResults as AgentMessage[] | undefined);
      if (message === event.message && toolResults === event.toolResults) return event;
      return { ...event, message, toolResults };
    }
    case "agent_end": {
      const messages = redactEachMessageImages(event.messages as AgentMessage[] | undefined);
      return messages === event.messages ? event : { ...event, messages };
    }
    case "tool": {
      const result = redactToolResultImages(event.result);
      return result === event.result ? event : { ...event, result };
    }
    default:
      return event;
  }
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
    return {
      type: "text",
      text: block.text,
      ...(block.textSignature === undefined ? {} : { textSignature: block.textSignature }),
    };
  }
  if (block.type === "image") {
    return { type: "image", data: IMAGE_DATA_OMITTED, mimeType: block.mimeType };
  }
  if (block.type === "thinking") {
    return {
      type: "thinking",
      thinking: block.thinking,
      ...(block.thinkingSignature === undefined
        ? {}
        : { thinkingSignature: block.thinkingSignature }),
      ...(block.redacted === undefined ? {} : { redacted: block.redacted }),
    };
  }
  return {
    type: "toolCall",
    id: block.id,
    name: block.name,
    arguments: block.arguments,
    ...(block.thoughtSignature === undefined ? {} : { thoughtSignature: block.thoughtSignature }),
  };
};

const redactMessageImages = (message: AgentMessage | undefined): AgentMessage | undefined => {
  if (message === undefined) return message;
  const content = (message as { readonly content?: unknown }).content;
  if (!Array.isArray(content)) return message;
  const redacted = redactContentImages(content);
  return redacted === content ? message : ({ ...message, content: redacted } as AgentMessage);
};

const redactEachMessageImages = (
  messages: AgentMessage[] | undefined,
): AgentMessage[] | undefined => {
  if (messages === undefined) return messages;
  let changed = false;
  const redacted = messages.map((message) => {
    const result = redactMessageImages(message);
    if (result !== message) changed = true;
    return result ?? message;
  });
  return changed ? redacted : messages;
};

const redactToolResultImages = (result: unknown): unknown => {
  if (result === null || typeof result !== "object") return result;
  const content = (result as { readonly content?: unknown }).content;
  if (!Array.isArray(content)) return result;
  const redacted = redactContentImages(content);
  return redacted === content ? result : { ...result, content: redacted };
};

const redactUnknownImages = (value: unknown): unknown => {
  if (Array.isArray(value)) return redactArrayImages(value);
  if (value === null || typeof value !== "object") return value;

  const record = value as Record<string, unknown>;
  if (record.type === "image" && typeof record.data === "string") {
    return record.data === IMAGE_DATA_OMITTED ? value : { ...record, data: IMAGE_DATA_OMITTED };
  }

  let changed = false;
  const next: Record<string, unknown> = {};
  for (const [key, child] of Object.entries(record)) {
    const redacted = redactUnknownImages(child);
    if (redacted !== child) changed = true;
    next[key] = redacted;
  }
  return changed ? next : value;
};

const redactArrayImages = (values: ReadonlyArray<unknown>): ReadonlyArray<unknown> => {
  let changed = false;
  const redacted = values.map((value) => {
    const result = redactUnknownImages(value);
    if (result !== value) changed = true;
    return result;
  });
  return changed ? redacted : values;
};

const redactContentImages = <T>(content: T[]): T[] => {
  let changed = false;
  const redacted = content.map((block) => {
    if (block === null || typeof block !== "object") return block;
    const { type, data } = block as { readonly type?: unknown; readonly data?: unknown };
    if (type === "image" && typeof data === "string" && data !== IMAGE_DATA_OMITTED) {
      changed = true;
      return { ...block, data: IMAGE_DATA_OMITTED };
    }
    return block;
  });
  return changed ? redacted : content;
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

export * as RunEventContract from "./RunEventContract.ts";
