import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, ToolResultMessage, UserMessage } from "@earendil-works/pi-ai";
import * as Schema from "effect/Schema";

export interface RunEvent {
  readonly type: string;
  readonly [key: string]: unknown;
}

const PublicEventIndex = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const PublicRunEventRest = Schema.Record(Schema.String, Schema.Unknown);
const publicRunEventEnvelope = {
  v: Schema.Literal(3),
  runId: Schema.String,
  eventIndex: PublicEventIndex,
  timestamp: Schema.String,
};

const publicConversationEventEnvelope = {
  v: Schema.Literal(3),
  instanceId: Schema.String,
  agentName: Schema.String,
  eventIndex: PublicEventIndex,
  timestamp: Schema.String,
  submissionId: Schema.optionalKey(Schema.String),
  messageId: Schema.optionalKey(Schema.String),
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
    runId: Schema.String,
    isError: Schema.Boolean,
    durationMs: Schema.Number.check(Schema.isGreaterThanOrEqualTo(0)),
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

const PublicConversationMessageEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literals(["message_start", "message_end"]),
    message: Schema.Unknown,
  }),
  [PublicRunEventRest],
);

const PublicConversationDeltaEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("text_delta"),
    text: Schema.String,
  }),
  [PublicRunEventRest],
);

const PublicConversationThinkingStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("thinking_start"),
    contentIndex: Schema.optionalKey(Schema.Number),
  }),
  [PublicRunEventRest],
);

const PublicConversationThinkingDeltaEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("thinking_delta"),
    contentIndex: Schema.optionalKey(Schema.Number),
    delta: Schema.String,
  }),
  [PublicRunEventRest],
);

const PublicConversationThinkingEndEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("thinking_end"),
    contentIndex: Schema.optionalKey(Schema.Number),
    content: Schema.String,
  }),
  [PublicRunEventRest],
);

const PublicConversationToolStartEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("tool_start"),
    toolName: Schema.String,
    toolCallId: Schema.String,
    input: Schema.optionalKey(Schema.Unknown),
    args: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const PublicConversationToolResultEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("tool"),
    toolName: Schema.String,
    toolCallId: Schema.String,
    isError: Schema.Boolean,
    result: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const PublicConversationAgentMarkerEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literals(["agent_start", "agent_end", "turn_start", "turn_messages"]),
  }),
  [PublicRunEventRest],
);

const PublicConversationTurnEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("turn"),
    request: Schema.optionalKey(Schema.Unknown),
    response: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const PublicConversationSubmissionSettledEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("submission_settled"),
    submissionId: Schema.String,
    outcome: Schema.Literals(["completed", "failed", "cancelled"]),
    result: Schema.optionalKey(Schema.Unknown),
    error: Schema.optionalKey(Schema.Unknown),
  }),
  [PublicRunEventRest],
);

const PublicConversationIdleEvent = Schema.StructWithRest(
  Schema.Struct({
    ...publicConversationEventEnvelope,
    type: Schema.Literal("idle"),
  }),
  [PublicRunEventRest],
);

export const PublicConversationEvent = Schema.Union([
  PublicConversationMessageEvent,
  PublicConversationDeltaEvent,
  PublicConversationThinkingStartEvent,
  PublicConversationThinkingDeltaEvent,
  PublicConversationThinkingEndEvent,
  PublicConversationToolStartEvent,
  PublicConversationToolResultEvent,
  PublicConversationAgentMarkerEvent,
  PublicConversationTurnEvent,
  PublicConversationSubmissionSettledEvent,
  PublicConversationIdleEvent,
]);

export const PublicStreamEvent = Schema.Union([PublicRunEvent, PublicConversationEvent]);

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
