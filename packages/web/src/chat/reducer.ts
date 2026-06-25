import type {
  ChatMessage,
  ChatMessagePart,
  ChatSnapshot,
  ChatStatus,
  DenoraConversationEvent,
} from "./types.ts";

interface PendingSend {
  readonly localId: string;
  readonly submissionId?: string | undefined;
}

export interface ChatState extends ChatSnapshot {
  readonly pendingSends: ReadonlyArray<PendingSend>;
  readonly activeSubmissionIds: ReadonlyArray<string>;
  readonly settledSubmissionIds: ReadonlyArray<string>;
  readonly recentEventIds: ReadonlyArray<string>;
  readonly reasoningPartIndexes: Readonly<Record<string, Readonly<Record<number, number>>>>;
}

export type ChatReducerEvent =
  | DenoraConversationEvent
  | { readonly type: "local_conversation_created"; readonly conversationId: string }
  | { readonly type: "local_send_submitted"; readonly localId: string; readonly message: string }
  | {
      readonly type: "local_send_admitted";
      readonly localId: string;
      readonly submissionId: string;
    }
  | { readonly type: "local_send_failed"; readonly localId: string; readonly error: Error }
  | {
      readonly type: "local_status";
      readonly status: Extract<ChatStatus, "hydrating" | "connecting">;
      readonly error?: Error | undefined;
    }
  | { readonly type: "local_history_ready" }
  | { readonly type: "local_stream_failed"; readonly error: Error };

export const emptyChatState: ChatState = {
  conversationId: undefined,
  messages: [],
  status: "idle",
  historyReady: false,
  error: undefined,
  pendingSends: [],
  activeSubmissionIds: [],
  settledSubmissionIds: [],
  recentEventIds: [],
  reasoningPartIndexes: {},
};

const IMAGE_DATA_OMITTED = "[image data omitted from event]";

const RECENT_EVENT_LIMIT = 1_000;

export function reduceChatEvent(state: ChatState, event: ChatReducerEvent): ChatState {
  if (!isStreamEvent(event)) return reduceChatEventOnce(state, event);
  const id = streamEventId(event);
  if (state.recentEventIds.includes(id)) return state;
  const next = reduceChatEventOnce(state, event);
  if (next === state) return state;
  return { ...next, recentEventIds: [...state.recentEventIds, id].slice(-RECENT_EVENT_LIMIT) };
}

function reduceChatEventOnce(state: ChatState, event: ChatReducerEvent): ChatState {
  switch (event.type) {
    case "local_conversation_created":
      return { ...state, conversationId: event.conversationId };
    case "local_send_submitted":
      return {
        ...state,
        messages: [...state.messages, optimisticMessage(event.localId, event.message)],
        status: "submitted",
        error: undefined,
        pendingSends: [...state.pendingSends, { localId: event.localId }],
      };
    case "local_send_admitted": {
      const durableUserId = userMessageId(event.submissionId);
      const hasEcho = state.messages.some((message) => message.id === durableUserId);
      const settled = state.settledSubmissionIds.includes(event.submissionId);
      const active = state.activeSubmissionIds.includes(event.submissionId);
      return {
        ...state,
        messages: hasEcho
          ? reconcileMessageIdentity(state.messages, event.localId, durableUserId)
          : state.messages,
        status: active
          ? "streaming"
          : settled
            ? statusWithout(event.localId, state.pendingSends)
            : state.status,
        pendingSends: settled
          ? state.pendingSends.filter((send) => send.localId !== event.localId)
          : state.pendingSends.map((send) =>
              send.localId === event.localId ? { ...send, submissionId: event.submissionId } : send,
            ),
      };
    }
    case "local_send_failed":
      return {
        ...state,
        messages: state.messages.filter((message) => message.id !== event.localId),
        status: "error",
        error: event.error,
        pendingSends: state.pendingSends.filter((send) => send.localId !== event.localId),
      };
    case "local_status":
      return state.status === "error"
        ? state
        : { ...state, status: event.status, error: event.error };
    case "local_history_ready":
      return {
        ...state,
        historyReady: true,
        status:
          state.status === "error" ? "error" : state.pendingSends.length > 0 ? "submitted" : "idle",
        error: state.status === "error" ? state.error : undefined,
      };
    case "local_stream_failed":
      return { ...state, status: "error", error: event.error };
    case "message_start":
    case "message_end":
      return reduceMessageBoundary(state, event);
    case "text_delta":
      return reduceTextDelta(state, event);
    case "thinking_start":
      return reduceThinkingStart(state, event);
    case "thinking_delta":
      return reduceThinkingDelta(state, event);
    case "thinking_end":
      return reduceThinkingEnd(state, event);
    case "tool_start":
      return reduceToolStart(state, event);
    case "tool":
      return reduceToolResult(state, event);
    case "turn":
      return reduceTurn(state, event);
    case "submission_settled":
      return reduceSubmissionSettled(state, event);
    case "idle":
      return reduceIdle(state, event);
    default:
      return state;
  }
}

function reduceMessageBoundary(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "message_start" | "message_end" }>,
): ChatState {
  const message = toMessage(event.message);
  if (message === undefined) return state;
  const durableId = messageId(event, message.role);
  const existing = state.messages.find((item) => item.id === durableId);
  const pending =
    message.role === "user" && event.submissionId !== undefined
      ? state.pendingSends.find((send) => send.submissionId === event.submissionId)
      : undefined;
  const optimistic =
    pending === undefined ? undefined : state.messages.find((item) => item.id === pending.localId);
  const nextMessage = snapshotMessage(
    durableId,
    message,
    event.type === "message_end",
    optimistic ?? existing,
  );
  const messages = pending
    ? replaceOptimisticMessage(state.messages, pending.localId, durableId, nextMessage)
    : replaceById(state.messages, durableId, nextMessage);
  const assistantForPending =
    message.role === "assistant" &&
    event.submissionId !== undefined &&
    state.pendingSends.some((send) => send.submissionId === event.submissionId);
  return {
    ...state,
    conversationId: state.conversationId ?? event.instanceId,
    messages,
    status: assistantForPending ? "streaming" : state.status,
    activeSubmissionIds:
      message.role === "assistant" && event.submissionId !== undefined
        ? addUnique(state.activeSubmissionIds, event.submissionId)
        : state.activeSubmissionIds,
    reasoningPartIndexes:
      message.role === "assistant"
        ? { ...state.reasoningPartIndexes, [durableId]: reasoningIndexes(message) }
        : state.reasoningPartIndexes,
  };
}

function reduceTextDelta(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "text_delta" }>,
): ChatState {
  const index = findEventAssistant(state.messages, event);
  if (index < 0) return state;
  const current = state.messages[index];
  if (current === undefined) return state;
  const parts = [...current.parts];
  const last = parts.at(-1);
  if (last?.type === "text" && last.state !== "done") {
    parts[parts.length - 1] = { ...last, text: last.text + event.text, state: "streaming" };
  } else {
    parts.push({ type: "text", text: event.text, state: "streaming" });
  }
  return replaceMessageAt({ ...state, status: "streaming" }, index, { ...current, parts });
}

function reduceThinkingStart(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "thinking_start" }>,
): ChatState {
  const index = findEventAssistant(state.messages, event);
  if (index < 0) return state;
  const current = state.messages[index];
  if (current === undefined) return state;
  const known =
    event.contentIndex === undefined
      ? undefined
      : state.reasoningPartIndexes[current.id]?.[event.contentIndex];
  if (known !== undefined && current.parts[known]?.type === "reasoning") return state;
  const partIndex = current.parts.length;
  const next = replaceMessageAt({ ...state, status: "streaming" }, index, {
    ...current,
    parts: [...current.parts, { type: "reasoning", text: "", state: "streaming" }],
  });
  return event.contentIndex === undefined
    ? next
    : {
        ...next,
        reasoningPartIndexes: setReasoningPartIndex(
          next.reasoningPartIndexes,
          current.id,
          event.contentIndex,
          partIndex,
        ),
      };
}

function reduceThinkingDelta(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "thinking_delta" }>,
): ChatState {
  const index = findEventAssistant(state.messages, event);
  if (index < 0) return state;
  const current = state.messages[index];
  if (current === undefined) return state;
  const reasoning =
    event.contentIndex === undefined
      ? current.parts.findLastIndex((part) => part.type === "reasoning" && part.state !== "done")
      : state.reasoningPartIndexes[current.id]?.[event.contentIndex];
  if (reasoning === undefined || reasoning < 0) return state;
  const part = current.parts[reasoning];
  if (part?.type !== "reasoning" || part.state === "done") return state;
  const parts = [...current.parts];
  parts[reasoning] = { ...part, text: part.text + event.delta, state: "streaming" };
  return replaceMessageAt({ ...state, status: "streaming" }, index, { ...current, parts });
}

function reduceThinkingEnd(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "thinking_end" }>,
): ChatState {
  const index = findEventAssistant(state.messages, event);
  if (index < 0) return state;
  const current = state.messages[index];
  if (current === undefined) return state;
  const reasoning =
    event.contentIndex === undefined
      ? current.parts.findLastIndex((part) => part.type === "reasoning")
      : state.reasoningPartIndexes[current.id]?.[event.contentIndex];
  if (reasoning === undefined || reasoning < 0) return state;
  const part = current.parts[reasoning];
  if (part?.type !== "reasoning") return state;
  const parts = [...current.parts];
  parts[reasoning] = { ...part, text: event.content, state: "done" };
  return replaceMessageAt(state, index, { ...current, parts });
}

function reduceToolStart(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "tool_start" }>,
): ChatState {
  let messages = state.messages;
  let index = findToolMessage(messages, event.toolCallId);
  if (index < 0) index = findEventAssistant(messages, event);
  if (index < 0) {
    const id = event.turnId === undefined ? `tool:${event.toolCallId}` : `turn:${event.turnId}`;
    messages = [...messages, { id, role: "assistant", parts: [] }];
    index = messages.length - 1;
  }
  const current = messages[index];
  if (current === undefined) return state;
  const input = event.input ?? event.args;
  const exists = current.parts.some(
    (part) => part.type === "dynamic-tool" && part.toolCallId === event.toolCallId,
  );
  const parts: ChatMessagePart[] = exists
    ? current.parts.map((part) =>
        part.type === "dynamic-tool" && part.toolCallId === event.toolCallId
          ? {
              type: "dynamic-tool",
              toolName: event.toolName,
              toolCallId: part.toolCallId,
              input: input ?? part.input,
              state: "input-available",
            }
          : part,
      )
    : [
        ...current.parts,
        {
          type: "dynamic-tool",
          toolName: event.toolName,
          toolCallId: event.toolCallId,
          state: "input-available",
          input,
        },
      ];
  return replaceMessageAt({ ...state, messages, status: "streaming" }, index, {
    ...current,
    parts,
  });
}

function reduceToolResult(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "tool" }>,
): ChatState {
  let next = state;
  let index = findToolMessage(next.messages, event.toolCallId);
  if (index < 0) {
    next = reduceToolStart(next, {
      ...event,
      type: "tool_start",
      input: undefined,
    });
    index = findToolMessage(next.messages, event.toolCallId);
  }
  if (index < 0) return state;
  const current = next.messages[index];
  if (current === undefined) return state;
  const parts = current.parts.map((part) => {
    if (part.type !== "dynamic-tool" || part.toolCallId !== event.toolCallId) return part;
    return event.isError
      ? {
          type: "dynamic-tool" as const,
          toolName: part.toolName,
          toolCallId: part.toolCallId,
          input: part.input,
          state: "output-error" as const,
          errorText: errorText(event.result),
        }
      : {
          type: "dynamic-tool" as const,
          toolName: part.toolName,
          toolCallId: part.toolCallId,
          input: part.input,
          state: "output-available" as const,
          output: event.result,
        };
  });
  return replaceMessageAt(next, index, { ...current, parts });
}

function reduceTurn(
  state: ChatState,
  event: DenoraConversationEvent & { readonly type: "turn" },
): ChatState {
  let index =
    event.turnId === undefined
      ? -1
      : state.messages.findIndex((item) => item.id === `turn:${event.turnId}`);
  if (index < 0) index = findLastAssistant(state.messages);
  if (index < 0) return state;
  const current = state.messages[index];
  if (current === undefined) return state;
  const request = objectRecord((event as { readonly request?: unknown }).request);
  const response = objectRecord((event as { readonly response?: unknown }).response);
  const provider = stringField(request, "providerId");
  const modelId = stringField(request, "requestedModel");
  const metadata = {
    ...current.metadata,
    ...(response?.usage === undefined ? {} : { usage: response.usage }),
    ...(provider === undefined || modelId === undefined
      ? {}
      : { model: { provider, id: modelId } }),
  };
  return replaceMessageAt(state, index, { ...current, metadata });
}

function reduceSubmissionSettled(
  state: ChatState,
  event: Extract<DenoraConversationEvent, { readonly type: "submission_settled" }>,
): ChatState {
  const pendingSends = state.pendingSends.filter(
    (send) => send.submissionId !== event.submissionId,
  );
  if (event.outcome === "failed" || event.outcome === "cancelled") {
    return {
      ...state,
      status: "error",
      error: new Error(errorMessage(event.error) ?? `Submission ${event.outcome}`),
      pendingSends,
      settledSubmissionIds: addUnique(state.settledSubmissionIds, event.submissionId),
    };
  }
  return {
    ...state,
    pendingSends,
    status: pendingSends.length > 0 ? "submitted" : state.status,
    settledSubmissionIds: addUnique(state.settledSubmissionIds, event.submissionId),
  };
}

function reduceIdle(state: ChatState, event: DenoraConversationEvent): ChatState {
  const pendingSends =
    event.submissionId === undefined
      ? state.pendingSends
      : state.pendingSends.filter((send) => send.submissionId !== event.submissionId);
  return {
    ...state,
    status: state.status === "error" ? "error" : pendingSends.length > 0 ? "submitted" : "idle",
    error: state.status === "error" ? state.error : undefined,
    pendingSends,
    activeSubmissionIds:
      event.submissionId === undefined
        ? state.activeSubmissionIds
        : state.activeSubmissionIds.filter((id) => id !== event.submissionId),
    settledSubmissionIds:
      event.submissionId === undefined
        ? state.settledSubmissionIds
        : addUnique(state.settledSubmissionIds, event.submissionId),
  };
}

function toMessage(
  value: unknown,
): (Pick<ChatMessage, "role"> & { readonly content: unknown }) | undefined {
  if (value === null || typeof value !== "object") return undefined;
  const record = value as { readonly role?: unknown; readonly content?: unknown };
  if (record.role !== "user" && record.role !== "assistant" && record.role !== "system")
    return undefined;
  return { role: record.role, content: record.content };
}

function snapshotMessage(
  id: string,
  message: Pick<ChatMessage, "role"> & { readonly content: unknown },
  done: boolean,
  previous?: ChatMessage | undefined,
): ChatMessage {
  const parts: ChatMessagePart[] = [];
  const previousFiles = previous?.parts.filter((part) => part.type === "file") ?? [];
  let previousFileIndex = 0;
  const content =
    typeof message.content === "string"
      ? [{ type: "text", text: message.content }]
      : Array.isArray(message.content)
        ? message.content
        : [];

  for (const block of content) {
    const record = objectRecord(block);
    if (record === undefined) continue;
    if (record.type === "text" && typeof record.text === "string") {
      parts.push({ type: "text", text: record.text, state: done ? "done" : "streaming" });
    } else if (record.type === "thinking" && typeof record.thinking === "string") {
      parts.push({ type: "reasoning", text: record.thinking, state: done ? "done" : "streaming" });
    } else if (
      record.type === "toolCall" &&
      typeof record.id === "string" &&
      typeof record.name === "string"
    ) {
      const prior = previous?.parts.find(
        (part): part is Extract<ChatMessagePart, { readonly type: "dynamic-tool" }> =>
          part.type === "dynamic-tool" && part.toolCallId === record.id,
      );
      parts.push(
        prior === undefined
          ? {
              type: "dynamic-tool",
              toolName: record.name,
              toolCallId: record.id,
              state: "input-available",
              input: record.arguments,
            }
          : { ...prior, toolName: record.name, input: record.arguments },
      );
    } else if (
      record.type === "image" &&
      typeof record.data === "string" &&
      typeof record.mimeType === "string"
    ) {
      const prior = previousFiles[previousFileIndex++];
      parts.push(
        record.data === IMAGE_DATA_OMITTED && prior?.mediaType === record.mimeType
          ? prior
          : {
              type: "file",
              mediaType: record.mimeType,
              url: imageUrl(record.data, record.mimeType),
            },
      );
    }
  }
  return { id, role: message.role, metadata: previous?.metadata, parts };
}

function reasoningIndexes(message: {
  readonly role: ChatMessage["role"];
  readonly content: unknown;
}): Record<number, number> {
  if (message.role !== "assistant" || !Array.isArray(message.content)) return {};
  const indexes: Record<number, number> = {};
  let partIndex = 0;
  for (const [contentIndex, block] of message.content.entries()) {
    const record = objectRecord(block);
    if (record?.type === "thinking") indexes[contentIndex] = partIndex;
    if (isRenderableContentBlock(record)) partIndex += 1;
  }
  return indexes;
}

function setReasoningPartIndex(
  indexes: ChatState["reasoningPartIndexes"],
  messageId: string,
  contentIndex: number,
  partIndex: number,
): ChatState["reasoningPartIndexes"] {
  return { ...indexes, [messageId]: { ...indexes[messageId], [contentIndex]: partIndex } };
}

function isRenderableContentBlock(record: Readonly<Record<string, unknown>> | undefined): boolean {
  return (
    record?.type === "text" ||
    record?.type === "thinking" ||
    record?.type === "toolCall" ||
    record?.type === "image"
  );
}

function optimisticMessage(localId: string, message: string): ChatMessage {
  return {
    id: localId,
    role: "user",
    parts: [{ type: "text", text: message, state: "done" }],
  };
}

function imageUrl(data: string, mimeType: string): string {
  if (data === IMAGE_DATA_OMITTED || data.startsWith("data:")) return data;
  return `data:${mimeType};base64,${data}`;
}

function streamEventId(event: DenoraConversationEvent): string {
  const context = event.submissionId ?? event.messageId ?? event.turnId ?? "event";
  return `${event.instanceId}:${context}:${event.eventIndex}:${event.timestamp}`;
}

function messageId(event: DenoraConversationEvent, role: ChatMessage["role"]): string {
  if (event.messageId !== undefined) return event.messageId;
  if (role === "assistant" && event.turnId !== undefined) return `turn:${event.turnId}`;
  if (role === "user" && event.submissionId !== undefined) return userMessageId(event.submissionId);
  return `event:${event.eventIndex}:${role}`;
}

function userMessageId(submissionId: string): string {
  return `submission:${submissionId}:user`;
}

function findEventAssistant(
  messages: ReadonlyArray<ChatMessage>,
  event: DenoraConversationEvent,
): number {
  if (event.turnId !== undefined) {
    const index = messages.findIndex((message) => message.id === `turn:${event.turnId}`);
    if (index >= 0) return index;
  }
  return messages.findLastIndex((message) => message.role === "assistant");
}

function findLastAssistant(messages: ReadonlyArray<ChatMessage>): number {
  return messages.findLastIndex((message) => message.role === "assistant");
}

function findToolMessage(messages: ReadonlyArray<ChatMessage>, toolCallId: string): number {
  return messages.findIndex((message) =>
    message.parts.some((part) => part.type === "dynamic-tool" && part.toolCallId === toolCallId),
  );
}

function replaceOptimisticMessage(
  messages: ReadonlyArray<ChatMessage>,
  localId: string,
  durableId: string,
  message: ChatMessage,
): ReadonlyArray<ChatMessage> {
  const withoutDurable = messages.filter((item) => item.id !== durableId);
  const localIndex = withoutDurable.findIndex((item) => item.id === localId);
  if (localIndex < 0) return replaceById(withoutDurable, durableId, message);
  const next = [...withoutDurable];
  next[localIndex] = message;
  return next;
}

function reconcileMessageIdentity(
  messages: ReadonlyArray<ChatMessage>,
  localId: string,
  durableId: string,
): ReadonlyArray<ChatMessage> {
  const localIndex = messages.findIndex((message) => message.id === localId);
  const durableIndex = messages.findIndex((message) => message.id === durableId);
  if (localIndex < 0 || durableIndex < 0)
    return messages.filter((message) => message.id !== localId);
  if (durableIndex < localIndex) return messages.filter((message) => message.id !== localId);
  const durable = messages[durableIndex];
  if (durable === undefined) return messages;
  const next = messages.filter((message) => message.id !== durableId);
  const targetIndex = next.findIndex((message) => message.id === localId);
  if (targetIndex < 0) return next;
  next[targetIndex] = durable;
  return next;
}

function replaceById(
  messages: ReadonlyArray<ChatMessage>,
  id: string,
  message: ChatMessage,
): ReadonlyArray<ChatMessage> {
  const index = messages.findIndex((item) => item.id === id);
  if (index < 0) return [...messages, message];
  const next = [...messages];
  next[index] = message;
  return next;
}

function replaceMessageAt(state: ChatState, index: number, message: ChatMessage): ChatState {
  const messages = [...state.messages];
  messages[index] = message;
  return { ...state, messages };
}

function statusWithout(localId: string, pendingSends: ReadonlyArray<PendingSend>): ChatStatus {
  return pendingSends.some((send) => send.localId !== localId) ? "submitted" : "idle";
}

function addUnique(values: ReadonlyArray<string>, value: string): ReadonlyArray<string> {
  return values.includes(value) ? values : [...values, value];
}

function errorMessage(value: unknown): string | undefined {
  if (value instanceof Error) return value.message;
  if (typeof value === "string") return value;
  if (value !== null && typeof value === "object") {
    const message = (value as { readonly message?: unknown }).message;
    if (typeof message === "string") return message;
  }
  return undefined;
}

function errorText(value: unknown): string {
  const message = errorMessage(value);
  if (message !== undefined) return message;
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return String(value);
  }
}

function objectRecord(value: unknown): Readonly<Record<string, unknown>> | undefined {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? (value as Readonly<Record<string, unknown>>)
    : undefined;
}

function stringField(
  record: Readonly<Record<string, unknown>> | undefined,
  field: string,
): string | undefined {
  const value = record?.[field];
  return typeof value === "string" ? value : undefined;
}

function isStreamEvent(event: ChatReducerEvent): event is DenoraConversationEvent {
  return "eventIndex" in event;
}

export * as ChatReducer from "./reducer.ts";
