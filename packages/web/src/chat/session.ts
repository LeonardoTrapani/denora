import { FetchError, type LiveMode } from "@durable-streams/client";

import { Api, type DenoraApiClient } from "../lib/api.ts";
import {
  type ChatReducerEvent,
  type ChatState,
  type PersistedConversationMessage,
  emptyChatState,
  reduceChatEvent,
} from "./reducer.ts";
import { createConversationEventStream, type ConversationEventStream } from "./stream.ts";
import type { ChatSnapshot, DenoraConversationEvent } from "./types.ts";

export type ChatHistory = number | "all";

export interface SendMessageResult {
  readonly conversationId: string;
}

export interface SendMessageImage {
  readonly data: string;
  readonly mimeType: string;
}

export interface SendMessageOptions {
  readonly images?: ReadonlyArray<SendMessageImage> | undefined;
  readonly modelId?: string | undefined;
  readonly thinkingLevel?: string | undefined;
}

export interface ConversationClient {
  readonly createConversation: () => Promise<{ readonly id: string }>;
  readonly submitMessage: (
    conversationId: string,
    message: string,
    options?: SendMessageOptions | undefined,
  ) => Promise<{
    readonly conversationId: string;
    readonly submissionId: string;
    readonly offset: string;
  }>;
  readonly stream: (options: {
    readonly conversationId: string;
    readonly offset?: string | undefined;
    readonly tail?: number | undefined;
    readonly live?: LiveMode | undefined;
  }) => ConversationEventStream;
}

export interface ChatSessionOptions {
  readonly conversationId?: string | undefined;
  readonly history?: ChatHistory | undefined;
  readonly live?: LiveMode | undefined;
  readonly initialMessages?: ReadonlyArray<PersistedConversationMessage> | undefined;
  readonly client?: ConversationClient | undefined;
  readonly onConversationCreated?: ((conversationId: string, session: Session) => void) | undefined;
}

const emptySnapshot: ChatSnapshot = {
  conversationId: undefined,
  messages: [],
  status: "idle",
  historyReady: true,
  error: undefined,
};

export class Session {
  private state: ChatState;
  private snapshot: ChatSnapshot;
  private readonly listeners = new Set<() => void>();
  private stream: ConversationEventStream | undefined;
  private disposed = false;
  private active = false;
  private generation = 0;
  private dormantFresh = false;
  private reconnectOffset: string | undefined;
  private admittedOffset: string | undefined;
  private reconnectAttempt = 0;
  private reconnectWake: (() => void) | undefined;
  private hydrationState: ChatState;
  private hydrationOffset: string | undefined;
  private hydrationLocalEvents: ChatReducerEvent[] = [];
  private localId = 0;
  private conversationId: string | undefined;
  private readonly history: ChatHistory;
  private readonly live: LiveMode;
  private readonly client: ConversationClient;
  private readonly onConversationCreated:
    | ((conversationId: string, session: Session) => void)
    | undefined;

  constructor(options: ChatSessionOptions = {}) {
    this.conversationId = options.conversationId;
    this.history = options.history ?? 100;
    this.live = options.live ?? "sse";
    this.client = options.client ?? defaultConversationClient;
    this.onConversationCreated = options.onConversationCreated;

    const initial = initialState(options.conversationId, options.initialMessages);
    this.state = initial;
    this.reconnectOffset = options.initialMessages === undefined ? undefined : "-1";
    this.hydrationState = { ...emptyChatState, conversationId: options.conversationId };
    this.snapshot = publicSnapshot(this.state);
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.disposed = false;
    this.generation++;
    if (this.conversationId === undefined) {
      this.publish();
      return;
    }
    void (this.state.historyReady ? this.connect(this.generation) : this.hydrate(this.generation));
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): ChatSnapshot => this.snapshot;

  async sendMessage(message: string, options: SendMessageOptions = {}): Promise<SendMessageResult> {
    const trimmed = message.trim();
    const sendOptions = normalizeSendOptions(options);
    const images = sendOptions?.images ?? [];
    if (trimmed.length === 0 && images.length === 0) {
      if (this.conversationId === undefined) throw new Error("Cannot send an empty message.");
      return { conversationId: this.conversationId };
    }

    const localId = `local:${++this.localId}`;
    const content = sendContent(trimmed, sendOptions ?? {});
    this.dispatch({ type: "local_send_submitted", localId, content });
    this.wakeReconnect();

    let conversationId = this.conversationId;
    try {
      if (conversationId === undefined) {
        conversationId = makeConversationId();
        this.conversationId = conversationId;
        this.dormantFresh = false;
        this.dispatch({ type: "local_conversation_created", conversationId });
        this.onConversationCreated?.(conversationId, this);
      }

      const receipt =
        sendOptions === undefined
          ? await this.client.submitMessage(conversationId, trimmed)
          : await this.client.submitMessage(conversationId, trimmed, sendOptions);
      this.conversationId = receipt.conversationId;
      this.admittedOffset = receipt.offset;
      this.dispatch({
        type: "local_send_admitted",
        localId,
        submissionId: receipt.submissionId,
      });

      if (this.dormantFresh) {
        this.dormantFresh = false;
        this.reconnectOffset = receipt.offset;
      }
      if (this.active && this.state.historyReady && this.stream === undefined) {
        queueMicrotask(() => void this.connect(this.generation));
      }
      return { conversationId: receipt.conversationId };
    } catch (error) {
      const normalized = toError(error);
      this.dispatch({ type: "local_send_failed", localId, error: normalized });
      throw error;
    }
  }

  dispose(): void {
    if (!this.active) return;
    this.active = false;
    this.disposed = true;
    this.generation++;
    this.stream?.cancel();
    this.stream = undefined;
    this.wakeReconnect();
  }

  private async hydrate(generation = this.generation): Promise<void> {
    if (
      !this.isCurrent(generation) ||
      this.stream !== undefined ||
      this.dormantFresh ||
      this.state.historyReady ||
      this.conversationId === undefined
    ) {
      return;
    }

    this.dispatch({ type: "local_connecting", error: this.snapshot.error });
    const options = this.hydrationOffset
      ? { live: false as const, offset: this.hydrationOffset }
      : this.history === "all"
        ? { live: false as const, offset: "-1" }
        : { live: false as const, offset: "-1", tail: this.history };

    let stream: ConversationEventStream;
    try {
      stream = this.client.stream({ conversationId: this.conversationId, ...options });
    } catch (error) {
      if (isStatus(error, 404)) {
        this.dormantFresh = true;
        this.dispatch({ type: "local_stream_not_found" });
      } else if (isFatal(error)) {
        this.dispatch({ type: "local_stream_failed", error: toError(error) });
      } else {
        await this.retry(toError(error), generation, "hydrate");
      }
      return;
    }

    this.stream = stream;
    try {
      for await (const event of stream) {
        if (!this.isCurrent(generation)) return;
        this.hydrationState = reduceChatEvent(this.hydrationState, event);
      }
      if (!this.isCurrent(generation) || this.stream !== stream) return;
      this.reconnectAttempt = 0;
      this.commitHydration(stream.offset, generation);
    } catch (error) {
      if (!this.isCurrent(generation) || this.stream !== stream) return;
      this.hydrationOffset = stream.offset !== "-1" ? stream.offset : this.hydrationOffset;
      if (isStatus(error, 404)) {
        if (this.admittedOffset !== undefined) {
          this.commitHydration(this.admittedOffset, generation);
        } else {
          this.dormantFresh = true;
          this.dispatch({ type: "local_stream_not_found" });
        }
        return;
      }
      if (isFatal(error)) {
        this.dispatch({ type: "local_stream_failed", error: toError(error) });
        return;
      }
      await this.retry(toError(error), generation, "hydrate");
    } finally {
      if (this.stream === stream) this.stream = undefined;
    }
  }

  private commitHydration(offset: string, generation: number): void {
    this.reconnectOffset = offset;
    this.state = this.hydrationLocalEvents.reduce(reduceChatEvent, this.hydrationState);
    this.state = reduceChatEvent(this.state, { type: "local_history_ready" });
    this.hydrationLocalEvents = [];
    this.publish();
    this.stream = undefined;
    queueMicrotask(() => void this.connect(generation));
  }

  private async connect(generation = this.generation): Promise<void> {
    if (
      !this.isCurrent(generation) ||
      this.stream !== undefined ||
      this.dormantFresh ||
      !this.state.historyReady ||
      this.conversationId === undefined
    ) {
      return;
    }

    const offset = this.reconnectOffset ?? this.admittedOffset;
    if (offset === undefined) return;
    this.dispatch({ type: "local_connecting", error: this.snapshot.error });

    let stream: ConversationEventStream;
    try {
      stream = this.client.stream({ conversationId: this.conversationId, live: this.live, offset });
    } catch (error) {
      if (isFatal(error)) {
        this.dispatch({ type: "local_stream_failed", error: toError(error) });
      } else {
        await this.retry(toError(error), generation, "connect");
      }
      return;
    }

    this.stream = stream;
    let delivered = false;
    try {
      for await (const event of stream) {
        if (!this.isCurrent(generation)) return;
        delivered = true;
        this.reconnectAttempt = 0;
        this.dispatch(event);
      }
      if (this.isCurrent(generation) && this.stream === stream) {
        this.reconnectOffset = stream.offset;
        await this.retry(
          new Error("Conversation event stream ended unexpectedly"),
          generation,
          "connect",
        );
      }
    } catch (error) {
      if (!this.isCurrent(generation) || this.stream !== stream) return;
      this.reconnectOffset = delivered ? stream.offset : this.reconnectOffset;
      if (!delivered && isStatus(error, 404) && this.admittedOffset !== undefined) {
        // Flue-style first prompts can return a pre-admission offset before the
        // coordinator-created stream exists. Treat that initial 404 as a race,
        // not a fatal missing conversation.
        this.reconnectOffset = this.admittedOffset;
        await this.retry(toError(error), generation, "connect", {
          baseDelayMs: 50,
          maxDelayMs: 1_000,
        });
        return;
      }
      if (isFatal(error)) {
        this.dispatch({ type: "local_stream_failed", error: toError(error) });
        return;
      }
      await this.retry(toError(error), generation, "connect");
    } finally {
      if (this.stream === stream) this.stream = undefined;
    }
  }

  private async retry(
    error: Error,
    generation = this.generation,
    phase: "hydrate" | "connect" = "connect",
    options: {
      readonly baseDelayMs?: number | undefined;
      readonly maxDelayMs?: number | undefined;
    } = {},
  ): Promise<void> {
    if (!this.isCurrent(generation)) return;
    this.dispatch({ type: "local_connecting", error });
    const baseDelayMs = options.baseDelayMs ?? 1_000;
    const maxDelayMs = options.maxDelayMs ?? 30_000;
    const delay = Math.min(baseDelayMs * 2 ** this.reconnectAttempt++, maxDelayMs);
    await new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        this.reconnectWake = undefined;
        resolve();
      }, delay);
      this.reconnectWake = () => {
        clearTimeout(timer);
        this.reconnectWake = undefined;
        resolve();
      };
    });
    if (this.isCurrent(generation)) {
      setTimeout(
        () => void (phase === "hydrate" ? this.hydrate(generation) : this.connect(generation)),
        0,
      );
    }
  }

  private isCurrent(generation: number): boolean {
    return this.active && !this.disposed && generation === this.generation;
  }

  private wakeReconnect(): void {
    this.reconnectWake?.();
  }

  private dispatch(event: ChatReducerEvent | DenoraConversationEvent): void {
    if (
      !this.state.historyReady &&
      !("eventIndex" in event) &&
      (event.type === "local_send_submitted" ||
        event.type === "local_send_admitted" ||
        event.type === "local_send_failed" ||
        event.type === "local_conversation_created")
    ) {
      this.hydrationLocalEvents.push(event);
    }
    const next = reduceChatEvent(this.state, event);
    if (next === this.state) return;
    this.state = next;
    this.publish();
  }

  private publish(): void {
    this.snapshot = publicSnapshot(this.state);
    for (const listener of this.listeners) listener();
  }
}

export const defaultConversationClient: ConversationClient = {
  createConversation: () =>
    Api.runApi(
      Api.apiEffect((client: DenoraApiClient) =>
        client.createConversation({ payload: { title: "New conversation" } }),
      ),
      { span: "chat.createConversation" },
    ),
  submitMessage: (conversationId, message, options) =>
    Api.runApi(
      Api.apiEffect((client: DenoraApiClient) =>
        client.submitConversationMessage({
          params: { conversationId },
          payload: { content: sendContent(message, options ?? {}) },
        }),
      ),
      { span: "chat.submitConversationMessage" },
    ),
  stream: (options) => createConversationEventStream(options),
};

function initialState(
  conversationId: string | undefined,
  initialMessages: ReadonlyArray<PersistedConversationMessage> | undefined,
): ChatState {
  if (conversationId === undefined) return { ...emptyChatState, historyReady: true };
  if (initialMessages === undefined) return { ...emptyChatState, conversationId };
  const loaded = reduceChatEvent(
    { ...emptyChatState, conversationId },
    { type: "local_history_loaded", messages: initialMessages },
  );
  return reduceChatEvent(loaded, { type: "local_history_ready" });
}

function publicSnapshot(state: ChatState): ChatSnapshot {
  return {
    conversationId: state.conversationId,
    messages: state.messages,
    status: state.status,
    historyReady: state.historyReady,
    error: state.error,
  };
}

function makeConversationId(): string {
  return `conversation_${crypto.randomUUID()}`;
}

function normalizeSendOptions(options: SendMessageOptions): SendMessageOptions | undefined {
  const images = options.images?.filter(
    (image) => image.data.length > 0 && image.mimeType.length > 0,
  );
  const normalized = {
    ...(images === undefined || images.length === 0 ? {} : { images }),
    ...(options.modelId === undefined ? {} : { modelId: options.modelId }),
    ...(options.thinkingLevel === undefined ? {} : { thinkingLevel: options.thinkingLevel }),
  };
  return Object.keys(normalized).length === 0 ? undefined : normalized;
}

function sendContent(message: string, options: SendMessageOptions): unknown {
  return {
    text: message,
    ...(options.images === undefined || options.images.length === 0
      ? {}
      : { images: options.images.map((image) => ({ type: "image", ...image })) }),
    ...(options.modelId === undefined ? {} : { modelId: options.modelId }),
    ...(options.thinkingLevel === undefined ? {} : { thinkingLevel: options.thinkingLevel }),
  };
}

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isStatus(error: unknown, status: number): boolean {
  return error instanceof FetchError && error.status === status;
}

function isFatal(error: unknown): boolean {
  return isStatus(error, 401) || isStatus(error, 403) || isStatus(error, 404);
}

export const emptyChatSnapshot = emptySnapshot;

export * as ChatSession from "./session.ts";
