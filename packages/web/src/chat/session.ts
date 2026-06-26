import { FetchError, type LiveMode } from "@durable-streams/client";

import { apiEffect, runApi } from "../lib/api.ts";
import { createConversationEventStream, type ConversationEventStream } from "./stream.ts";
import {
  type ChatReducerEvent,
  type ChatState,
  type PersistedConversationMessage,
  emptyChatState,
  reduceChatEvent,
} from "./reducer.ts";
import type { ChatSnapshot, DenoraConversationEvent } from "./types.ts";

export type ChatHistory = number | "all";

export interface ConversationChatSessionOptions {
  readonly conversationId?: string | undefined;
  readonly history?: ChatHistory | undefined;
  readonly live?: LiveMode | undefined;
  readonly resetKey?: unknown;
}

export class ConversationChatSession {
  private readonly options: ConversationChatSessionOptions;
  private state: ChatState;
  private snapshot: ChatSnapshot;
  private listeners = new Set<() => void>();
  private stream: ConversationEventStream | undefined;
  private active = false;
  private generation = 0;
  private reconnectOffset: string | undefined;
  private admittedOffset: string | undefined;
  private reconnectAttempt = 0;
  private localId = 0;
  private hydrationState: ChatState;
  private hydrationLocalEvents: ChatReducerEvent[] = [];

  constructor(options: ConversationChatSessionOptions = {}) {
    this.options = options;
    this.state = {
      ...emptyChatState,
      conversationId: options.conversationId,
      historyReady: options.conversationId === undefined,
    };
    this.hydrationState = { ...emptyChatState, conversationId: options.conversationId };
    this.snapshot = publicSnapshot(this.state);
  }

  start(): void {
    if (this.active) return;
    this.active = true;
    this.generation += 1;
    if (this.state.conversationId !== undefined && !this.state.historyReady) {
      void this.hydrate(this.generation);
    }
  }

  dispose(): void {
    if (!this.active) return;
    this.active = false;
    this.generation += 1;
    this.stream?.cancel();
    this.stream = undefined;
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  getSnapshot = (): ChatSnapshot => this.snapshot;

  async sendMessage(message: string): Promise<void> {
    const localId = `local:${++this.localId}`;
    this.dispatch({ type: "local_send_submitted", localId, message });
    try {
      const conversationId = await this.ensureConversation();
      const receipt = await runApi(
        apiEffect((client) =>
          client.submitConversationMessage({
            params: { conversationId },
            payload: { message },
          }),
        ),
        { span: "chat.submit-message" },
      );
      this.dispatch({
        type: "local_send_admitted",
        localId,
        submissionId: receipt.submissionId,
      });
      this.admittedOffset = receipt.offset;
      this.reconnectOffset = receipt.offset;
      if (this.active && this.stream === undefined) {
        queueMicrotask(() => void this.connect(this.generation));
      }
    } catch (error) {
      const normalized = toError(error);
      this.dispatch({ type: "local_send_failed", localId, error: normalized });
      throw error;
    }
  }

  private async ensureConversation(): Promise<string> {
    if (this.state.conversationId !== undefined) return this.state.conversationId;
    const conversation = await runApi(
      apiEffect((client) =>
        client.createConversation({
          payload: { title: "New conversation" },
        }),
      ),
      { span: "chat.create-conversation" },
    );
    this.dispatch({ type: "local_conversation_created", conversationId: conversation.id });
    this.dispatch({ type: "local_history_ready" });
    return conversation.id;
  }

  private async hydrate(generation: number): Promise<void> {
    const conversationId = this.state.conversationId;
    if (!this.isCurrent(generation) || conversationId === undefined || this.stream !== undefined)
      return;
    this.dispatch({ type: "local_status", status: "hydrating" });
    const stream = createConversationEventStream({
      conversationId,
      live: false,
      offset: "-1",
      ...(this.options.history === "all" ? {} : { tail: this.options.history ?? 100 }),
    });
    this.hydrationState = { ...emptyChatState, conversationId };
    this.stream = stream;
    try {
      for await (const event of stream) {
        if (!this.isCurrent(generation)) return;
        this.hydrationState = reduceChatEvent(this.hydrationState, event);
      }
      if (!this.isCurrent(generation) || this.stream !== stream) return;
      this.reconnectOffset = stream.offset;
      this.commitHydration();
      this.stream = undefined;
      queueMicrotask(() => void this.connect(generation));
    } catch (error) {
      if (!this.isCurrent(generation) || this.stream !== stream) return;
      this.reconnectOffset = stream.offset !== "-1" ? stream.offset : this.reconnectOffset;
      if (isStatus(error, 404)) {
        this.stream = undefined;
        await this.hydrateFromPersistedMessages(conversationId, generation);
        queueMicrotask(() => void this.connect(generation));
        return;
      }
      await this.retry(toError(error), generation, "hydrate");
    } finally {
      if (this.stream === stream) this.stream = undefined;
    }
  }

  private async hydrateFromPersistedMessages(
    conversationId: string,
    generation: number,
  ): Promise<void> {
    try {
      const messages = await runApi(
        apiEffect((client) =>
          client.listConversationMessages({
            params: { conversationId },
          }),
        ),
        { span: "chat.list-messages" },
      );
      if (!this.isCurrent(generation)) return;
      this.hydrationState = reduceChatEvent(
        { ...emptyChatState, conversationId },
        {
          type: "local_history_loaded",
          messages: messages as ReadonlyArray<PersistedConversationMessage>,
        },
      );
      this.commitHydration();
    } catch (error) {
      if (!this.isCurrent(generation)) return;
      await this.retry(toError(error), generation, "hydrate");
    }
  }

  private async connect(generation: number): Promise<void> {
    const conversationId = this.state.conversationId;
    if (
      !this.isCurrent(generation) ||
      conversationId === undefined ||
      this.stream !== undefined ||
      !this.state.historyReady
    ) {
      return;
    }
    const offset = this.reconnectOffset ?? "-1";
    this.dispatch({ type: "local_status", status: "connecting" });
    const stream = createConversationEventStream({
      conversationId,
      offset,
      live: this.options.live ?? true,
    });
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
      if (delivered) this.reconnectOffset = stream.offset;
      if (!delivered && isStatus(error, 404)) {
        if (this.admittedOffset !== undefined) {
          this.reconnectOffset = this.admittedOffset;
          await this.retry(toError(error), generation, "connect");
        } else {
          this.dispatch({ type: "local_stream_missing" });
        }
        return;
      }
      await this.retry(toError(error), generation, "connect");
    } finally {
      if (this.stream === stream) this.stream = undefined;
    }
  }

  private async retry(
    error: Error,
    generation: number,
    phase: "hydrate" | "connect",
  ): Promise<void> {
    if (!this.isCurrent(generation)) return;
    this.dispatch({
      type: "local_status",
      status: phase === "hydrate" ? "hydrating" : "connecting",
      error,
    });
    const delay = Math.min(1_000 * 2 ** this.reconnectAttempt++, 30_000);
    await new Promise((resolve) => setTimeout(resolve, delay));
    if (!this.isCurrent(generation)) return;
    setTimeout(
      () => void (phase === "hydrate" ? this.hydrate(generation) : this.connect(generation)),
      0,
    );
  }

  private dispatch(event: ChatReducerEvent | DenoraConversationEvent): void {
    if (
      !this.state.historyReady &&
      (event.type === "local_send_submitted" ||
        event.type === "local_send_admitted" ||
        event.type === "local_send_failed")
    ) {
      this.hydrationLocalEvents.push(event);
    }
    const next = reduceChatEvent(this.state, event);
    if (next === this.state) return;
    this.state = next;
    this.publish();
  }

  private commitHydration(): void {
    this.state = this.hydrationLocalEvents.reduce(reduceChatEvent, this.hydrationState);
    this.state = reduceChatEvent(this.state, { type: "local_history_ready" });
    this.hydrationLocalEvents = [];
    this.publish();
  }

  private publish(): void {
    this.snapshot = publicSnapshot(this.state);
    for (const listener of this.listeners) listener();
  }

  private isCurrent(generation: number): boolean {
    return this.active && generation === this.generation;
  }
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

function toError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function isStatus(error: unknown, status: number): boolean {
  return (
    (error instanceof FetchError && error.status === status) ||
    (typeof error === "object" &&
      error !== null &&
      "status" in error &&
      (error as { readonly status?: unknown }).status === status)
  );
}

export * as ChatSession from "./session.ts";
