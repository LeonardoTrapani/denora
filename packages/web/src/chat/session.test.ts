import { FetchError, type LiveMode } from "@durable-streams/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { type ConversationEventStream } from "./stream.ts";
import { type ConversationClient, Session } from "./session.ts";
import type { DenoraConversationEvent } from "./types.ts";
import {
  clearConversationChatSessionCache,
  releaseConversationChatSession,
  retainConversationChatSession,
} from "./useConversationChat.ts";

function streamFrom<T extends DenoraConversationEvent>(
  events: T[],
  offset = "offset-1",
): ConversationEventStream {
  return {
    offset,
    cancel: vi.fn(),
    async *[Symbol.asyncIterator]() {
      for (const event of events) yield event;
    },
  };
}

function failedStream(error: unknown, offset = "-1"): ConversationEventStream {
  return {
    offset,
    cancel: vi.fn(),
    [Symbol.asyncIterator]() {
      return {
        next: () => Promise.reject(error),
      };
    },
  };
}

function streamThenFail(
  event: DenoraConversationEvent,
  error: unknown,
  offset: string,
): ConversationEventStream {
  return {
    offset,
    cancel: vi.fn(),
    async *[Symbol.asyncIterator]() {
      yield event;
      throw error;
    },
  };
}

function pendingStream(
  offset = "-1",
): ConversationEventStream & { push(event: DenoraConversationEvent): void } {
  let canceled = false;
  let wake: (() => void) | undefined;
  const values: DenoraConversationEvent[] = [];
  return {
    offset,
    push(event) {
      values.push(event);
      wake?.();
    },
    cancel() {
      canceled = true;
      wake?.();
    },
    async *[Symbol.asyncIterator]() {
      while (!canceled) {
        const value = values.shift();
        if (value !== undefined) yield value;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
  };
}

function finiteControlledStream(offset: string): ConversationEventStream & {
  push(event: DenoraConversationEvent): void;
  finish(): void;
} {
  let done = false;
  let wake: (() => void) | undefined;
  const values: DenoraConversationEvent[] = [];
  return {
    offset,
    push(event) {
      values.push(event);
      wake?.();
    },
    finish() {
      done = true;
      wake?.();
    },
    cancel() {
      done = true;
      wake?.();
    },
    async *[Symbol.asyncIterator]() {
      while (!done || values.length > 0) {
        const value = values.shift();
        if (value !== undefined) yield value;
        else await new Promise<void>((resolve) => (wake = resolve));
      }
    },
  };
}

function client(overrides: Partial<ConversationClient>): ConversationClient {
  return {
    createConversation: vi.fn().mockResolvedValue({ id: "conversation-1" }),
    submitMessage: vi.fn().mockResolvedValue({
      conversationId: "conversation-1",
      submissionId: "submission-1",
      offset: "offset-admitted",
    }),
    stream: vi.fn(() => pendingStream()),
    ...overrides,
  } as ConversationClient;
}

async function settle(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

const base = {
  v: 3 as const,
  instanceId: "conversation-1",
  agentName: "denora",
  timestamp: "2026-06-12T00:00:00.000Z",
};

function event(value: Record<string, unknown>): DenoraConversationEvent {
  return { ...base, ...value } as DenoraConversationEvent;
}

function notFound(): FetchError {
  return new FetchError(
    404,
    "not found",
    undefined,
    {},
    "https://denora.test/conversations/1/events",
  );
}

afterEach(() => {
  vi.useRealTimers();
  clearConversationChatSessionCache();
});

describe("Session", () => {
  it("publishes initial durable history atomically when catch-up completes", async () => {
    const history = finiteControlledStream("offset-history");
    const live = pendingStream("offset-history");
    const stream = vi.fn().mockReturnValueOnce(history).mockReturnValueOnce(live);
    const session = new Session({
      conversationId: "conversation-1",
      history: "all",
      client: client({ stream }),
    });

    session.start();
    history.push(
      event({
        type: "message_end",
        message: { role: "user", content: "first" },
        eventIndex: 0,
        submissionId: "submission-1",
      }),
    );
    await settle();

    expect(session.getSnapshot()).toMatchObject({
      messages: [],
      status: "connecting",
      historyReady: false,
    });

    history.push(
      event({
        type: "message_end",
        message: { role: "assistant", content: [{ type: "text", text: "second" }] },
        eventIndex: 1,
        timestamp: "2026-06-12T00:00:01.000Z",
        turnId: "turn-1",
      }),
    );
    history.finish();
    await settle();

    expect(session.getSnapshot().historyReady).toBe(true);
    expect(session.getSnapshot().messages.map((message) => message.id)).toEqual([
      "submission:submission-1:user:0",
      "turn:turn-1",
    ]);
    await settle();
    expect(stream.mock.calls[1]?.[0]).toEqual({
      conversationId: "conversation-1",
      live: true,
      offset: "offset-history",
    });
    session.dispose();
  });

  it("retains optimistic sends made while initial history is loading", async () => {
    const history = finiteControlledStream("offset-history");
    const live = pendingStream("offset-history");
    const stream = vi.fn().mockReturnValueOnce(history).mockReturnValueOnce(live);
    const submitMessage = vi.fn().mockResolvedValue({
      conversationId: "conversation-1",
      submissionId: "submission-2",
      offset: "offset-admitted",
    });
    const session = new Session({
      conversationId: "conversation-1",
      client: client({ stream, submitMessage }),
    });

    session.start();
    history.push(
      event({
        type: "message_end",
        message: { role: "user", content: "existing" },
        eventIndex: 0,
        submissionId: "submission-1",
      }),
    );
    await session.sendMessage("new");
    history.finish();
    await settle();

    expect(session.getSnapshot().historyReady).toBe(true);
    expect(session.getSnapshot().messages.map((message) => message.parts[0])).toEqual([
      { type: "text", text: "existing", state: "done" },
      { type: "text", text: "new", state: "done" },
    ]);
    session.dispose();
  });

  it("restarts after a StrictMode-style start/dispose/start cycle", async () => {
    const stream = vi.fn(() => pendingStream());
    const session = new Session({ conversationId: "conversation-1", client: client({ stream }) });

    session.start();
    session.dispose();
    session.start();
    await settle();

    expect(stream).toHaveBeenCalledTimes(2);
    expect(session.getSnapshot().status).toBe("connecting");
    session.dispose();
  });

  it("becomes idle after a fresh stream 404 and attaches from the admission offset on send", async () => {
    const stream = vi
      .fn()
      .mockReturnValueOnce(failedStream(notFound()))
      .mockReturnValueOnce(pendingStream("offset-admitted"));
    const submitMessage = vi.fn().mockResolvedValue({
      conversationId: "conversation-1",
      submissionId: "submission-1",
      offset: "offset-admitted",
    });
    const session = new Session({
      conversationId: "conversation-1",
      client: client({ stream, submitMessage }),
    });

    session.start();
    await settle();

    expect(session.getSnapshot()).toMatchObject({ messages: [], status: "idle", error: undefined });
    await session.sendMessage("hello");
    await new Promise((resolve) => setTimeout(resolve, 0));

    expect(stream).toHaveBeenCalledTimes(2);
    expect(stream.mock.calls[1]?.[0]).toMatchObject({ live: true, offset: "offset-admitted" });
    expect(session.getSnapshot().status).toBe("connecting");
    session.dispose();
  });

  it("uses the configured SSE transport for initial and resumed streams", async () => {
    vi.useFakeTimers();
    const history = streamFrom(
      [
        event({
          type: "message_end",
          message: { role: "assistant", content: [{ type: "text", text: "hello" }] },
          eventIndex: 1,
          turnId: "turn-1",
        }),
      ],
      "offset-1",
    );
    const live = streamThenFail(
      event({ type: "idle", eventIndex: 2, timestamp: "2026-06-12T00:00:01.000Z" }),
      new TypeError("disconnected"),
      "offset-2",
    );
    const resumed = pendingStream("offset-2");
    const stream = vi
      .fn()
      .mockReturnValueOnce(history)
      .mockReturnValueOnce(live)
      .mockReturnValueOnce(resumed);
    const session = new Session({
      conversationId: "conversation-1",
      history: 100,
      live: "sse" as LiveMode,
      client: client({ stream }),
    });

    session.start();
    await settle();
    await vi.runAllTimersAsync();

    expect(stream.mock.calls[0]?.[0]).toMatchObject({ live: false, offset: "-1", tail: 100 });
    expect(stream.mock.calls[1]?.[0]).toMatchObject({ live: "sse", offset: "offset-1" });
    expect(stream.mock.calls[2]?.[0]).toMatchObject({ live: "sse", offset: "offset-2" });
    session.dispose();
  });

  it("publishes and aliases a created conversation before message admission completes", async () => {
    let admit!: (value: {
      readonly conversationId: string;
      readonly submissionId: string;
      readonly offset: string;
    }) => void;
    const submitMessage = vi.fn(
      () =>
        new Promise<{
          readonly conversationId: string;
          readonly submissionId: string;
          readonly offset: string;
        }>((resolve) => {
          admit = resolve;
        }),
    );
    const onConversationCreated = vi.fn();
    const session = new Session({
      client: client({
        createConversation: vi.fn().mockResolvedValue({ id: "conversation-new" }),
        submitMessage,
      }),
      onConversationCreated,
    });
    session.start();

    const send = session.sendMessage("hello");
    await settle();

    expect(session.getSnapshot()).toMatchObject({
      conversationId: "conversation-new",
      status: "submitted",
    });
    expect(onConversationCreated).toHaveBeenCalledWith("conversation-new", session);
    expect(submitMessage).toHaveBeenCalledWith("conversation-new", "hello");

    admit({
      conversationId: "conversation-new",
      submissionId: "submission-new",
      offset: "offset-admitted",
    });
    await expect(send).resolves.toEqual({ conversationId: "conversation-new" });
    session.dispose();
  });

  it("aliases a new-conversation session before route reuse", async () => {
    const stream = vi.fn(() => pendingStream("offset-admitted"));
    const createConversation = vi.fn().mockResolvedValue({ id: "conversation-new" });
    const submitMessage = vi.fn().mockResolvedValue({
      conversationId: "conversation-new",
      submissionId: "submission-new",
      offset: "offset-admitted",
    });
    const draft = retainConversationChatSession("__test-draft__", {
      client: client({ createConversation, submitMessage, stream }),
    });
    draft.start();

    const result = await draft.sendMessage("hello");
    const reused = retainConversationChatSession("conversation-new");

    expect(result.conversationId).toBe("conversation-new");
    expect(reused).toBe(draft);
    expect(reused.getSnapshot().messages[0]?.parts).toEqual([
      { type: "text", text: "hello", state: "done" },
    ]);

    releaseConversationChatSession("conversation-new", reused);
    releaseConversationChatSession("__test-draft__", draft);
  });

  it("detaches the draft alias as soon as the durable route retains it", async () => {
    const createConversation = vi.fn().mockResolvedValue({ id: "conversation-new" });
    const submitMessage = vi.fn().mockResolvedValue({
      conversationId: "conversation-new",
      submissionId: "submission-new",
      offset: "offset-admitted",
    });
    const draft = retainConversationChatSession("__denora:draft-conversation__", {
      client: client({ createConversation, submitMessage }),
    });
    draft.start();
    await draft.sendMessage("hello");
    const durable = retainConversationChatSession("conversation-new");
    const nextDraft = retainConversationChatSession("__denora:draft-conversation__", {
      client: client({}),
    });

    expect(durable).toBe(draft);
    expect(nextDraft).not.toBe(draft);
    expect(nextDraft.getSnapshot().conversationId).toBeUndefined();

    releaseConversationChatSession("conversation-new", durable);
    releaseConversationChatSession("__denora:draft-conversation__", nextDraft);
  });

  it("drops the draft alias after a new conversation route retains the durable session", async () => {
    vi.useFakeTimers();
    const createConversation = vi.fn().mockResolvedValue({ id: "conversation-new" });
    const submitMessage = vi.fn().mockResolvedValue({
      conversationId: "conversation-new",
      submissionId: "submission-new",
      offset: "offset-admitted",
    });
    const draft = retainConversationChatSession("__test-draft__", {
      client: client({ createConversation, submitMessage }),
    });
    draft.start();
    await draft.sendMessage("hello");
    const durable = retainConversationChatSession("conversation-new");

    releaseConversationChatSession("__test-draft__", draft);
    await vi.advanceTimersByTimeAsync(251);
    const nextDraft = retainConversationChatSession("__test-draft__", { client: client({}) });

    expect(durable).toBe(draft);
    expect(nextDraft).not.toBe(draft);

    releaseConversationChatSession("conversation-new", durable);
    releaseConversationChatSession("__test-draft__", nextDraft);
  });
});
