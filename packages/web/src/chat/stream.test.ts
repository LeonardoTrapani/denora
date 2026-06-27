import { getEventListeners } from "node:events";
import { describe, expect, it } from "vitest";

import {
  createConversationEventStream,
  UnsupportedConversationEventVersionError,
} from "./stream.ts";

const baseEvent = {
  instanceId: "conversation-1",
  agentName: "default",
  eventIndex: 0,
  timestamp: "2026-06-12T00:00:00.000Z",
};

const conversationEvent = (event: Record<string, unknown> = {}) => ({
  ...baseEvent,
  type: "idle",
  v: 3,
  ...event,
});

describe("createConversationEventStream", () => {
  it("rejects every non-v3 event without compatibility normalization", async () => {
    for (const version of [1, 2, 4, undefined]) {
      const events = createConversationEventStream({
        conversationId: "conversation-1",
        baseUrl: "https://denora.test",
        live: false,
        fetch: async () => dsJsonResponse([{ ...conversationEvent(), v: version }]),
      });
      const iterator = events[Symbol.asyncIterator]();

      const error = await iterator.next().catch((error: unknown) => error);

      expect(error).toBeInstanceOf(UnsupportedConversationEventVersionError);
      expect(error).toMatchObject({ received: version, supported: 3 });
    }
  });

  it("constructs the correct conversation event URL", async () => {
    const urls: string[] = [];
    const events = createConversationEventStream({
      conversationId: "conversation 1",
      baseUrl: "https://denora.test/api/",
      offset: "0000000000000000_0000000000000042",
      live: false,
      fetch: async (input) => {
        urls.push(typeof input === "string" ? input : new Request(input).url);
        return dsJsonResponse([conversationEvent()]);
      },
    });

    const received = [];
    for await (const event of events) received.push(event);

    expect(received).toHaveLength(1);
    const [url] = urls;
    if (!url) throw new Error("Expected a stream request URL.");
    const parsed = new URL(url);
    expect(parsed.pathname).toBe("/conversations/conversation%201/events");
    expect(parsed.searchParams.get("offset")).toBe("0000000000000000_0000000000000042");
  });

  it("preserves tail alongside Durable Streams query parameters", async () => {
    let url = "";
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      offset: "-1",
      tail: 100,
      live: false,
      fetch: async (input) => {
        url = typeof input === "string" ? input : new Request(input).url;
        return dsJsonResponse([]);
      },
    });

    for await (const _ of events) {
    }

    const parsed = new URL(url);
    expect(parsed.searchParams.get("tail")).toBe("100");
    expect(parsed.searchParams.get("offset")).toBe("-1");
  });

  it("cancel() before iteration does not start a connection", async () => {
    let fetchCount = 0;
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      live: false,
      fetch: async () => {
        fetchCount++;
        return dsJsonResponse([]);
      },
    });

    events.cancel();
    const received = [];
    for await (const event of events) received.push(event);

    expect(received).toEqual([]);
    expect(fetchCount).toBe(0);
  });

  it("stops cleanly when canceled during initial connection", async () => {
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      live: false,
      fetch: async (_input, init) =>
        await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () =>
            reject(new DOMException("Aborted", "AbortError")),
          );
        }),
    });

    const next = events[Symbol.asyncIterator]().next();
    await new Promise((resolve) => setTimeout(resolve, 0));
    events.cancel();

    await expect(next).resolves.toEqual({ value: undefined, done: true });
  });

  it("removes the listener on an external signal when the initial connection fails", async () => {
    const controller = new AbortController();
    const events = createConversationEventStream({
      conversationId: "missing",
      baseUrl: "https://denora.test",
      live: false,
      signal: controller.signal,
      fetch: async () => new Response("not found", { status: 404 }),
    });
    const iterator = events[Symbol.asyncIterator]();

    await expect(iterator.next()).rejects.toThrow();
    expect(getEventListeners(controller.signal, "abort")).toHaveLength(0);
  });

  it("delivers events in call order when next() is called concurrently", async () => {
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      live: false,
      fetch: async () =>
        dsJsonResponse([
          conversationEvent({ type: "agent_start", eventIndex: 1 }),
          conversationEvent({
            type: "turn_start",
            eventIndex: 2,
            turnId: "turn-1",
            purpose: "agent",
          }),
          conversationEvent({ type: "idle", eventIndex: 3 }),
        ]),
    });
    const iterator = events[Symbol.asyncIterator]();

    const results = await Promise.all([
      iterator.next(),
      iterator.next(),
      iterator.next(),
      iterator.next(),
    ]);

    expect(results).toEqual([
      { value: conversationEvent({ type: "agent_start", eventIndex: 1 }), done: false },
      {
        value: conversationEvent({
          type: "turn_start",
          eventIndex: 2,
          turnId: "turn-1",
          purpose: "agent",
        }),
        done: false,
      },
      { value: conversationEvent({ type: "idle", eventIndex: 3 }), done: false },
      { value: undefined, done: true },
    ]);
  });

  it("tracks the latest stream offset after reading an event", async () => {
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      live: false,
      fetch: async () =>
        dsJsonResponse([conversationEvent()], {
          nextOffset: "0000000000000000_0000000000000001",
          closed: true,
        }),
    });

    expect(events.offset).toBe("-1");
    const iterator = events[Symbol.asyncIterator]();
    await iterator.next();
    expect(events.offset).toBe("0000000000000000_0000000000000001");
  });

  it("advances offset only when the last event of a batch has been delivered", async () => {
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      fetch: async (input) => {
        const url = new URL(typeof input === "string" ? input : new Request(input).url);
        if (url.searchParams.get("offset") === "-1") {
          return dsJsonResponse(
            [
              conversationEvent({ type: "agent_start", eventIndex: 1 }),
              conversationEvent({
                type: "turn_start",
                eventIndex: 2,
                turnId: "turn-1",
                purpose: "agent",
              }),
            ],
            { nextOffset: "0000000000000000_0000000000000002" },
          );
        }
        return dsJsonResponse([conversationEvent({ type: "idle", eventIndex: 3 })], {
          closed: true,
          nextOffset: "0000000000000000_0000000000000003",
        });
      },
    });
    const iterator = events[Symbol.asyncIterator]();

    await iterator.next();
    expect(events.offset).toBe("-1");
    await iterator.next();
    expect(events.offset).toBe("0000000000000000_0000000000000002");
    await iterator.next();
    expect(events.offset).toBe("0000000000000000_0000000000000003");
    await expect(iterator.next()).resolves.toEqual({ value: undefined, done: true });
  });

  it("cancel() stops iteration and aborts the underlying connection", async () => {
    let fetchCount = 0;
    let lastSignal: AbortSignal | undefined;
    const events = createConversationEventStream({
      conversationId: "conversation-1",
      baseUrl: "https://denora.test",
      live: false,
      fetch: async (_input, init) => {
        fetchCount++;
        lastSignal = init?.signal as AbortSignal | undefined;
        return dsJsonResponse([conversationEvent()]);
      },
    });

    const received = [];
    for await (const event of events) {
      received.push(event);
      events.cancel();
    }

    expect(received).toHaveLength(1);
    expect(fetchCount).toBe(1);
    expect(lastSignal?.aborted).toBe(true);
  });
});

function dsJsonResponse(
  events: readonly unknown[],
  options: {
    readonly closed?: boolean;
    readonly upToDate?: boolean;
    readonly nextOffset?: string;
  } = {},
): Response {
  const nextOffset = options.nextOffset ?? String(events.length).padStart(16, "0");
  const headers: Record<string, string> = {
    "content-type": "application/json",
    "stream-next-offset": nextOffset,
  };
  if (options.upToDate !== false) headers["stream-up-to-date"] = "true";
  if (options.closed) headers["stream-closed"] = "true";
  return new Response(
    JSON.stringify(
      events.map((event) =>
        event && typeof event === "object" && !("v" in event) ? { ...event, v: 3 } : event,
      ),
    ),
    { status: 200, headers },
  );
}
