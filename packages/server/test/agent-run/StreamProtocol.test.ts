import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  EventStorageFailed,
  type EventStreamStore,
  makeInMemoryEventStreamStore,
  agentStreamPath,
} from "../../src/agent-run/EventStreamStore.ts";
import {
  handleAgentConversationObjectRequest,
  handleConversationObjectRequest,
  handleRunObjectRequest,
  handleStreamHead,
  handleStreamRead,
} from "../../src/agent-run/StreamProtocol.ts";

const parseSseFrames = (body: string): Array<{ event: string; data: string }> =>
  body
    .split("\n\n")
    .map((block) => block.trim())
    .filter((block) => block !== "" && !block.startsWith(":"))
    .map((block) => {
      const lines = block.split("\n");
      const event = lines.find((line) => line.startsWith("event: "))?.slice("event: ".length) ?? "";
      const data = lines
        .filter((line) => line.startsWith("data:"))
        .map((line) => line.slice("data:".length))
        .join("\n");
      return { event, data };
    });

const countOccurrences = (input: string, needle: string): number => input.split(needle).length - 1;

const runEvent = (
  runId: string,
  eventIndex: number,
  event: Record<string, unknown> = { type: "agent_start" },
) => ({
  v: 3,
  runId,
  eventIndex,
  timestamp: `2026-01-01T00:00:${String(eventIndex).padStart(2, "0")}.000Z`,
  ...event,
});

const collectSseFor = (
  response: Response,
  controller: AbortController,
  durationMs: number,
): Effect.Effect<string> =>
  Effect.promise(async () => {
    const body = response.text();
    await new Promise((resolve) => setTimeout(resolve, durationMs));
    controller.abort();
    return body;
  });

const collectSseUntilError = (response: Response): Effect.Effect<string> =>
  Effect.promise(async () => {
    const reader = response.body?.getReader();
    assert.isDefined(reader);
    const decoder = new TextDecoder();
    let body = "";
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        body += decoder.decode(value, { stream: true });
      }
    } catch {
      body += decoder.decode();
    }
    return body;
  });

describe("StreamProtocol", () => {
  it.effect("serves HEAD metadata and catch-up JSON", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_protocol");
      const start = runEvent("run_protocol", 0, {
        type: "run_start",
        workflowName: "denora.agent-run",
        startedAt: "2026-01-01T00:00:00.000Z",
        input: null,
      });
      const offset = yield* store.appendEvent("runs/run_protocol", start);

      const head = yield* handleStreamHead(store, "runs/run_protocol");
      assert.strictEqual(head.status, 200);
      assert.strictEqual(head.headers.get("Stream-Next-Offset"), offset);

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_protocol",
        request: new Request("https://api.test/runs/run_protocol"),
      });
      assert.strictEqual(response.status, 200);
      assert.strictEqual(response.headers.get("Stream-Next-Offset"), offset);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), [start]);
    }),
  );

  it.effect("serves the default attached-agent stream through the conversation events alias", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const conversationId = "conversation_protocol_alias";
      const streamPath = agentStreamPath("default", conversationId);
      const event = {
        v: 3,
        type: "message_start",
        instanceId: conversationId,
        agentName: "default",
        eventIndex: 0,
        timestamp: "2026-01-01T00:00:00.000Z",
        turnId: `conversation:${conversationId}:user`,
        message: { role: "user", content: [{ type: "text", text: "hello" }] },
      };
      yield* store.createStream(streamPath);
      yield* store.appendEvent(streamPath, event);

      const response = yield* handleConversationObjectRequest(
        store,
        new Request(`https://api.test/conversations/${conversationId}/events?offset=-1`),
      );

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), [event]);
    }),
  );

  it.effect("routes durable object /runs/:runId requests to the run stream path", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const runId = "run_object_forwarding";
      const event = runEvent(runId, 0, {
        type: "run_start",
        workflowName: "denora.agent-run",
        startedAt: "2026-01-01T00:00:00.000Z",
        input: null,
      });
      yield* store.createStream(`runs/${runId}`);
      yield* store.appendEvent(`runs/${runId}`, event);

      const response = yield* handleAgentConversationObjectRequest(
        store,
        new Request(`https://api.test/runs/${runId}?offset=-1`),
      );

      assert.strictEqual(response.status, 200);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), [event]);
    }),
  );

  it.effect("preserves durable object attached-agent stream aliases", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const conversationId = "conversation_object_forwarding";
      const defaultEvent = {
        v: 3,
        type: "message_start",
        instanceId: conversationId,
        agentName: "default",
        eventIndex: 0,
        timestamp: "2026-01-01T00:00:00.000Z",
        turnId: `conversation:${conversationId}:user`,
        message: { role: "user", content: [{ type: "text", text: "hello" }] },
      };
      const customEvent = {
        ...defaultEvent,
        agentName: "researcher",
        eventIndex: 1,
      };
      yield* store.createStream(agentStreamPath("default", conversationId));
      yield* store.appendEvent(agentStreamPath("default", conversationId), defaultEvent);
      yield* store.createStream(agentStreamPath("researcher", conversationId));
      yield* store.appendEvent(agentStreamPath("researcher", conversationId), customEvent);

      const conversationResponse = yield* handleAgentConversationObjectRequest(
        store,
        new Request(`https://api.test/conversations/${conversationId}/events?offset=-1`),
      );
      const agentResponse = yield* handleAgentConversationObjectRequest(
        store,
        new Request(`https://api.test/agents/researcher/${conversationId}?offset=-1`),
      );

      assert.strictEqual(conversationResponse.status, 200);
      assert.deepStrictEqual(yield* Effect.promise(() => conversationResponse.json()), [
        defaultEvent,
      ]);
      assert.strictEqual(agentResponse.status, 200);
      assert.deepStrictEqual(yield* Effect.promise(() => agentResponse.json()), [customEvent]);
    }),
  );

  it.effect("returns 304 for matching catch-up ETag", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_etag");
      yield* store.appendEvent("runs/run_etag", runEvent("run_etag", 0));

      const first = yield* handleStreamRead({
        store,
        path: "runs/run_etag",
        request: new Request("https://api.test/runs/run_etag"),
      });
      const second = yield* handleStreamRead({
        store,
        path: "runs/run_etag",
        request: new Request("https://api.test/runs/run_etag", {
          headers: { "If-None-Match": first.headers.get("etag") ?? "" },
        }),
      });

      assert.strictEqual(second.status, 304);
    }),
  );

  it.effect("long-poll times out with stream cursor headers", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_long_poll");

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_long_poll",
        request: new Request("https://api.test/runs/run_long_poll?offset=-1&live=long-poll"),
        longPollTimeoutMs: 1,
      });

      assert.strictEqual(response.status, 204);
      assert.strictEqual(response.headers.get("Stream-Up-To-Date"), "true");
      assert.isNotNull(response.headers.get("Stream-Cursor"));
    }),
  );

  it.effect("returns only trailing events for catch-up, long-poll, and SSE", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_tail_catchup");
      yield* store.createStream("runs/run_tail_long_poll");
      yield* store.createStream("runs/run_tail_sse");
      for (let index = 0; index < 5; index++) {
        yield* store.appendEvent(
          "runs/run_tail_catchup",
          runEvent("run_tail_catchup", index, { type: "text_delta", text: String(index), index }),
        );
        yield* store.appendEvent(
          "runs/run_tail_long_poll",
          runEvent("run_tail_long_poll", index, { type: "text_delta", text: String(index), index }),
        );
        yield* store.appendEvent(
          "runs/run_tail_sse",
          runEvent("run_tail_sse", index, { type: "text_delta", text: String(index), index }),
        );
      }
      yield* store.closeStream("runs/run_tail_sse");

      const catchUp = yield* handleStreamRead({
        store,
        path: "runs/run_tail_catchup",
        request: new Request("https://api.test/runs/run_tail_catchup?offset=-1&tail=2"),
      });
      const longPoll = yield* handleStreamRead({
        store,
        path: "runs/run_tail_long_poll",
        request: new Request(
          "https://api.test/runs/run_tail_long_poll?offset=-1&tail=2&live=long-poll",
        ),
      });
      const sse = yield* handleStreamRead({
        store,
        path: "runs/run_tail_sse",
        request: new Request("https://api.test/runs/run_tail_sse?offset=-1&tail=2&live=sse"),
        sseHeartbeatMs: 60_000,
      });
      const frames = parseSseFrames(yield* Effect.promise(() => sse.text()));
      const dataFrame = frames.find((frame) => frame.event === "data");

      assert.deepStrictEqual(
        ((yield* Effect.promise(() => catchUp.json())) as Array<{ index: number }>).map(
          (event) => event.index,
        ),
        [3, 4],
      );
      assert.deepStrictEqual(
        ((yield* Effect.promise(() => longPoll.json())) as Array<{ index: number }>).map(
          (event) => event.index,
        ),
        [3, 4],
      );
      assert.deepStrictEqual(
        (JSON.parse(dataFrame?.data ?? "[]") as Array<{ index: number }>).map(
          (event) => event.index,
        ),
        [3, 4],
      );
    }),
  );

  it.effect("offset=now catch-up omits ETag and long-poll returns appended data", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_now_catchup");
      yield* store.appendEvent("runs/run_now_catchup", runEvent("run_now_catchup", 0));

      const catchUp = yield* handleStreamRead({
        store,
        path: "runs/run_now_catchup",
        request: new Request("https://api.test/runs/run_now_catchup?offset=now"),
      });

      yield* store.createStream("runs/run_now_long_poll");
      let appendedLongPollEvent = false;
      const longPollStore = {
        ...store,
        getStreamMeta: (path) =>
          Effect.gen(function* () {
            const meta = yield* store.getStreamMeta(path);
            if (!appendedLongPollEvent && path === "runs/run_now_long_poll") {
              appendedLongPollEvent = true;
              yield* store.appendEvent(path, runEvent("run_now_long_poll", 0));
            }
            return meta;
          }),
      } satisfies EventStreamStore;
      const longPoll = yield* handleStreamRead({
        store: longPollStore,
        path: "runs/run_now_long_poll",
        request: new Request("https://api.test/runs/run_now_long_poll?offset=now&live=long-poll"),
      });

      assert.strictEqual(catchUp.status, 200);
      assert.strictEqual(catchUp.headers.get("etag"), null);
      assert.deepStrictEqual(yield* Effect.promise(() => catchUp.json()), []);
      assert.strictEqual(longPoll.status, 200);
      assert.strictEqual(
        ((yield* Effect.promise(() => longPoll.json())) as Array<{ type: string }>)[0]?.type,
        "agent_start",
      );
    }),
  );

  it.effect("serves SSE data and closed control frames", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_sse");
      const start = runEvent("run_sse", 0, {
        type: "run_start",
        workflowName: "denora.agent-run",
        startedAt: "2026-01-01T00:00:00.000Z",
        input: null,
      });
      const offset = yield* store.appendEvent("runs/run_sse", start);
      yield* store.closeStream("runs/run_sse");

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse",
        request: new Request("https://api.test/runs/run_sse?offset=-1&live=sse"),
        sseHeartbeatMs: 60_000,
      });

      assert.strictEqual(response.status, 200);
      assert.strictEqual(response.headers.get("content-type"), "text/event-stream");
      const body = yield* Effect.promise(() => response.text());
      assert.include(body, "event: data\n");
      assert.include(body, `data:${JSON.stringify([start])}\n\n`);
      assert.include(body, "event: control\n");
      assert.include(body, `data:{"streamNextOffset":"${offset}","streamClosed":true}\n\n`);
      assert.strictEqual(countOccurrences(body, "event: control\n"), 1);
    }),
  );

  it.effect("rejects SSE reads without an offset", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_sse_no_offset");

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_no_offset",
        request: new Request("https://api.test/runs/run_sse_no_offset?live=sse"),
      });

      assert.strictEqual(response.status, 400);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
        error: {
          type: "invalid_request",
          code: "missing_live_offset",
          message: "Offset is required for live mode.",
          details: { live: "sse" },
        },
      });
    }),
  );

  it.effect("returns 404 for SSE reads on missing streams", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_missing",
        request: new Request("https://api.test/runs/run_sse_missing?offset=-1&live=sse"),
      });

      assert.strictEqual(response.status, 404);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
        error: {
          type: "run_not_found",
          message: 'Agent Run "run_sse_missing" was not found.',
        },
      });
    }),
  );

  it.effect("returns generic stream_not_found for missing attached-agent streams", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();

      const response = yield* handleStreamRead({
        store,
        path: "agents/denora/missing",
        request: new Request("https://api.test/agents/denora/missing?offset=-1"),
      });

      assert.strictEqual(response.status, 404);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
        error: {
          type: "stream_not_found",
          message: 'Event stream "agents/denora/missing" was not found.',
        },
      });
    }),
  );

  it.effect("fails the SSE stream instead of emitting error frames when reads fail", () =>
    Effect.gen(function* () {
      const base = makeInMemoryEventStreamStore();
      yield* base.createStream("runs/run_sse_read_failure");
      let readCount = 0;
      const store = {
        ...base,
        readEvents: (path, opts) =>
          Effect.gen(function* () {
            readCount += 1;
            if (readCount > 1) {
              return yield* new EventStorageFailed({
                operation: "read stream events",
                cause: new Error("boom"),
              });
            }
            return yield* base.readEvents(path, opts);
          }),
      } satisfies EventStreamStore;

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_read_failure",
        request: new Request("https://api.test/runs/run_sse_read_failure?offset=-1&live=sse"),
        sseHeartbeatMs: 60_000,
        sseIdleTimeoutMs: 1,
      });

      const body = yield* collectSseUntilError(response);

      assert.strictEqual(response.status, 200);
      assert.include(body, "event: control\n");
      assert.notInclude(body, "event: error\n");
    }),
  );

  it.effect("renders object request store failures as stable internal errors", () =>
    Effect.gen(function* () {
      const base = makeInMemoryEventStreamStore();
      yield* base.createStream("runs/run_storage_failed");
      const store = {
        ...base,
        readEvents: () =>
          Effect.fail(
            new EventStorageFailed({
              operation: "read stream events",
              cause: new Error("boom"),
            }),
          ),
      } satisfies EventStreamStore;

      const response = yield* handleRunObjectRequest(
        store,
        new Request("https://api.test/runs/run_storage_failed"),
      );

      assert.strictEqual(response.status, 500);
      const body = (yield* Effect.promise(() => response.json())) as {
        readonly error: {
          readonly type: string;
          readonly code: string;
          readonly message: string;
          readonly details: { readonly traceId: string };
        };
      };
      assert.strictEqual(body.error.type, "internal_error");
      assert.strictEqual(body.error.code, "internal_error");
      assert.strictEqual(body.error.message, "Internal error.");
      assert.isString(body.error.details.traceId);
    }),
  );

  it.effect("includes browser security headers on stream responses", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_headers");

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_headers",
        request: new Request("https://api.test/runs/run_headers?offset=-1"),
      });
      const head = yield* handleStreamHead(store, "runs/run_headers");

      assert.strictEqual(response.headers.get("x-content-type-options"), "nosniff");
      assert.strictEqual(head.headers.get("x-content-type-options"), "nosniff");
      assert.strictEqual(response.headers.get("cross-origin-resource-policy"), "cross-origin");
    }),
  );

  it.effect("bounds SSE production for a slow response body reader", () =>
    Effect.gen(function* () {
      const base = makeInMemoryEventStreamStore();
      yield* base.createStream("runs/run_sse_backpressure");
      for (let index = 0; index < 250; index++) {
        yield* base.appendEvent(
          "runs/run_sse_backpressure",
          runEvent("run_sse_backpressure", index, {
            type: "text_delta",
            text: String(index),
            index,
          }),
        );
      }
      let readCount = 0;
      const store = {
        ...base,
        readEvents: (path, opts) =>
          Effect.gen(function* () {
            readCount += 1;
            return yield* base.readEvents(path, opts);
          }),
      } satisfies EventStreamStore;

      const controller = new AbortController();
      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_backpressure",
        request: new Request("https://api.test/runs/run_sse_backpressure?offset=-1&live=sse", {
          signal: controller.signal,
        }),
        sseHeartbeatMs: 60_000,
      });
      const reader = response.body?.getReader();
      assert.isDefined(reader);
      yield* Effect.promise(() => reader.read());
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
      yield* Effect.sync(() => {
        controller.abort();
        void reader.cancel();
      });

      assert.strictEqual(response.status, 200);
      assert.isAtMost(readCount, 2);
    }),
  );

  it.effect("sends SSE heartbeat comments without heartbeat control frames", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_sse_heartbeat");
      const controller = new AbortController();

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_heartbeat",
        request: new Request("https://api.test/runs/run_sse_heartbeat?offset=-1&live=sse", {
          signal: controller.signal,
        }),
        sseHeartbeatMs: 2,
        sseIdleTimeoutMs: 100,
      });

      const body = yield* collectSseFor(response, controller, 20);

      assert.isAtLeast(countOccurrences(body, ": heartbeat\n\n"), 1);
      assert.strictEqual(countOccurrences(body, "event: control\n"), 1);
    }),
  );

  it.effect("sends SSE control keep-alives after the idle timeout", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_sse_idle");
      const controller = new AbortController();

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_sse_idle",
        request: new Request("https://api.test/runs/run_sse_idle?offset=-1&live=sse", {
          signal: controller.signal,
        }),
        sseHeartbeatMs: 1_000,
        sseIdleTimeoutMs: 5,
      });

      const body = yield* collectSseFor(response, controller, 25);

      assert.isAtLeast(countOccurrences(body, "event: control\n"), 2);
      assert.include(body, '"upToDate":true');
    }),
  );

  const invalidRequestCases = [
    {
      name: "duplicate offset",
      url: "https://api.test/runs/run_invalid?offset=-1&offset=now",
      body: {
        error: {
          type: "invalid_request",
          code: "duplicate_offset_parameter",
          message: "Duplicate offset parameters are not allowed.",
          details: { values: ["-1", "now"] },
        },
      },
    },
    {
      name: "duplicate tail",
      url: "https://api.test/runs/run_invalid?tail=1&tail=2",
      body: {
        error: {
          type: "invalid_request",
          code: "duplicate_tail_parameter",
          message: "Duplicate tail parameters are not allowed.",
          details: { values: ["1", "2"] },
        },
      },
    },
    {
      name: "invalid tail",
      url: "https://api.test/runs/run_invalid?tail=0",
      body: {
        error: {
          type: "invalid_request",
          code: "invalid_tail_parameter",
          message: "Tail must be an integer greater than or equal to 1.",
          details: { tail: "0" },
        },
      },
    },
    {
      name: "live mode without offset",
      url: "https://api.test/runs/run_invalid?live=long-poll",
      body: {
        error: {
          type: "invalid_request",
          code: "missing_live_offset",
          message: "Offset is required for live mode.",
          details: { live: "long-poll" },
        },
      },
    },
    {
      name: "invalid live mode",
      url: "https://api.test/runs/run_invalid?offset=-1&live=stream",
      body: {
        error: {
          type: "invalid_request",
          code: "invalid_live_mode",
          message: 'Invalid live mode. Use "long-poll" or "sse".',
          details: { live: "stream" },
        },
      },
    },
    {
      name: "malformed offset",
      url: "https://api.test/runs/run_invalid?offset=banana",
      body: {
        error: {
          type: "invalid_request",
          code: "invalid_offset_format",
          message: "Invalid stream offset format.",
          details: { offset: "banana" },
        },
      },
    },
  ] as const;

  for (const testCase of invalidRequestCases) {
    it.effect(`returns structured invalid_request for ${testCase.name}`, () =>
      Effect.gen(function* () {
        const store = makeInMemoryEventStreamStore();
        const response = yield* handleStreamRead({
          store,
          path: "runs/run_invalid",
          request: new Request(testCase.url),
        });

        assert.strictEqual(response.status, 400);
        assert.strictEqual(response.headers.get("content-type"), "application/json");
        assert.deepStrictEqual(yield* Effect.promise(() => response.json()), testCase.body);
      }),
    );
  }
});
