import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeInMemoryEventStreamStore } from "../../src/agent-run/EventStreamStore.ts";
import { handleStreamHead, handleStreamRead } from "../../src/agent-run/StreamProtocol.ts";

const countOccurrences = (input: string, needle: string): number => input.split(needle).length - 1;

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

describe("StreamProtocol", () => {
  it.effect("serves HEAD metadata and catch-up JSON", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_protocol");
      const offset = yield* store.appendEvent("runs/run_protocol", {
        type: "run_start",
        input: null,
      });

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
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), [
        { type: "run_start", input: null },
      ]);
    }),
  );

  it.effect("returns 304 for matching catch-up ETag", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_etag");
      yield* store.appendEvent("runs/run_etag", { type: "event" });

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

  it.effect("serves SSE data and closed control frames", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_sse");
      const offset = yield* store.appendEvent("runs/run_sse", {
        type: "run_start",
        input: null,
      });
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
      assert.include(body, 'data:[{"type":"run_start","input":null}]\n\n');
      assert.include(body, "event: control\n");
      assert.include(body, `data:{"streamNextOffset":"${offset}","streamClosed":true}\n\n`);
      assert.strictEqual(countOccurrences(body, "event: control\n"), 1);
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
