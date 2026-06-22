import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  type EventStreamStore,
  makeInMemoryEventStreamStore,
} from "../../src/agent-run/EventStreamStore.ts";
import { handleStreamHead, handleStreamRead } from "../../src/agent-run/StreamProtocol.ts";

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

  it.effect("returns only trailing events for catch-up, long-poll, and SSE", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_tail_catchup");
      yield* store.createStream("runs/run_tail_long_poll");
      yield* store.createStream("runs/run_tail_sse");
      for (let index = 0; index < 5; index++) {
        yield* store.appendEvent("runs/run_tail_catchup", { index });
        yield* store.appendEvent("runs/run_tail_long_poll", { index });
        yield* store.appendEvent("runs/run_tail_sse", { index });
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

      assert.deepStrictEqual(yield* Effect.promise(() => catchUp.json()), [
        { index: 3 },
        { index: 4 },
      ]);
      assert.deepStrictEqual(yield* Effect.promise(() => longPoll.json()), [
        { index: 3 },
        { index: 4 },
      ]);
      assert.deepStrictEqual(dataFrame === undefined ? undefined : JSON.parse(dataFrame.data), [
        { index: 3 },
        { index: 4 },
      ]);
    }),
  );

  it.effect("offset=now catch-up omits ETag and long-poll returns appended data", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_now_catchup");
      yield* store.appendEvent("runs/run_now_catchup", { type: "old" });

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
              yield* store.appendEvent(path, { type: "new" });
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
      assert.deepStrictEqual(yield* Effect.promise(() => longPoll.json()), [{ type: "new" }]);
    }),
  );

  it.effect("returns structured invalid-request errors for malformed offsets", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_bad_offset");

      const response = yield* handleStreamRead({
        store,
        path: "runs/run_bad_offset",
        request: new Request("https://api.test/runs/run_bad_offset?offset=banana"),
      });

      assert.strictEqual(response.status, 400);
      assert.deepStrictEqual(yield* Effect.promise(() => response.json()), {
        error: { type: "invalid_request", message: "Invalid offset format." },
      });
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
        error: { type: "invalid_request", message: "Offset is required for live mode." },
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
        yield* base.appendEvent("runs/run_sse_backpressure", { index });
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
});
