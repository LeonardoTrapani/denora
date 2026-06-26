import type { AssistantMessage, AssistantMessageEvent } from "@earendil-works/pi-ai";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as TestClock from "effect/testing/TestClock";
import {
  makeSqliteStreamChunkStore,
  makeStreamChunkWriter,
  MAX_STREAM_CHUNK_SEGMENT_BYTES,
  reconstructInterruptedStream,
  STREAM_FLUSH_INTERVAL,
  type StreamChunkStore,
  type StreamChunkStorageFailed,
} from "../../src/agent-run/StreamChunks.ts";
import { makeSqliteStorage } from "../helpers/SqliteStorage.ts";

const withSqliteStreamChunkStore = <A, E>(
  run: (store: StreamChunkStore) => Effect.Effect<A, E>,
): Effect.Effect<A, E | StreamChunkStorageFailed> =>
  Effect.acquireUseRelease(
    Effect.gen(function* () {
      const storage = makeSqliteStorage();
      const store = yield* makeSqliteStreamChunkStore(storage.sql);
      return { storage, store };
    }),
    ({ store }) => run(store),
    ({ storage }) => Effect.sync(() => storage.close()),
  );

const fakePartial = (content: AssistantMessage["content"] = []): AssistantMessage => ({
  role: "assistant",
  content,
  api: "openai-chat-completions" as AssistantMessage["api"],
  provider: "test",
  model: "test-model",
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  stopReason: "stop",
  timestamp: Date.now(),
});

const textDelta = (contentIndex: number, delta: string): AssistantMessageEvent => ({
  type: "text_delta",
  contentIndex,
  delta,
  partial: fakePartial(),
});

const textEnd = (contentIndex: number, content: string): AssistantMessageEvent => ({
  type: "text_end",
  contentIndex,
  content,
  partial: fakePartial(),
});

const thinkingStart = (contentIndex: number): AssistantMessageEvent => ({
  type: "thinking_start",
  contentIndex,
  partial: fakePartial(),
});

const thinkingDelta = (contentIndex: number, delta: string): AssistantMessageEvent => ({
  type: "thinking_delta",
  contentIndex,
  delta,
  partial: fakePartial(),
});

const thinkingEnd = (contentIndex: number, content: string): AssistantMessageEvent => ({
  type: "thinking_end",
  contentIndex,
  content,
  partial: fakePartial(),
});

const segment = (
  segmentIndex: number,
  events: ReadonlyArray<AssistantMessageEvent | Record<string, unknown>>,
): { readonly segmentIndex: number; readonly body: string } => ({
  segmentIndex,
  body: JSON.stringify(events),
});

describe("StreamChunks store", () => {
  it.effect("appends segments and reads them sorted by segment index", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        assert.isTrue(yield* store.appendStreamChunkSegment("stream-a", 2, "third"));
        assert.isTrue(yield* store.appendStreamChunkSegment("stream-a", 0, "first"));
        assert.isTrue(yield* store.appendStreamChunkSegment("stream-a", 1, "second"));

        assert.deepStrictEqual(yield* store.readStreamChunkSegments("stream-a"), [
          { segmentIndex: 0, body: "first" },
          { segmentIndex: 1, body: "second" },
          { segmentIndex: 2, body: "third" },
        ]);
      }),
    ),
  );

  it.effect("returns false for duplicate segment indexes without overwriting", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        assert.isTrue(yield* store.appendStreamChunkSegment("stream-a", 0, "original"));
        assert.isFalse(yield* store.appendStreamChunkSegment("stream-a", 0, "replacement"));

        assert.deepStrictEqual(yield* store.readStreamChunkSegments("stream-a"), [
          { segmentIndex: 0, body: "original" },
        ]);
      }),
    ),
  );

  it.effect("deletes all segments for a stream key only", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        yield* store.appendStreamChunkSegment("stream-a", 0, "a0");
        yield* store.appendStreamChunkSegment("stream-a", 1, "a1");
        yield* store.appendStreamChunkSegment("stream-b", 0, "b0");

        yield* store.deleteStreamChunkSegments("stream-a");

        assert.deepStrictEqual(yield* store.readStreamChunkSegments("stream-a"), []);
        assert.deepStrictEqual(yield* store.readStreamChunkSegments("stream-b"), [
          { segmentIndex: 0, body: "b0" },
        ]);
      }),
    ),
  );

  it.effect("rejects oversized UTF-8 segment bodies before insert", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        const error = yield* store
          .appendStreamChunkSegment("oversized", 0, "a".repeat(MAX_STREAM_CHUNK_SEGMENT_BYTES + 1))
          .pipe(Effect.flip);

        assert.strictEqual(error._tag, "StreamChunkSegmentTooLarge");
        assert.deepStrictEqual(yield* store.readStreamChunkSegments("oversized"), []);
      }),
    ),
  );
});

describe("StreamChunkWriter", () => {
  it.effect("buffers writes until explicit flush or the throttled interval", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        const writer = makeStreamChunkWriter(store, "writer-throttled");
        yield* writer.write(textDelta(0, "buffered"));

        assert.deepStrictEqual(yield* store.readStreamChunkSegments("writer-throttled"), []);

        yield* TestClock.adjust(STREAM_FLUSH_INTERVAL);

        const segments = yield* store.readStreamChunkSegments("writer-throttled");
        assert.strictEqual(segments.length, 1);
        assert.deepStrictEqual(
          (JSON.parse(segments[0]?.body ?? "[]") as ReadonlyArray<Record<string, unknown>>).map(
            (event) => event.type,
          ),
          ["text_delta"],
        );
      }),
    ),
  );

  it.effect("cancel stops a pending throttled flush", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        const writer = makeStreamChunkWriter(store, "writer-cancel");
        yield* writer.write(textDelta(0, "pending"));
        yield* writer.cancel();
        yield* TestClock.adjust(STREAM_FLUSH_INTERVAL);

        assert.deepStrictEqual(yield* store.readStreamChunkSegments("writer-cancel"), []);
      }),
    ),
  );

  it.effect("compacts buffered assistant events into explicit flush segments", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        const writer = makeStreamChunkWriter(store, "writer-a");
        yield* writer.write(textDelta(0, "Hello "));
        yield* writer.write(textDelta(0, "world"));
        yield* writer.flush();

        const segments = yield* store.readStreamChunkSegments("writer-a");
        assert.strictEqual(segments.length, 1);
        const events = JSON.parse(segments[0]?.body ?? "[]") as ReadonlyArray<
          Record<string, unknown>
        >;
        assert.deepStrictEqual(
          events.map((event) => event.type),
          ["text_delta", "text_delta"],
        );
        assert.strictEqual(events[0]?.partial, undefined);
        assert.strictEqual(typeof events[1]?.partial, "object");
      }),
    ),
  );

  it.effect("stores cumulative partial deltas with linear serialized growth", () =>
    Effect.gen(function* () {
      const stored: Array<{ readonly segmentIndex: number; readonly body: string }> = [];
      const store: Pick<StreamChunkStore, "appendStreamChunkSegment"> = {
        appendStreamChunkSegment: (_streamKey, segmentIndex, body) =>
          Effect.sync(() => {
            stored.push({ segmentIndex, body });
            return true;
          }),
      };
      const writer = makeStreamChunkWriter(store, "writer-linear");
      let cumulative = "";
      for (let index = 0; index < 500; index += 1) {
        const delta = `token-${String(index)} `;
        cumulative += delta;
        yield* writer.write({
          type: "text_delta",
          contentIndex: 0,
          delta,
          partial: fakePartial([{ type: "text", text: cumulative }]),
        });
      }
      yield* writer.flush();

      const totalBytes = stored.reduce(
        (total, item) => total + new TextEncoder().encode(item.body).byteLength,
        0,
      );
      assert.isBelow(totalBytes, 100_000);
      const recovered = reconstructInterruptedStream(stored, "writer-linear");
      assert.deepStrictEqual(recovered?.partial.content, [{ type: "text", text: cumulative }]);
    }),
  );

  it.effect("rejects writer-level oversized UTF-8 segments before writing to storage", () =>
    Effect.gen(function* () {
      let callCount = 0;
      const store: Pick<StreamChunkStore, "appendStreamChunkSegment"> = {
        appendStreamChunkSegment: () =>
          Effect.sync(() => {
            callCount += 1;
            return true;
          }),
      };
      const writer = makeStreamChunkWriter(store, "writer-oversized");
      const content = '🙂"\\\n'.repeat(250_000);
      yield* writer.write({
        type: "text_delta",
        contentIndex: 0,
        delta: content,
        partial: fakePartial([{ type: "text", text: content }]),
      });
      const error = yield* writer.flush().pipe(Effect.flip);

      assert.strictEqual(error._tag, "StreamChunkSegmentTooLarge");
      if (error._tag === "StreamChunkSegmentTooLarge") {
        const tooLarge = error as {
          readonly maximumBytes: number;
          readonly serializedBytes: number;
        };
        assert.strictEqual(tooLarge.maximumBytes, MAX_STREAM_CHUNK_SEGMENT_BYTES);
        assert.isAbove(tooLarge.serializedBytes, MAX_STREAM_CHUNK_SEGMENT_BYTES);
      }
      assert.strictEqual(callCount, 0);
      assert.isTrue(writer.isFailed());
    }),
  );

  it.effect(
    "keeps compact tool-call streams ineligible for recovery without persisting arguments",
    () =>
      Effect.gen(function* () {
        const stored: Array<{ readonly segmentIndex: number; readonly body: string }> = [];
        const store: Pick<StreamChunkStore, "appendStreamChunkSegment"> = {
          appendStreamChunkSegment: (_streamKey, segmentIndex, body) =>
            Effect.sync(() => {
              stored.push({ segmentIndex, body });
              return true;
            }),
        };
        const writer = makeStreamChunkWriter(store, "writer-toolcall");
        const argumentsText = '{"prompt":"do not persist this marker"}';
        yield* writer.write({
          type: "toolcall_delta",
          contentIndex: 0,
          delta: argumentsText,
          partial: fakePartial(),
        });
        yield* writer.flush();

        assert.strictEqual(stored.length, 1);
        assert.notInclude(stored[0]?.body ?? "", "do not persist this marker");
        const events = JSON.parse(stored[0]?.body ?? "[]") as ReadonlyArray<
          Record<string, unknown>
        >;
        assert.deepStrictEqual(
          events.map((event) => event.type),
          ["toolcall"],
        );
        assert.isNull(reconstructInterruptedStream(stored, "writer-toolcall"));
      }),
  );

  it.effect("marks the writer failed after duplicate append and stops future writes", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        assert.isTrue(yield* store.appendStreamChunkSegment("writer-duplicate", 0, "original"));
        const writer = makeStreamChunkWriter(store, "writer-duplicate");
        yield* writer.write(textDelta(0, "replacement"));
        yield* writer.flush();

        assert.isTrue(writer.isFailed());
        yield* writer.write(textDelta(0, "ignored"));
        yield* writer.flush();
        assert.deepStrictEqual(yield* store.readStreamChunkSegments("writer-duplicate"), [
          { segmentIndex: 0, body: "original" },
        ]);
      }),
    ),
  );

  it.effect("close flushes buffered events then ignores later writes", () =>
    withSqliteStreamChunkStore((store) =>
      Effect.gen(function* () {
        const writer = makeStreamChunkWriter(store, "writer-close");
        yield* writer.write(textDelta(0, "final"));
        yield* writer.close();

        const closedSegments = yield* store.readStreamChunkSegments("writer-close");
        assert.strictEqual(closedSegments.length, 1);
        yield* writer.write(textDelta(0, "ignored"));
        yield* writer.flush();

        assert.deepStrictEqual(
          yield* store.readStreamChunkSegments("writer-close"),
          closedSegments,
        );
      }),
    ),
  );
});

describe("reconstructInterruptedStream", () => {
  it("returns null for empty segment lists and empty segment bodies", () => {
    assert.isNull(reconstructInterruptedStream([], "stream-empty"));
    assert.isNull(reconstructInterruptedStream([segment(0, [])], "stream-empty-body"));
  });

  it("reconstructs text deltas into an aborted assistant partial", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [textDelta(0, "Hello "), textDelta(0, "world")])],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [{ type: "text", text: "Hello world" }]);
    assert.strictEqual(result.partial.stopReason, "aborted");
    assert.strictEqual(result.partial.errorMessage, "Stream interrupted before completion.");
    assert.deepStrictEqual(result.interrupted.attributes, { streamKey: "stream-a" });
    assert.strictEqual(result.continued.type, "stream_continued");
  });

  it("uses text_end content as authoritative over accumulated deltas", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [textDelta(0, "wrong"), textEnd(0, "right")])],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [{ type: "text", text: "right" }]);
  });

  it("reconstructs thinking deltas and thinking_end content", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [thinkingStart(0), thinkingDelta(0, "Let me "), thinkingEnd(0, "Let me think")])],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [
      { type: "thinking", thinking: "Let me think" },
    ]);
  });

  it("returns null when a toolcall marker is present", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [textDelta(0, "before tool"), { type: "toolcall", partial: fakePartial() }])],
      "stream-a",
    );

    assert.isNull(result);
  });

  it("returns null when no useful partial content can be reconstructed", () => {
    assert.isNull(
      reconstructInterruptedStream(
        [segment(0, [{ type: "text_start", contentIndex: 0, partial: fakePartial() }])],
        "stream-a",
      ),
    );
    assert.isNull(reconstructInterruptedStream([segment(0, [textDelta(0, "")])], "stream-a"));
  });

  it("reconstructs multiple valid segments in order", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [textDelta(0, "First ")]), segment(1, [textDelta(0, "second")])],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [{ type: "text", text: "First second" }]);
  });

  it("filters out empty content blocks", () => {
    const result = reconstructInterruptedStream(
      [segment(0, [textDelta(0, ""), textDelta(1, "real content")])],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [{ type: "text", text: "real content" }]);
  });

  it("ignores malformed segments and reconstructs remaining valid segments", () => {
    const result = reconstructInterruptedStream(
      [
        { segmentIndex: 0, body: "not-json" },
        segment(1, [textDelta(0, "ok")]),
        { segmentIndex: 2, body: JSON.stringify({ type: "text_delta", delta: "ignored" }) },
      ],
      "stream-a",
    );

    assert.isNotNull(result);
    if (result === null) throw new Error("Expected reconstructed stream.");
    assert.deepStrictEqual(result.partial.content, [{ type: "text", text: "ok" }]);
  });
});
