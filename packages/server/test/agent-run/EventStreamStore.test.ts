import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  makeInMemoryEventStreamStore,
  makeSqliteEventStreamStore,
  type EventStreamStore,
  type EventStorageFailed,
} from "../../src/agent-run/EventStreamStore.ts";
import { makeSqliteStorage } from "../helpers/SqliteStorage.ts";

interface EventStreamStoreContractBackend {
  readonly useStore: <A, E>(
    run: (store: EventStreamStore) => Effect.Effect<A, E>,
  ) => Effect.Effect<A, E | EventStorageFailed>;
}

const inMemoryBackend: EventStreamStoreContractBackend = {
  useStore: (run) => run(makeInMemoryEventStreamStore()),
};

const sqliteBackend: EventStreamStoreContractBackend = {
  useStore: (run) =>
    Effect.acquireUseRelease(
      Effect.gen(function* () {
        const storage = makeSqliteStorage();
        const store = yield* makeSqliteEventStreamStore(storage.sql);
        return { storage, store };
      }),
      ({ store }) => run(store),
      ({ storage }) => Effect.sync(() => storage.close()),
    ),
};

const defineEventStreamStoreContractTests = (
  label: string,
  backend: EventStreamStoreContractBackend,
): void => {
  describe(label, () => {
    it.effect("replays appended events with monotonic offsets", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const first = yield* store.appendEvent("runs/test", { index: 0 });
          const second = yield* store.appendEvent("runs/test", { index: 1 });

          assert.strictEqual(first, "0000000000000000_0000000000000000");
          assert.strictEqual(second, "0000000000000000_0000000000000001");
          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset: "-1" }), {
            events: [
              { data: { index: 0 }, offset: first },
              { data: { index: 1 }, offset: second },
            ],
            nextOffset: second,
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("returns null metadata and an empty up-to-date read for missing streams", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          assert.isNull(yield* store.getStreamMeta("runs/missing"));
          assert.deepStrictEqual(yield* store.readEvents("runs/missing", { offset: "-1" }), {
            events: [],
            nextOffset: "-1",
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("rejects append operations when the stream does not exist", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          const appendError = yield* store
            .appendEvent("runs/missing", { index: 0 })
            .pipe(Effect.flip);
          const onceError = yield* store
            .appendEventOnce("runs/missing", "event-1", { index: 0 })
            .pipe(Effect.flip);

          assert.strictEqual(appendError._tag, "StreamNotFound");
          assert.strictEqual(onceError._tag, "StreamNotFound");
        }),
      ),
    );

    it.effect("returns the tail cursor with no events when offset is now", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          yield* store.appendEvent("runs/test", { index: 0 });
          const tail = yield* store.appendEvent("runs/test", { index: 1 });

          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset: "now" }), {
            events: [],
            nextOffset: tail,
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("preserves existing events when createStream is called twice", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const offset = yield* store.appendEvent("runs/test", { index: 0 });

          yield* store.createStream("runs/test");

          assert.deepStrictEqual(yield* store.getStreamMeta("runs/test"), {
            nextOffset: offset,
            closed: false,
          });
          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset: "-1" }), {
            events: [{ data: { index: 0 }, offset }],
            nextOffset: offset,
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("paginates streams and marks exact tail pages as up to date", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          for (let index = 0; index < 4; index += 1) {
            yield* store.appendEvent("runs/test", { index });
          }

          const firstPage = yield* store.readEvents("runs/test", { offset: "-1", limit: 2 });
          assert.deepStrictEqual(firstPage, {
            events: [
              { data: { index: 0 }, offset: "0000000000000000_0000000000000000" },
              { data: { index: 1 }, offset: "0000000000000000_0000000000000001" },
            ],
            nextOffset: "0000000000000000_0000000000000001",
            upToDate: false,
            closed: false,
          });

          const exactTailPage = yield* store.readEvents("runs/test", {
            offset: firstPage.nextOffset,
            limit: 2,
          });
          assert.deepStrictEqual(exactTailPage, {
            events: [
              { data: { index: 2 }, offset: "0000000000000000_0000000000000002" },
              { data: { index: 3 }, offset: "0000000000000000_0000000000000003" },
            ],
            nextOffset: "0000000000000000_0000000000000003",
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("uses the default read limit when a non-positive limit is requested", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          for (let index = 0; index < 3; index += 1) {
            yield* store.appendEvent("runs/test", { index });
          }

          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset: "-1", limit: 0 }), {
            events: [
              { data: { index: 0 }, offset: "0000000000000000_0000000000000000" },
              { data: { index: 1 }, offset: "0000000000000000_0000000000000001" },
              { data: { index: 2 }, offset: "0000000000000000_0000000000000002" },
            ],
            nextOffset: "0000000000000000_0000000000000002",
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("records closed metadata and rejects appends after close", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const offset = yield* store.appendEvent("runs/test", { index: 0 });
          yield* store.closeStream("runs/test");

          assert.deepStrictEqual(yield* store.getStreamMeta("runs/test"), {
            nextOffset: offset,
            closed: true,
          });
          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset }), {
            events: [],
            nextOffset: offset,
            upToDate: true,
            closed: true,
          });
          const appendError = yield* store.appendEvent("runs/test", { index: 1 }).pipe(Effect.flip);
          assert.strictEqual(appendError._tag, "StreamClosed");
        }),
      ),
    );

    it.effect("returns the same offset for repeated appendEventOnce payloads", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const first = yield* store.appendEventOnce("runs/test", "terminal-1", { index: 0 });
          const retry = yield* store.appendEventOnce("runs/test", "terminal-1", { index: 0 });
          const keyed = yield* store.readEventByKey("runs/test", "terminal-1");

          assert.strictEqual(retry, first);
          assert.deepStrictEqual(keyed, { offset: first, event: { index: 0 } });
          assert.deepStrictEqual(yield* store.readEvents("runs/test"), {
            events: [{ data: { index: 0 }, offset: first }],
            nextOffset: first,
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("rejects conflicting appendEventOnce payloads without appending", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const offset = yield* store.appendEventOnce("runs/test", "terminal-1", { index: 0 });
          const conflict = yield* store
            .appendEventOnce("runs/test", "terminal-1", { index: 1 })
            .pipe(Effect.flip);

          assert.strictEqual(conflict._tag, "EventStorageFailed");
          if (conflict._tag === "EventStorageFailed") {
            assert.include(String(conflict.cause), "conflicting payload");
          }
          assert.deepStrictEqual(yield* store.getStreamMeta("runs/test"), {
            nextOffset: offset,
            closed: false,
          });
          assert.deepStrictEqual(yield* store.readEvents("runs/test", { offset: "-1" }), {
            events: [{ data: { index: 0 }, offset }],
            nextOffset: offset,
            upToDate: true,
            closed: false,
          });
        }),
      ),
    );

    it.effect("allocates distinct offsets and events for concurrent appendEventOnce calls", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          const offsets = yield* Effect.all(
            [
              store.appendEventOnce("runs/test", "event-1", { index: 0 }),
              store.appendEventOnce("runs/test", "event-2", { index: 1 }),
            ],
            { concurrency: "unbounded" },
          );

          assert.strictEqual(new Set(offsets).size, 2);
          const read = yield* store.readEvents("runs/test", { offset: "-1" });
          assert.strictEqual(read.events.length, 2);
          assert.deepStrictEqual(
            [...read.events].sort(
              (left, right) =>
                (left.data as { index: number }).index - (right.data as { index: number }).index,
            ),
            [
              { data: { index: 0 }, offset: offsets[0] },
              { data: { index: 1 }, offset: offsets[1] },
            ],
          );
          assert.deepStrictEqual(yield* store.readEventByKey("runs/test", "event-1"), {
            offset: offsets[0],
            event: { index: 0 },
          });
          assert.deepStrictEqual(yield* store.readEventByKey("runs/test", "event-2"), {
            offset: offsets[1],
            event: { index: 1 },
          });
        }),
      ),
    );

    it.effect("notifies subscribers on append and close", () =>
      backend.useStore((store) =>
        Effect.gen(function* () {
          yield* store.createStream("runs/test");
          let notifications = 0;
          const unsubscribe = yield* store.subscribe("runs/test", () => {
            notifications += 1;
          });

          yield* store.appendEvent("runs/test", { index: 0 });
          yield* store.closeStream("runs/test");
          unsubscribe();

          assert.strictEqual(notifications, 2);
        }),
      ),
    );
  });
};

describe("EventStreamStore", () => {
  defineEventStreamStoreContractTests("makeInMemoryEventStreamStore", inMemoryBackend);
  defineEventStreamStoreContractTests("makeSqliteEventStreamStore", sqliteBackend);
});
