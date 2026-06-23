import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { makeInMemoryEventStreamStore } from "../../src/agent-run/EventStreamStore.ts";

describe("EventStreamStore", () => {
  it.effect("appends monotonic offsets and reads after a cursor", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_store");

      const first = yield* store.appendEvent("runs/run_store", { type: "first" });
      const second = yield* store.appendEvent("runs/run_store", { type: "second" });
      assert.strictEqual(first, "0000000000000000_0000000000000000");
      assert.strictEqual(second, "0000000000000000_0000000000000001");

      const read = yield* store.readEvents("runs/run_store", { offset: first });
      assert.deepStrictEqual(
        read.events.map((event) => event.data),
        [{ type: "second" }],
      );
      assert.strictEqual(read.nextOffset, second);
      assert.isTrue(read.upToDate);
    }),
  );

  it.effect("wakes subscribers on append and close", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_subscribe");

      let wakeups = 0;
      const unsubscribe = yield* store.subscribe("runs/run_subscribe", () => {
        wakeups += 1;
      });
      yield* store.appendEvent("runs/run_subscribe", { type: "event" });
      yield* store.closeStream("runs/run_subscribe");
      unsubscribe();

      assert.strictEqual(wakeups, 2);
    }),
  );

  it.effect("fails typed appends after close", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_closed");
      yield* store.closeStream("runs/run_closed");

      const error = yield* store.appendEvent("runs/run_closed", { type: "late" }).pipe(Effect.flip);
      assert.strictEqual(error._tag, "StreamClosed");
    }),
  );

  it.effect("appends an event idempotently by key", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      yield* store.createStream("runs/run_once");

      const first = yield* store.appendEventOnce("runs/run_once", "terminal", {
        type: "run_end",
      });
      const replay = yield* store.appendEventOnce("runs/run_once", "terminal", {
        type: "run_end",
      });
      const conflict = yield* store
        .appendEventOnce("runs/run_once", "terminal", { type: "other" })
        .pipe(Effect.flip);
      const read = yield* store.readEvents("runs/run_once");

      assert.strictEqual(first, "0000000000000000_0000000000000000");
      assert.strictEqual(replay, first);
      assert.strictEqual(conflict._tag, "EventStorageFailed");
      assert.deepStrictEqual(
        read.events.map((event) => event.data),
        [{ type: "run_end" }],
      );
    }),
  );
});
