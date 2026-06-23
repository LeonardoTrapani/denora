import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  makeInMemoryEventStreamStore,
  runStreamPath,
} from "../../src/agent-run/EventStreamStore.ts";
import { AgentRunLifecycle } from "../../src/agent-run/Lifecycle.ts";
import type { Interface as PiRuntimeInterface } from "../../src/agent-loop/PiRuntime.ts";

describe("AgentRunLifecycle", () => {
  it.effect("translates Pi agent events into the durable run stream once", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const runId = `run_${crypto.randomUUID()}`;
      const streamPath = runStreamPath(runId);
      let modelCalls = 0;
      const pi: PiRuntimeInterface = {
        streamFn: ((model) => {
          modelCalls += 1;
          const stream = createAssistantMessageEventStream();
          const message: AssistantMessage = {
            role: "assistant",
            content: [{ type: "text", text: "hello" }],
            api: model.api,
            provider: model.provider,
            model: model.id,
            usage: emptyUsage,
            stopReason: "stop",
            timestamp: Date.now(),
          };

          queueMicrotask(() => {
            stream.push({ type: "start", partial: { ...message, content: [] } });
            stream.push({ type: "text_start", contentIndex: 0, partial: message });
            stream.push({ type: "text_delta", contentIndex: 0, delta: "hello", partial: message });
            stream.push({ type: "text_end", contentIndex: 0, content: "hello", partial: message });
            stream.push({ type: "done", reason: "stop", message });
            stream.end();
          });

          return stream;
        }) satisfies StreamFn,
      };

      const created = yield* AgentRunLifecycle.startRun(store, {
        runId,
        input: { prompt: "hello" },
        scheduleExecution: () =>
          AgentRunLifecycle.executeRun(store, { runId, input: { prompt: "hello" }, pi }).pipe(
            Effect.forkDetach({ startImmediately: true }),
            Effect.asVoid,
          ),
      });
      assert.isTrue(created.created);
      yield* waitForClosed(store, streamPath);

      const replay = yield* store.readEvents(streamPath, { offset: "-1" });
      const events = replay.events.map((event) => event.data as Record<string, unknown>);
      assert.strictEqual(events[0]?.type, "run_start");
      assert.includeMembers(
        events.map((event) => event.type),
        ["agent_start", "text_delta", "turn", "agent_end", "run_end"],
      );
      assert.notIncludeMembers(
        events.map((event) => event.type),
        ["message_update", "text_start", "text_end", "done", "turn_request"],
      );
      assert.strictEqual(events.find((event) => event.type === "text_delta")?.text, "hello");
      assert.isBelow(
        events.findIndex((event) => event.type === "text_delta"),
        events.findIndex((event) => event.type === "turn"),
      );
      assert.deepStrictEqual(
        events.map((event) => event.eventIndex),
        events.map((_, index) => index),
      );
      assert.strictEqual(events.find((event) => event.type === "run_end")?.outcome, "completed");
      assert.strictEqual(modelCalls, 1);

      const duplicate = yield* AgentRunLifecycle.startRun(store, {
        runId,
        input: { prompt: "hello again" },
        scheduleExecution: () => Effect.die("duplicate should not schedule execution"),
      });
      assert.isFalse(duplicate.created);
      assert.strictEqual(modelCalls, 1);
    }),
  );
});

const waitForClosed = (store: ReturnType<typeof makeInMemoryEventStreamStore>, path: string) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const meta = yield* store.getStreamMeta(path);
      if (meta?.closed === true) return;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for stream ${path} to close.`);
  });

const emptyUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
