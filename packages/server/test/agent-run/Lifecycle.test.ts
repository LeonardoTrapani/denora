import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import {
  agentStreamPath,
  makeInMemoryEventStreamStore,
  runStreamPath,
} from "../../src/agent-run/EventStreamStore.ts";
import { AgentRunLifecycle } from "../../src/agent-run/Lifecycle.ts";
import type { Interface as PiRuntimeInterface } from "../../src/agent-loop/PiRuntime.ts";

describe("AgentRunLifecycle", () => {
  it.effect("creates conversation submission streams without admission-time user events", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const instanceId = `conversation_${crypto.randomUUID()}`;
      const submissionId = `submission_${crypto.randomUUID()}`;
      const streamPath = agentStreamPath("denora", instanceId);

      const created = yield* AgentRunLifecycle.createConversationSubmission(store, {
        agentName: "denora",
        conversationId: instanceId,
        submissionId,
        runId: `run_${crypto.randomUUID()}`,
        triggerMessageId: `message_${crypto.randomUUID()}`,
        input: { submittedMessage: { text: "hello" } },
      });

      assert.strictEqual(created.streamPath, streamPath);
      assert.strictEqual(created.offset, "-1");
      const replay = yield* store.readEvents(streamPath, { offset: "-1" });
      assert.deepStrictEqual(replay.events, []);
    }),
  );

  it.effect("appends Flue-compatible attached agent user events when input is applied", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const instanceId = `conversation_${crypto.randomUUID()}`;
      const submissionId = `submission_${crypto.randomUUID()}`;
      const streamPath = agentStreamPath("denora", instanceId);

      yield* AgentRunLifecycle.createConversationSubmission(store, {
        agentName: "denora",
        conversationId: instanceId,
        submissionId,
        runId: `run_${crypto.randomUUID()}`,
        triggerMessageId: `message_${crypto.randomUUID()}`,
      });
      yield* AgentRunLifecycle.appendConversationUserMessageApplied(store, {
        agentName: "denora",
        conversationId: instanceId,
        submissionId,
        userTurnId: `submission:${submissionId}:user`,
        message: { role: "user", content: "hello" },
      });
      yield* AgentRunLifecycle.appendConversationUserMessageApplied(store, {
        agentName: "denora",
        conversationId: instanceId,
        submissionId,
        userTurnId: `submission:${submissionId}:user`,
        message: { role: "user", content: "hello" },
      });

      const replay = yield* store.readEvents(streamPath, { offset: "-1" });
      const events = replay.events.map((event) => event.data as Record<string, unknown>);
      assert.deepStrictEqual(
        events.map((event) => event.type),
        ["message_start", "message_end"],
      );
      for (const event of events) {
        assert.strictEqual(event.v, 3);
        assert.strictEqual(event.instanceId, instanceId);
        assert.strictEqual(event.conversationId, instanceId);
        assert.strictEqual(event.agentName, "denora");
        assert.strictEqual(event.submissionId, submissionId);
        assert.strictEqual(event.turnId, `submission:${submissionId}:user`);
        assert.notProperty(event, "runId");
        assert.notProperty(event, "messageId");
      }
    }),
  );

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
      assert.strictEqual(events[0]?.v, 3);
      assert.deepStrictEqual(events[0]?.input, { prompt: "hello" });
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
      const runEnd = events.find((event) => event.type === "run_end");
      assert.strictEqual(runEnd?.isError, false);
      assert.isNumber(runEnd?.durationMs);
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

  it.effect("flushes buffered streaming deltas while a run is still open", () =>
    Effect.gen(function* () {
      const store = makeInMemoryEventStreamStore();
      const runId = `run_${crypto.randomUUID()}`;
      const streamPath = runStreamPath(runId);
      let stream: ReturnType<typeof createAssistantMessageEventStream> | undefined;
      const message: AssistantMessage = {
        role: "assistant",
        content: [{ type: "text", text: "still streaming" }],
        api: "openai-completions",
        provider: "cloudflare-workers-ai",
        model: "@cf/meta/llama-3.1-8b-instruct-fast",
        usage: emptyUsage,
        stopReason: "stop",
        timestamp: Date.now(),
      };
      const pi: PiRuntimeInterface = {
        streamFn: (() => {
          stream = createAssistantMessageEventStream();
          return stream;
        }) satisfies StreamFn,
      };

      yield* AgentRunLifecycle.startRun(store, {
        runId,
        input: { prompt: "hello" },
        scheduleExecution: () =>
          AgentRunLifecycle.executeRun(store, { runId, input: { prompt: "hello" }, pi }).pipe(
            Effect.forkDetach({ startImmediately: true }),
            Effect.asVoid,
          ),
      });

      yield* waitForStream(() => stream);
      stream?.push({ type: "start", partial: { ...message, content: [] } });
      stream?.push({ type: "text_start", contentIndex: 0, partial: message });
      stream?.push({
        type: "text_delta",
        contentIndex: 0,
        delta: "still streaming",
        partial: message,
      });

      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 3_100)));
      const replay = yield* store.readEvents(streamPath, { offset: "-1" });
      const openEvents = replay.events.map((event) => event.data as Record<string, unknown>);
      assert.include(
        openEvents.map((event) => event.type),
        "text_delta",
      );
      assert.notInclude(
        openEvents.map((event) => event.type),
        "run_end",
      );

      stream?.push({
        type: "text_end",
        contentIndex: 0,
        content: "still streaming",
        partial: message,
      });
      stream?.push({ type: "done", reason: "stop", message });
      stream?.end();
      yield* waitForClosed(store, streamPath);
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

const waitForStream = <A>(get: () => A | undefined) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const value = get();
      if (value !== undefined) return value;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error("Timed out waiting for stream.");
  });

const emptyUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};
