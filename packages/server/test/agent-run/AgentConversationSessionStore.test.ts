import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { AgentMessage, StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import {
  AgentConversationCoordinator,
  type Interface as AgentConversationCoordinatorInterface,
} from "../../src/agent-run/AgentConversationCoordinator.ts";
import { AgentConversationSessionStore } from "../../src/agent-run/AgentConversationSessionStore.ts";
import {
  EventStreamStore as EventStreamStoreModule,
  agentStreamPath,
  type EventStreamStore,
} from "../../src/agent-run/EventStreamStore.ts";
import { AgentRunLifecycle } from "../../src/agent-run/Lifecycle.ts";
import { SqlStorage } from "../../src/agent-run/SqlStorage.ts";
import type { Interface as PiRuntimeInterface } from "../../src/agent-loop/PiRuntime.ts";
import {
  MAX_AGENT_CONVERSATION_IMAGE_DATA_LENGTH,
  MAX_AGENT_CONVERSATION_TEXT_LENGTH,
} from "../../src/agent-run/AgentConversationContentLimits.ts";
import { SqliteStorage, type TestSqliteStorage } from "../helpers/SqliteStorage.ts";

type TestSqlStorage = TestSqliteStorage["sql"];

describe("AgentConversationSessionStore", () => {
  it.effect("replaying the same submission admission does not duplicate input stream events", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        const firstAdmission = yield* coordinator.admitSubmission(input);
        const firstCreated = yield* AgentRunLifecycle.createConversationSubmission(store, input);
        const secondAdmission = yield* coordinator.admitSubmission(input);
        const secondCreated = yield* AgentRunLifecycle.createConversationSubmission(store, input);

        const replay = yield* store.readEvents(
          agentStreamPath(input.agentName, input.conversationId),
          {
            offset: "-1",
          },
        );
        const events = replay.events.map((event) => event.data as Record<string, unknown>);

        assert.isTrue(firstAdmission.admitted);
        assert.isFalse(secondAdmission.admitted);
        assert.isTrue(firstCreated.created);
        assert.isFalse(secondCreated.created);
        assert.deepStrictEqual(
          events.map((event) => event.type),
          ["message_start", "message_end"],
        );
        assert.strictEqual(yield* countSubmissions(sql), 1);
      }),
    ),
  );

  it.effect("rejects new submissions for inactive conversations", () =>
    withHarness(
      Effect.gen(function* () {
        const { coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "blocked" });

        yield* coordinator.setConversationLifecycle({
          conversationId: input.conversationId,
          status: "deleting",
        });
        const error = yield* coordinator.admitSubmission(input).pipe(Effect.flip);

        assert.strictEqual(error._tag, "EventStorageFailed");
        if (error._tag !== "EventStorageFailed") return;
        assert.strictEqual(error.operation, "admit agent conversation submission");
      }),
    ),
  );

  it.effect("settles queued submissions when a conversation is archived", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "queued archive" });

        yield* admitAndCreate(store, coordinator, input);
        yield* coordinator.setConversationLifecycle({
          conversationId: input.conversationId,
          status: "archived",
        });

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { message?: string };
        };
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "settled");
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(
          event.error?.message,
          `Conversation ${input.conversationId} is archived; agent submissions are not accepted.`,
        );
        assert.strictEqual(yield* countMessages(sql, "user"), 0);
        assert.strictEqual(yield* countMessages(sql, "assistant"), 0);
      }),
    ),
  );

  it.effect("does not persist assistant checkpoints after a conversation starts deleting", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "delete while running" });
        let stream: ReturnType<typeof createAssistantMessageEventStream> | undefined;
        const message: AssistantMessage = {
          role: "assistant",
          content: [{ type: "text", text: "must not persist" }],
          api: "openai-completions",
          provider: "cloudflare-workers-ai",
          model: "@cf/meta/llama-3.1-8b-instruct",
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

        yield* admitAndCreate(store, coordinator, input);
        const fiber = yield* Effect.forkChild(
          coordinator.reconcile({ pi, scheduleWake: () => Effect.void }),
        );

        yield* waitFor(() => stream !== undefined);
        yield* coordinator.setConversationLifecycle({
          conversationId: input.conversationId,
          status: "deleted",
        });
        stream?.push({ type: "start", partial: { ...message, content: [] } });
        stream?.push({ type: "text_start", contentIndex: 0, partial: message });
        stream?.push({
          type: "text_delta",
          contentIndex: 0,
          delta: "must not persist",
          partial: message,
        });
        stream?.push({
          type: "text_end",
          contentIndex: 0,
          content: "must not persist",
          partial: message,
        });
        stream?.push({ type: "done", reason: "stop", message });
        stream?.end();
        yield* Fiber.join(fiber);

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { message?: string };
        };
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "settled");
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(yield* countMessages(sql, "assistant"), 0);
      }),
    ),
  );

  it.effect("does not emit streaming deltas after deleting a started assistant turn", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "delete after assistant start" });
        let stream: ReturnType<typeof createAssistantMessageEventStream> | undefined;
        const message: AssistantMessage = {
          role: "assistant",
          content: [{ type: "text", text: "must not stream" }],
          api: "openai-completions",
          provider: "cloudflare-workers-ai",
          model: "@cf/meta/llama-3.1-8b-instruct",
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

        yield* admitAndCreate(store, coordinator, input);
        const fiber = yield* Effect.forkChild(
          coordinator.reconcile({ pi, scheduleWake: () => Effect.void }),
        );

        const streamPath = agentStreamPath(input.agentName, input.conversationId);
        yield* waitFor(() => stream !== undefined);
        stream?.push({ type: "start", partial: { ...message, content: [] } });
        yield* waitForMessageStatus(sql, `assistant:${input.runId}:0`, "started");
        yield* waitForStreamEvent(
          store,
          streamPath,
          (event) =>
            event.type === "message_start" &&
            (event.message as { readonly role?: unknown } | undefined)?.role === "assistant",
        );

        yield* coordinator.setConversationLifecycle({
          conversationId: input.conversationId,
          status: "deleted",
        });
        stream?.push({ type: "text_start", contentIndex: 0, partial: message });
        stream?.push({
          type: "text_delta",
          contentIndex: 0,
          delta: "must not stream",
          partial: message,
        });
        stream?.push({
          type: "text_end",
          contentIndex: 0,
          content: "must not stream",
          partial: message,
        });
        stream?.push({ type: "done", reason: "stop", message });
        stream?.end();
        yield* Fiber.join(fiber);

        const replay = yield* store.readEvents(streamPath, { offset: "-1" });
        const events = replay.events.map((event) => event.data as Record<string, unknown>);
        const assistant = (yield* readSessionMessages(sql)).find(
          (row) => row.message_id === `assistant:${input.runId}:0`,
        );

        assert.notInclude(
          events.map((event) => event.type),
          "text_delta",
        );
        assert.strictEqual(assistant?.status, "started");
        assert.strictEqual(assistant?.plain_text, "");
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "settled");
      }),
    ),
  );

  it.effect("does not duplicate the user input when the same submission is applied twice", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        const replay = yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        const messages = yield* readSessionMessages(sql);

        assert.deepStrictEqual(
          messages.map((message) => message.role),
          ["user"],
        );
        assert.strictEqual(messages[0]?.message_id, input.triggerMessageId);
        assert.strictEqual(messages[0]?.parent_message_id, null);
        assert.strictEqual(messages[0]?.submission_id, input.submissionId);
        assert.deepStrictEqual((replay.input as { readonly prompt: string }).prompt, "");
        assert.deepStrictEqual(
          (replay.input as { readonly messages: ReadonlyArray<AgentMessage> }).messages.map(
            (message) => message.role,
          ),
          ["user"],
        );
      }),
    ),
  );

  it.effect("rejects oversized user text before writing a session message row", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        const error = yield* sessions
          .recordSubmissionStarted({
            ...recordStartedInput(input),
            content: { text: "x".repeat(MAX_AGENT_CONVERSATION_TEXT_LENGTH + 1) },
          })
          .pipe(Effect.flip);

        assert.strictEqual(error._tag, "EventStorageFailed");
        assert.strictEqual(error.operation, "validate conversation session message");
        assert.strictEqual(yield* countMessages(sql, "user"), 0);
      }),
    ),
  );

  it.effect("rejects oversized image content before writing a session message row", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        const error = yield* sessions
          .recordSubmissionStarted({
            ...recordStartedInput(input),
            content: {
              text: "describe this image",
              image: {
                type: "image",
                data: "x".repeat(MAX_AGENT_CONVERSATION_IMAGE_DATA_LENGTH + 1),
                mimeType: "image/png",
              },
            },
          })
          .pipe(Effect.flip);

        assert.strictEqual(error._tag, "EventStorageFailed");
        assert.strictEqual(error.operation, "validate conversation session message");
        assert.strictEqual(yield* countMessages(sql, "user"), 0);
      }),
    ),
  );

  it.effect("rejects oversized submission payloads before admission rows are written", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        const error = yield* coordinator
          .admitSubmission({
            ...input,
            input: {
              userId: "user_1",
              submittedMessage: { text: "x".repeat(MAX_AGENT_CONVERSATION_TEXT_LENGTH + 1) },
            },
          })
          .pipe(Effect.flip);

        assert.strictEqual(error._tag, "EventStorageFailed");
        if (error._tag !== "EventStorageFailed") return;
        assert.strictEqual(error.operation, "validate agent conversation submission");
        assert.strictEqual(yield* countSubmissions(sql), 0);
      }),
    ),
  );

  it.effect("records assistant started checkpoints with stable ids idempotently", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* sessions.recordAssistantMessageStarted({
          conversationId: input.conversationId,
          runId: input.runId,
          submissionId: input.submissionId,
          messageIndex: 0,
        });
        yield* sessions.recordAssistantMessageStarted({
          conversationId: input.conversationId,
          runId: input.runId,
          submissionId: input.submissionId,
          messageIndex: 0,
        });

        const messages = yield* readSessionMessages(sql);
        assert.deepStrictEqual(
          messages.map((message) => message.message_id),
          [input.triggerMessageId, `assistant:${input.runId}:0`],
        );
        assert.strictEqual(messages[1]?.parent_message_id, input.triggerMessageId);
        assert.strictEqual(messages[1]?.status, "started");
        assert.strictEqual(messages[1]?.plain_text, "");
        assert.deepStrictEqual(JSON.parse(messages[1]?.parts_json ?? "null"), []);
      }),
    ),
  );

  it.effect("records assistant text and completed checkpoints idempotently", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "hello" });

        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        for (let attempt = 0; attempt < 2; attempt += 1) {
          yield* sessions.recordAssistantMessageStarted({
            conversationId: input.conversationId,
            runId: input.runId,
            submissionId: input.submissionId,
            messageIndex: 0,
          });
          yield* sessions.recordAssistantTextPartCompleted({
            conversationId: input.conversationId,
            runId: input.runId,
            submissionId: input.submissionId,
            messageIndex: 0,
            contentIndex: 0,
            text: "done",
          });
          yield* sessions.recordAssistantMessageCompleted({
            conversationId: input.conversationId,
            runId: input.runId,
            submissionId: input.submissionId,
            messageIndex: 0,
            parts: [{ type: "text", text: "done" }],
            plainText: "done",
          });
        }

        const messages = yield* readSessionMessages(sql);
        const completed = yield* sessions.reconstructCompletedRun({
          conversationId: input.conversationId,
          runId: input.runId,
        });

        assert.strictEqual(yield* countMessages(sql, "assistant"), 1);
        assert.strictEqual(messages[1]?.message_id, `assistant:${input.runId}:0`);
        assert.strictEqual(messages[1]?.status, "completed");
        assert.strictEqual(messages[1]?.plain_text, "done");
        assert.deepStrictEqual(JSON.parse(messages[1]?.parts_json ?? "null"), [
          { type: "text", text: "done" },
        ]);
        assert.deepStrictEqual(completed, { assistantText: "done" });
      }),
    ),
  );

  it.effect("persists assistant checkpoints during a running coordinator turn", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "stream me" });
        let stream: ReturnType<typeof createAssistantMessageEventStream> | undefined;
        const message: AssistantMessage = {
          role: "assistant",
          content: [{ type: "text", text: "streamed" }],
          api: "openai-completions",
          provider: "cloudflare-workers-ai",
          model: "@cf/meta/llama-3.1-8b-instruct",
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

        yield* admitAndCreate(store, coordinator, input);
        const fiber = yield* Effect.forkChild(
          coordinator.reconcile({ pi, scheduleWake: () => Effect.void }),
        );

        yield* waitFor(() => stream !== undefined);
        stream?.push({ type: "start", partial: { ...message, content: [] } });
        yield* waitForMessageStatus(sql, `assistant:${input.runId}:0`, "started");
        stream?.push({ type: "text_start", contentIndex: 0, partial: message });
        stream?.push({ type: "text_delta", contentIndex: 0, delta: "streamed", partial: message });
        stream?.push({ type: "text_end", contentIndex: 0, content: "streamed", partial: message });
        yield* waitForMessageStatus(sql, `assistant:${input.runId}:0`, "partial");

        const partial = (yield* readSessionMessages(sql)).find(
          (row) => row.message_id === `assistant:${input.runId}:0`,
        );
        assert.strictEqual(partial?.plain_text, "streamed");
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "running");

        stream?.push({ type: "done", reason: "stop", message });
        stream?.end();
        yield* Fiber.join(fiber);

        const completed = (yield* readSessionMessages(sql)).find(
          (row) => row.message_id === `assistant:${input.runId}:0`,
        );
        assert.strictEqual(completed?.status, "completed");
        assert.strictEqual(completed?.plain_text, "streamed");
      }),
    ),
  );

  it.effect("records turn journal phases monotonically through a coordinator turn", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "journal me" });
        let stream: ReturnType<typeof createAssistantMessageEventStream> | undefined;
        const message: AssistantMessage = {
          role: "assistant",
          content: [{ type: "text", text: "journaled" }],
          api: "openai-completions",
          provider: "cloudflare-workers-ai",
          model: "@cf/meta/llama-3.1-8b-instruct",
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

        yield* admitAndCreate(store, coordinator, input);
        const fiber = yield* Effect.forkChild(
          coordinator.reconcile({ pi, scheduleWake: () => Effect.void }),
        );

        yield* waitFor(() => stream !== undefined);
        assert.deepInclude(yield* readTurnJournal(sql, input.submissionId), {
          phase: "model_started",
          phase_order: 2,
          revision: 2,
        });

        stream?.push({ type: "start", partial: { ...message, content: [] } });
        yield* waitForTurnJournalPhase(sql, input.submissionId, "assistant_started");
        assert.deepInclude(yield* readTurnJournal(sql, input.submissionId), {
          phase: "assistant_started",
          phase_order: 3,
          revision: 3,
        });

        stream?.push({ type: "text_start", contentIndex: 0, partial: message });
        stream?.push({ type: "text_delta", contentIndex: 0, delta: "journaled", partial: message });
        stream?.push({ type: "text_end", contentIndex: 0, content: "journaled", partial: message });
        yield* waitForTurnJournalPhase(sql, input.submissionId, "assistant_checkpointed");
        assert.deepInclude(yield* readTurnJournal(sql, input.submissionId), {
          phase: "assistant_checkpointed",
          phase_order: 4,
          revision: 4,
        });

        stream?.push({ type: "done", reason: "stop", message });
        stream?.end();
        yield* Fiber.join(fiber);

        assert.deepInclude(yield* readTurnJournal(sql, input.submissionId), {
          phase: "settled",
          phase_order: 6,
          revision: 6,
        });
      }),
    ),
  );

  it.effect(
    "does not create ambiguous turn journal state when a settled submission is replayed",
    () =>
      withHarness(
        Effect.gen(function* () {
          const { sql, store, coordinator } = yield* AgentConversationHarness;
          const input = submissionInput({ text: "replay journal" });
          const contexts: Array<ReadonlyArray<AgentMessage>> = [];

          yield* admitAndCreate(store, coordinator, input);
          yield* coordinator.reconcile({
            pi: makePi(["done"], contexts),
            scheduleWake: () => Effect.void,
          });
          const beforeReplay = yield* readTurnJournal(sql, input.submissionId);

          yield* coordinator.reconcile({
            pi: makePi(["should not run"], contexts),
            scheduleWake: () => Effect.void,
          });
          const afterReplay = yield* readTurnJournal(sql, input.submissionId);

          assert.strictEqual(contexts.length, 1);
          assert.strictEqual(yield* countTurnJournals(sql, input.submissionId), 1);
          assert.deepStrictEqual(afterReplay, beforeReplay);
          assert.deepInclude(afterReplay, { phase: "settled", phase_order: 6, revision: 6 });
        }),
      ),
  );

  it.effect("records attempt marker lifecycle and snapshot state", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "mark me" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* coordinator.reconcile({
          pi: makePi(["marked"], contexts),
          scheduleWake: () => Effect.void,
        });

        const markers = yield* readAttemptMarkers(sql, input.submissionId);
        const marker = markers[0];
        assert.strictEqual(markers.length, 1);
        assert.strictEqual(marker?.name, "agent-conversation-submission");
        assert.strictEqual(marker?.status, "completed");
        assert.isNumber(marker?.completed_at);
        assert.deepInclude(JSON.parse(marker?.snapshot_json ?? "{}"), {
          submissionId: input.submissionId,
          runId: input.runId,
          phase: "terminal_reserved",
          isError: false,
        });
      }),
    ),
  );

  it.effect("reconciles unfinished attempt markers on wake without stale timeout", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "recover marker" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markRunningAppliedWithAttemptMarker(sql, input.submissionId, input.runId);

        yield* coordinator.reconcile({
          pi: makePi(["recovered from marker"], contexts),
          scheduleWake: () => Effect.void,
        });

        const markers = yield* readAttemptMarkers(sql, input.submissionId);
        assert.includeMembers(
          markers.map((marker) => marker.status),
          ["interrupted", "completed"],
        );
        assert.strictEqual(
          markers.find((marker) => marker.attempt_id === "attempt_crashed")?.status,
          "interrupted",
        );
        assert.strictEqual(contexts.length, 1);
        assert.deepStrictEqual(
          contexts[0]?.map((message) => message.role),
          ["user"],
        );
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "settled");
        assert.strictEqual(yield* countMessages(sql, "user"), 1);
      }),
    ),
  );

  it.effect("persists rich session message fields while preserving linear order", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        const first = submissionInput({
          submissionId: "submission_first",
          runId: "run_first",
          triggerMessageId: "message_first",
          text: "first",
        });
        const second = submissionInput({
          submissionId: "submission_second",
          runId: "run_second",
          triggerMessageId: "message_second",
          text: "second",
        });

        yield* sessions.recordSubmissionStarted(recordStartedInput(first));
        yield* sessions.finishRun({
          conversationId: first.conversationId,
          runId: first.runId,
          submissionId: first.submissionId,
          isError: false,
          result: { assistantText: "first reply" },
        });
        yield* sessions.recordSubmissionStarted(recordStartedInput(second));

        const messages = yield* readSessionMessages(sql);
        assert.deepStrictEqual(
          messages.map((message) => message.message_id),
          ["message_first", "assistant:run_first:0", "message_second"],
        );
        assert.deepStrictEqual(
          messages.map((message) => message.parent_message_id),
          [null, "message_first", "assistant:run_first:0"],
        );
        assert.deepStrictEqual(
          messages.map((message) => message.submission_id),
          ["submission_first", "submission_first", "submission_second"],
        );
        assert.deepStrictEqual(
          messages.map((message) => message.run_id),
          ["run_first", "run_first", "run_second"],
        );
        assert.deepStrictEqual(
          messages.map((message) => message.plain_text),
          ["first", "first reply", "second"],
        );
        assert.deepStrictEqual(
          messages.map((message) => message.status),
          ["completed", "completed", "completed"],
        );
        assert.deepStrictEqual(JSON.parse(messages[0]?.parts_json ?? "null"), [
          { type: "text", text: "first" },
        ]);
        assert.isString(messages[0]?.created_at);
        assert.strictEqual(messages[0]?.updated_at, messages[0]?.created_at);
      }),
    ),
  );

  it.effect("retries after input was applied using the persisted local session history", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "retry me" });
        yield* coordinator.admitSubmission(input);
        yield* AgentRunLifecycle.createConversationSubmission(store, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markStaleApplied(sql, input.submissionId);

        const contexts: Array<ReadonlyArray<AgentMessage>> = [];
        yield* coordinator.reconcile({
          pi: makePi(["recovered"], contexts),
          scheduleWake: () => Effect.void,
        });

        const messages = yield* readSessionMessages(sql);
        assert.deepStrictEqual(
          messages.map((message) => message.role),
          ["user", "assistant"],
        );
        assert.deepStrictEqual(
          contexts[0]?.map((message) => message.role),
          ["user"],
        );
        assert.strictEqual(yield* countMessages(sql, "user"), 1);
      }),
    ),
  );

  it.effect("fails safely when assistant progress exists without an input marker", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "contradictory progress" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordAssistantMessageStarted({
          conversationId: input.conversationId,
          runId: input.runId,
          submissionId: input.submissionId,
          messageIndex: 0,
        });
        yield* markStaleBeforeInput(sql, input.submissionId, {
          attemptCount: 1,
          maxAttempts: 3,
        });

        yield* coordinator.reconcile({
          pi: makePi(["should not run"], contexts),
          scheduleWake: () => Effect.void,
        });

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { message?: string };
        };
        const state = yield* readSubmissionState(sql, input.submissionId);
        assert.strictEqual(contexts.length, 0);
        assert.strictEqual(state.status, "settled");
        assert.strictEqual(
          state.last_error,
          "Agent run was interrupted after partial assistant progress and cannot be safely resumed yet. Please retry.",
        );
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(event.error?.message, state.last_error);
        assert.strictEqual(yield* countMessages(sql, "user"), 0);
        assert.strictEqual(yield* countMessages(sql, "assistant"), 1);
      }),
    ),
  );

  it.effect("increments attempt_count when retrying stale applied work", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "retry budget" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markStaleApplied(sql, input.submissionId, {
          attemptCount: 1,
          maxAttempts: 3,
        });

        yield* coordinator.reconcile({
          pi: makePi(["recovered with budget"], contexts),
          scheduleWake: () => Effect.void,
        });

        const state = yield* readSubmissionState(sql, input.submissionId);
        assert.strictEqual(state.status, "settled");
        assert.strictEqual(state.attempt_count, 2);
        assert.strictEqual(state.max_attempts, 3);
        assert.strictEqual(contexts.length, 1);
      }),
    ),
  );

  it.effect("settles retry-exhausted stale work with a visible failure terminal", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "exhaust me" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markStaleApplied(sql, input.submissionId, {
          attemptCount: 3,
          maxAttempts: 3,
        });

        yield* coordinator.reconcile({
          pi: makePi(["should not run"], contexts),
          scheduleWake: () => Effect.void,
        });

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { message?: string };
        };
        const state = yield* readSubmissionState(sql, input.submissionId);
        assert.strictEqual(contexts.length, 0);
        assert.strictEqual(state.status, "settled");
        assert.strictEqual(state.attempt_count, 3);
        assert.strictEqual(
          state.last_error,
          "Agent run exceeded its retry budget after 3 of 3 attempts.",
        );
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(event.error?.message, state.last_error);
      }),
    ),
  );

  it.effect("detects stale running work from an expired advisory lease", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "expired lease" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markRunningApplied(sql, input.submissionId, {
          attemptCount: 1,
          maxAttempts: 3,
          startedAt: Date.now(),
          inputAppliedAt: Date.now(),
          leaseExpiresAt: Date.now() - 1,
        });

        yield* coordinator.reconcile({
          pi: makePi(["lease recovered"], contexts),
          scheduleWake: () => Effect.void,
        });

        const state = yield* readSubmissionState(sql, input.submissionId);
        assert.strictEqual(contexts.length, 1);
        assert.strictEqual(state.status, "settled");
        assert.strictEqual(state.attempt_count, 2);
      }),
    ),
  );

  it.effect("settles timed-out running work without retrying", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "timeout" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* sessions.recordSubmissionStarted(recordStartedInput(input));
        yield* markRunningApplied(sql, input.submissionId, {
          attemptCount: 1,
          maxAttempts: 3,
          startedAt: Date.now(),
          inputAppliedAt: Date.now(),
          leaseExpiresAt: Date.now() - 1,
          timeoutAt: Date.now() - 1,
        });

        yield* coordinator.reconcile({
          pi: makePi(["should not run"], contexts),
          scheduleWake: () => Effect.void,
        });

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as {
          readonly outcome?: unknown;
          readonly error?: { message?: string };
        };
        const state = yield* readSubmissionState(sql, input.submissionId);
        assert.strictEqual(contexts.length, 0);
        assert.strictEqual(state.status, "settled");
        assert.strictEqual(state.last_error, "Agent run timed out after 1 of 3 attempts.");
        assert.strictEqual(event.outcome, "failed");
        assert.strictEqual(event.error?.message, state.last_error);
      }),
    ),
  );

  it.effect("publishes a pending terminal outbox before invoking any model work", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, coordinator } = yield* AgentConversationHarness;
        const input = submissionInput({ text: "terminal outbox" });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, input);
        yield* markPendingTerminalOutbox(sql, input);

        yield* coordinator.reconcile({
          pi: makePi(["should not run"], contexts),
          scheduleWake: () => Effect.void,
        });

        const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
        const event = terminal?.event as Record<string, unknown> | undefined;
        assert.strictEqual(contexts.length, 0);
        assert.strictEqual(yield* readSubmissionStatus(sql, input.submissionId), "settled");
        assert.strictEqual(event?.type, "submission_settled");
        assert.strictEqual(event?.outcome, "completed");
        assert.deepStrictEqual(event?.result, { assistantText: "already terminal" });
      }),
    ),
  );

  it.effect(
    "settles from a persisted assistant final after crash without re-running the model",
    () =>
      withHarness(
        Effect.gen(function* () {
          const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
          const input = submissionInput({ text: "recover completed" });
          const contexts: Array<ReadonlyArray<AgentMessage>> = [];

          yield* coordinator.admitSubmission(input);
          yield* AgentRunLifecycle.createConversationSubmission(store, input);
          yield* sessions.recordSubmissionStarted(recordStartedInput(input));
          yield* sessions.finishRun({
            conversationId: input.conversationId,
            runId: input.runId,
            submissionId: input.submissionId,
            isError: false,
            result: { assistantText: "already done" },
          });
          yield* markStaleApplied(sql, input.submissionId);

          yield* coordinator.reconcile({
            pi: makePi(["should not run"], contexts),
            scheduleWake: () => Effect.void,
          });

          const terminal = yield* coordinator.getSubmissionTerminal(input.submissionId);
          const event = terminal?.event as Record<string, unknown> | undefined;
          assert.strictEqual(contexts.length, 0);
          assert.strictEqual(event?.type, "submission_settled");
          assert.strictEqual(event?.outcome, "completed");
          assert.deepStrictEqual(event?.result, { assistantText: "already done" });
          assert.strictEqual(yield* countMessages(sql, "assistant"), 1);
        }),
      ),
  );

  it.effect("does not duplicate the assistant final message for the same run", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, sessions } = yield* AgentConversationHarness;
        yield* sessions.finishRun({
          conversationId: "conversation_1",
          runId: "run_1",
          submissionId: "submission_1",
          isError: false,
          result: { assistantText: "done" },
        });
        yield* sessions.finishRun({
          conversationId: "conversation_1",
          runId: "run_1",
          submissionId: "submission_1",
          isError: false,
          result: { assistantText: "done" },
        });

        const messages = yield* readSessionMessages(sql);
        assert.strictEqual(yield* countMessages(sql, "assistant"), 1);
        assert.strictEqual(messages[0]?.message_id, "assistant:run_1:0");
        assert.strictEqual(messages[0]?.submission_id, "submission_1");
      }),
    ),
  );

  it.effect("uses prior locally persisted messages for later model context", () =>
    withHarness(
      Effect.gen(function* () {
        const { store, coordinator } = yield* AgentConversationHarness;
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];
        const pi = makePi(["first reply", "second reply"], contexts);

        const first = submissionInput({
          submissionId: "submission_first",
          runId: "run_first",
          triggerMessageId: "message_first",
          text: "first",
        });
        yield* coordinator.admitSubmission(first);
        yield* AgentRunLifecycle.createConversationSubmission(store, first);
        yield* coordinator.reconcile({ pi, scheduleWake: () => Effect.void });

        const second = submissionInput({
          submissionId: "submission_second",
          runId: "run_second",
          triggerMessageId: "message_second",
          text: "second",
        });
        yield* coordinator.admitSubmission(second);
        yield* AgentRunLifecycle.createConversationSubmission(store, second);
        yield* coordinator.reconcile({ pi, scheduleWake: () => Effect.void });

        assert.deepStrictEqual(
          contexts[1]?.map((message) => message.role),
          ["user", "assistant", "user"],
        );
        assert.deepStrictEqual(
          (contexts[1]?.[1] as Extract<AgentMessage, { role: "assistant" }> | undefined)?.content,
          [{ type: "text", text: "first reply" }],
        );
      }),
    ),
  );

  it.effect(
    "does not claim a later same-session submission while an earlier submission is running",
    () =>
      withHarness(
        Effect.gen(function* () {
          const { sql, store, coordinator } = yield* AgentConversationHarness;
          const first = submissionInput({
            submissionId: "submission_first_running",
            runId: "run_first_running",
            triggerMessageId: "message_first_running",
            text: "first",
          });
          const second = submissionInput({
            submissionId: "submission_second_blocked",
            runId: "run_second_blocked",
            triggerMessageId: "message_second_blocked",
            text: "second",
          });
          const contexts: Array<ReadonlyArray<AgentMessage>> = [];

          yield* admitAndCreate(store, coordinator, first);
          yield* admitAndCreate(store, coordinator, second);
          yield* markRunning(sql, first.submissionId);

          yield* coordinator.reconcile({
            pi: makePi(["should not run"], contexts),
            scheduleWake: () => Effect.void,
          });

          assert.strictEqual(contexts.length, 0);
          assert.strictEqual(yield* readSubmissionStatus(sql, second.submissionId), "queued");

          yield* markSettled(sql, first.submissionId);
          yield* coordinator.reconcile({
            pi: makePi(["second reply"], contexts),
            scheduleWake: () => Effect.void,
          });

          assert.strictEqual(contexts.length, 1);
          assert.strictEqual(yield* readSubmissionStatus(sql, second.submissionId), "settled");
        }),
      ),
  );

  it.effect(
    "claims a different session while an earlier active submission blocks only its own session",
    () =>
      withHarness(
        Effect.gen(function* () {
          const { sql, store, coordinator } = yield* AgentConversationHarness;
          const running = submissionInput({
            submissionId: "submission_active_session_a",
            runId: "run_active_session_a",
            triggerMessageId: "message_active_session_a",
            conversationId: "conversation_a",
            text: "active a",
          });
          const blocked = submissionInput({
            submissionId: "submission_blocked_session_a",
            runId: "run_blocked_session_a",
            triggerMessageId: "message_blocked_session_a",
            conversationId: "conversation_a",
            text: "blocked a",
          });
          const runnable = submissionInput({
            submissionId: "submission_runnable_session_b",
            runId: "run_runnable_session_b",
            triggerMessageId: "message_runnable_session_b",
            conversationId: "conversation_b",
            text: "runnable b",
          });
          const contexts: Array<ReadonlyArray<AgentMessage>> = [];

          yield* admitAndCreate(store, coordinator, running);
          yield* admitAndCreate(store, coordinator, blocked);
          yield* admitAndCreate(store, coordinator, runnable);
          yield* markRunning(sql, running.submissionId);

          yield* coordinator.reconcile({
            pi: makePi(["session b reply"], contexts),
            scheduleWake: () => Effect.void,
          });

          assert.strictEqual(contexts.length, 1);
          assert.strictEqual(yield* readSubmissionStatus(sql, blocked.submissionId), "queued");
          assert.strictEqual(yield* readSubmissionStatus(sql, runnable.submissionId), "settled");
        }),
      ),
  );

  it.effect(
    "does not claim a later same-session submission while an earlier submission is terminalizing",
    () =>
      withHarness(
        Effect.gen(function* () {
          const { sql, store, coordinator } = yield* AgentConversationHarness;
          const terminalizing = submissionInput({
            submissionId: "submission_terminalizing_session_a",
            runId: "run_terminalizing_session_a",
            triggerMessageId: "message_terminalizing_session_a",
            conversationId: "conversation_terminalizing_a",
            text: "terminalizing a",
          });
          const blocked = submissionInput({
            submissionId: "submission_after_terminalizing_a",
            runId: "run_after_terminalizing_a",
            triggerMessageId: "message_after_terminalizing_a",
            conversationId: "conversation_terminalizing_a",
            text: "after terminalizing a",
          });
          const runnable = submissionInput({
            submissionId: "submission_terminalizing_session_b",
            runId: "run_terminalizing_session_b",
            triggerMessageId: "message_terminalizing_session_b",
            conversationId: "conversation_terminalizing_b",
            text: "terminalizing b",
          });
          const contexts: Array<ReadonlyArray<AgentMessage>> = [];

          yield* admitAndCreate(store, coordinator, terminalizing);
          yield* admitAndCreate(store, coordinator, blocked);
          yield* admitAndCreate(store, coordinator, runnable);
          yield* markTerminalizing(sql, terminalizing.submissionId);

          yield* coordinator.reconcile({
            pi: makePi(["session b reply"], contexts),
            scheduleWake: () => Effect.void,
          });

          assert.strictEqual(contexts.length, 1);
          assert.strictEqual(
            yield* readSubmissionStatus(sql, terminalizing.submissionId),
            "terminalizing",
          );
          assert.strictEqual(yield* readSubmissionStatus(sql, blocked.submissionId), "queued");
          assert.strictEqual(yield* readSubmissionStatus(sql, runnable.submissionId), "settled");
        }),
      ),
  );

  it.effect("retries a stale earlier submission before a later same-session submission", () =>
    withHarness(
      Effect.gen(function* () {
        const { sql, store, sessions, coordinator } = yield* AgentConversationHarness;
        const first = submissionInput({
          submissionId: "submission_stale_first",
          runId: "run_stale_first",
          triggerMessageId: "message_stale_first",
          text: "retry first",
        });
        const second = submissionInput({
          submissionId: "submission_after_stale",
          runId: "run_after_stale",
          triggerMessageId: "message_after_stale",
          text: "after stale",
        });
        const contexts: Array<ReadonlyArray<AgentMessage>> = [];

        yield* admitAndCreate(store, coordinator, first);
        yield* admitAndCreate(store, coordinator, second);
        yield* sessions.recordSubmissionStarted(recordStartedInput(first));
        yield* markStaleApplied(sql, first.submissionId);

        yield* coordinator.reconcile({
          pi: makePi(["first recovered", "second reply"], contexts),
          scheduleWake: () => Effect.void,
        });

        assert.strictEqual(contexts.length, 2);
        assert.deepStrictEqual(
          contexts[0]?.map((message) => message.role),
          ["user"],
        );
        assert.deepStrictEqual(
          contexts[1]?.map((message) => message.role),
          ["user", "assistant", "user"],
        );
        assert.strictEqual(yield* readSubmissionStatus(sql, first.submissionId), "settled");
        assert.strictEqual(yield* readSubmissionStatus(sql, second.submissionId), "settled");
        assert.strictEqual(yield* countMessages(sql, "user"), 2);
      }),
    ),
  );
});

interface AgentConversationHarnessValue {
  readonly sql: TestSqlStorage;
  readonly store: EventStreamStore;
  readonly sessions: AgentConversationSessionStore.Interface;
  readonly coordinator: AgentConversationCoordinatorInterface;
}

class AgentConversationHarness extends Context.Service<
  AgentConversationHarness,
  AgentConversationHarnessValue
>()("test/AgentConversationHarness") {}

const agentConversationHarnessLayer = Layer.effect(
  AgentConversationHarness,
  Effect.gen(function* () {
    const sqlite = yield* SqliteStorage.Service;
    const store = yield* EventStreamStoreModule.Service;
    const sessions = yield* AgentConversationSessionStore.Service;
    const coordinator = yield* AgentConversationCoordinator.Service;
    return AgentConversationHarness.of({
      sql: sqlite.sql,
      store,
      sessions,
      coordinator,
    });
  }),
).pipe(
  Layer.provideMerge(AgentConversationCoordinator.sqliteLayer),
  Layer.provideMerge(EventStreamStoreModule.sqliteLayer),
  Layer.provideMerge(AgentConversationSessionStore.sqliteLayer),
  Layer.provideMerge(
    Layer.effect(
      SqlStorage.Service,
      Effect.gen(function* () {
        const sqlite = yield* SqliteStorage.Service;
        return SqlStorage.Service.of(sqlite.sql);
      }),
    ),
  ),
  Layer.provide(SqliteStorage.layer),
);

const withHarness = <A, E>(effect: Effect.Effect<A, E, AgentConversationHarness>) =>
  effect.pipe(Effect.provide(agentConversationHarnessLayer));

const submissionInput = (
  options: {
    readonly submissionId?: string | undefined;
    readonly runId?: string | undefined;
    readonly triggerMessageId?: string | undefined;
    readonly conversationId?: string | undefined;
    readonly text: string;
  } = { text: "hello" },
) => ({
  runId: options.runId ?? "run_1",
  agentName: "default",
  conversationId: options.conversationId ?? "conversation_1",
  submissionId: options.submissionId ?? "submission_1",
  triggerMessageId: options.triggerMessageId ?? "message_1",
  input: { userId: "user_1", submittedMessage: { text: options.text } },
});

const admitAndCreate = (
  store: EventStreamStore,
  coordinator: AgentConversationCoordinatorInterface,
  input: ReturnType<typeof submissionInput>,
) =>
  Effect.gen(function* () {
    yield* coordinator.admitSubmission(input);
    yield* AgentRunLifecycle.createConversationSubmission(store, input);
  });

const recordStartedInput = (input: ReturnType<typeof submissionInput>) => ({
  conversationId: input.conversationId,
  userId: "user_1",
  agentName: input.agentName,
  messageId: input.triggerMessageId,
  submissionId: input.submissionId,
  runId: input.runId,
  content: (input.input as { readonly submittedMessage: unknown }).submittedMessage,
});

const makePi = (
  replies: ReadonlyArray<string>,
  contexts: Array<ReadonlyArray<AgentMessage>>,
): PiRuntimeInterface => {
  let calls = 0;
  return {
    streamFn: ((model, context) => {
      contexts.push(context.messages.slice() as ReadonlyArray<AgentMessage>);
      const text = replies[calls++] ?? "reply";
      const stream = createAssistantMessageEventStream();
      const message: AssistantMessage = {
        role: "assistant",
        content: [{ type: "text", text }],
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
        stream.push({ type: "text_delta", contentIndex: 0, delta: text, partial: message });
        stream.push({ type: "text_end", contentIndex: 0, content: text, partial: message });
        stream.push({ type: "done", reason: "stop", message });
        stream.end();
      });

      return stream;
    }) satisfies StreamFn,
  };
};

const markStaleApplied = (
  sql: TestSqlStorage,
  submissionId: string,
  options: { readonly attemptCount?: number; readonly maxAttempts?: number } = {},
) =>
  markRunningApplied(sql, submissionId, {
    attemptCount: options.attemptCount ?? 0,
    maxAttempts: options.maxAttempts ?? 3,
    startedAt: Date.now() - 16 * 60 * 1000,
    inputAppliedAt: Date.now() - 16 * 60 * 1000,
    leaseExpiresAt: Date.now() - 1,
  });

const markRunningApplied = (
  sql: TestSqlStorage,
  submissionId: string,
  options: {
    readonly attemptCount: number;
    readonly maxAttempts: number;
    readonly startedAt: number;
    readonly inputAppliedAt: number;
    readonly leaseExpiresAt?: number | undefined;
    readonly timeoutAt?: number | undefined;
  },
) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'running', attempt_id = 'attempt_crashed', started_at = ?, input_applied_at = ?,
           attempt_count = ?, max_attempts = ?, lease_expires_at = ?, timeout_at = ?
       WHERE submission_id = ?`,
      options.startedAt,
      options.inputAppliedAt,
      options.attemptCount,
      options.maxAttempts,
      options.leaseExpiresAt ?? options.startedAt + 15 * 60 * 1000,
      options.timeoutAt ?? options.startedAt + 45 * 60 * 1000,
      submissionId,
    )
    .pipe(Effect.asVoid);

const markStaleBeforeInput = (
  sql: TestSqlStorage,
  submissionId: string,
  options: { readonly attemptCount: number; readonly maxAttempts: number },
) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'running', attempt_id = 'attempt_crashed', started_at = ?, input_applied_at = NULL,
           attempt_count = ?, max_attempts = ?, lease_expires_at = ?, timeout_at = ?
       WHERE submission_id = ?`,
      Date.now() - 16 * 60 * 1000,
      options.attemptCount,
      options.maxAttempts,
      Date.now() - 1,
      Date.now() + 45 * 60 * 1000,
      submissionId,
    )
    .pipe(Effect.asVoid);

const markRunningAppliedWithAttemptMarker = (
  sql: TestSqlStorage,
  submissionId: string,
  runId: string,
) =>
  Effect.gen(function* () {
    const now = Date.now();
    yield* sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET status = 'running', attempt_id = 'attempt_crashed', started_at = ?, input_applied_at = ?,
             attempt_count = 1, max_attempts = 3, lease_expires_at = ?, timeout_at = ?
         WHERE submission_id = ?`,
        now,
        now,
        now - 1,
        now + 45 * 60 * 1000,
        submissionId,
      )
      .pipe(Effect.asVoid);
    yield* sql
      .exec(
        `INSERT INTO denora_agent_attempt_markers
         (attempt_id, submission_id, name, status, snapshot_json, started_at, updated_at, completed_at)
         VALUES ('attempt_crashed', ?, 'agent-conversation-submission', 'running', ?, ?, ?, NULL)`,
        submissionId,
        JSON.stringify({
          submissionId,
          attemptId: "attempt_crashed",
          runId,
          phase: "model_started",
        }),
        now,
        now,
      )
      .pipe(Effect.asVoid);
  });

const markRunning = (sql: TestSqlStorage, submissionId: string) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'running', attempt_id = 'attempt_active', started_at = ?
       WHERE submission_id = ?`,
      Date.now(),
      submissionId,
    )
    .pipe(Effect.asVoid);

const markSettled = (sql: TestSqlStorage, submissionId: string) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'settled', settled_at = ?
       WHERE submission_id = ?`,
      Date.now(),
      submissionId,
    )
    .pipe(Effect.asVoid);

const markTerminalizing = (sql: TestSqlStorage, submissionId: string) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'terminalizing', attempt_id = 'attempt_terminalizing'
       WHERE submission_id = ?`,
      submissionId,
    )
    .pipe(Effect.asVoid);

const markPendingTerminalOutbox = (
  sql: TestSqlStorage,
  input: ReturnType<typeof submissionInput>,
) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'terminalizing', attempt_id = 'attempt_terminalizing',
           terminal_event_key = ?, terminal_event_json = ?
       WHERE submission_id = ?`,
      `agent-conversation:${input.submissionId}:terminal`,
      JSON.stringify({
        v: 3,
        type: "submission_settled",
        instanceId: input.conversationId,
        agentName: input.agentName,
        submissionId: input.submissionId,
        timestamp: new Date().toISOString(),
        outcome: "completed",
        result: { assistantText: "already terminal" },
      }),
      input.submissionId,
    )
    .pipe(Effect.asVoid);

const waitFor = (predicate: () => boolean) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      if (predicate()) return;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error("Timed out waiting for condition.");
  });

const waitForStreamEvent = (
  store: EventStreamStore,
  path: string,
  predicate: (event: Record<string, unknown>) => boolean,
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const replay = yield* store.readEvents(path, { offset: "-1" });
      const events = replay.events.map((event) => event.data as Record<string, unknown>);
      if (events.some(predicate)) return;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for stream event in ${path}.`);
  });

const waitForMessageStatus = (sql: TestSqlStorage, messageId: string, status: string) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const cursor = yield* sql.exec<MessageStatusRow>(
        `SELECT status
         FROM denora_agent_conversation_session_messages
         WHERE message_id = ?
         LIMIT 1`,
        messageId,
      );
      const rows = yield* cursor.toArray();
      if (rows[0]?.status === status) return;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for ${messageId} status ${status}.`);
  });

const waitForTurnJournalPhase = (sql: TestSqlStorage, submissionId: string, phase: string) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 50; attempt += 1) {
      const journal = yield* readTurnJournal(sql, submissionId);
      if (journal?.phase === phase) return;
      yield* Effect.promise(() => new Promise((resolve) => setTimeout(resolve, 10)));
    }
    throw new Error(`Timed out waiting for ${submissionId} turn journal phase ${phase}.`);
  });

const readTurnJournal = (sql: TestSqlStorage, submissionId: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<TurnJournalRow>(
      `SELECT submission_id, attempt_id, phase, phase_order, revision
       FROM denora_agent_turn_journals
       WHERE submission_id = ?
       LIMIT 1`,
      submissionId,
    );
    const rows = yield* cursor.toArray();
    return rows[0] ?? null;
  });

const readSubmissionStatus = (sql: TestSqlStorage, submissionId: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<SubmissionStatusRow>(
      `SELECT status
       FROM denora_agent_conversation_submissions
       WHERE submission_id = ?
       LIMIT 1`,
      submissionId,
    );
    return (yield* cursor.one()).status;
  });

const readSubmissionState = (sql: TestSqlStorage, submissionId: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<SubmissionStateRow>(
      `SELECT status, attempt_count, max_attempts, last_error, timeout_at, lease_expires_at
       FROM denora_agent_conversation_submissions
       WHERE submission_id = ?
       LIMIT 1`,
      submissionId,
    );
    return yield* cursor.one();
  });

const readSessionMessages = (sql: TestSqlStorage) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<MessageRow>(
      `SELECT message_id, conversation_id, parent_message_id, run_id, submission_id, role,
              parts_json, plain_text, status, created_at, updated_at
       FROM denora_agent_conversation_session_messages
       ORDER BY sequence ASC`,
    );
    return yield* cursor.toArray();
  });

const countMessages = (sql: TestSqlStorage, role: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<CountRow>(
      `SELECT COUNT(*) AS count
       FROM denora_agent_conversation_session_messages
       WHERE role = ?`,
      role,
    );
    return (yield* cursor.one()).count;
  });

const countSubmissions = (sql: TestSqlStorage) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<CountRow>(
      `SELECT COUNT(*) AS count
       FROM denora_agent_conversation_submissions`,
    );
    return (yield* cursor.one()).count;
  });

const countTurnJournals = (sql: TestSqlStorage, submissionId: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<CountRow>(
      `SELECT COUNT(*) AS count
       FROM denora_agent_turn_journals
       WHERE submission_id = ?`,
      submissionId,
    );
    return (yield* cursor.one()).count;
  });

const readAttemptMarkers = (sql: TestSqlStorage, submissionId: string) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<AttemptMarkerRow>(
      `SELECT attempt_id, submission_id, name, status, snapshot_json, started_at, updated_at, completed_at
       FROM denora_agent_attempt_markers
       WHERE submission_id = ?
       ORDER BY started_at ASC, updated_at ASC`,
      submissionId,
    );
    return yield* cursor.toArray();
  });

const emptyUsage = {
  input: 0,
  output: 0,
  cacheRead: 0,
  cacheWrite: 0,
  totalTokens: 0,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
};

interface MessageRow extends Record<string, string | number | null> {
  readonly message_id: string;
  readonly conversation_id: string;
  readonly parent_message_id: string | null;
  readonly run_id: string | null;
  readonly submission_id: string | null;
  readonly role: string;
  readonly parts_json: string;
  readonly plain_text: string;
  readonly status: string;
  readonly created_at: string;
  readonly updated_at: string;
}

interface CountRow extends Record<string, string | number | null> {
  readonly count: number;
}

interface SubmissionStatusRow extends Record<string, string | number | null> {
  readonly status: string;
}

interface SubmissionStateRow extends Record<string, string | number | null> {
  readonly status: string;
  readonly attempt_count: number;
  readonly max_attempts: number;
  readonly last_error: string | null;
  readonly timeout_at: number;
  readonly lease_expires_at: number;
}

interface MessageStatusRow extends Record<string, string | number | null> {
  readonly status: string;
}

interface TurnJournalRow extends Record<string, string | number | null> {
  readonly submission_id: string;
  readonly attempt_id: string;
  readonly phase: string;
  readonly phase_order: number;
  readonly revision: number;
}

interface AttemptMarkerRow extends Record<string, string | number | null> {
  readonly attempt_id: string;
  readonly submission_id: string;
  readonly name: string;
  readonly status: string;
  readonly snapshot_json: string | null;
  readonly started_at: number;
  readonly updated_at: number;
  readonly completed_at: number | null;
}
