import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { AgentMessage, StreamFn } from "@earendil-works/pi-agent-core";
import { assert, describe, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
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
          isError: false,
          result: { assistantText: "done" },
        });
        yield* sessions.finishRun({
          conversationId: "conversation_1",
          runId: "run_1",
          isError: false,
          result: { assistantText: "done" },
        });

        assert.strictEqual(yield* countMessages(sql, "assistant"), 1);
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

const markStaleApplied = (sql: TestSqlStorage, submissionId: string) =>
  sql
    .exec(
      `UPDATE denora_agent_conversation_submissions
       SET status = 'running', attempt_id = 'attempt_crashed', started_at = ?, input_applied_at = ?
       WHERE submission_id = ?`,
      Date.now() - 16 * 60 * 1000,
      Date.now() - 16 * 60 * 1000,
      submissionId,
    )
    .pipe(Effect.asVoid);

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

const readSessionMessages = (sql: TestSqlStorage) =>
  Effect.gen(function* () {
    const cursor = yield* sql.exec<MessageRow>(
      `SELECT message_id, role, content_json
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
  readonly role: string;
  readonly content_json: string;
}

interface CountRow extends Record<string, string | number | null> {
  readonly count: number;
}

interface SubmissionStatusRow extends Record<string, string | number | null> {
  readonly status: string;
}
