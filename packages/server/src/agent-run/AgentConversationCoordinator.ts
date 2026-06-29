import type * as Cloudflare from "alchemy/Cloudflare";
import { RuntimeContext } from "alchemy";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  AgentRunLifecycle,
  type CreateConversationSubmissionInput,
  type ExecuteRunAttemptResult,
} from "./Lifecycle.ts";
import { AgentConversationSessionStore } from "./AgentConversationSessionStore.ts";
import {
  assertAgentConversationContentWithinLimits,
  assertAgentConversationJsonWithinLimits,
} from "./AgentConversationContentLimits.ts";
import {
  type EventStreamError,
  type EventStreamStore,
  EventSerializationFailed,
  EventStorageFailed,
  Service as EventStreamStoreService,
  agentStreamPath,
  formatOffset,
  parseOffset,
} from "./EventStreamStore.ts";
import type { Interface as PiRuntimeInterface } from "../agent-loop/PiRuntime.ts";
import { DurableFiber } from "./DurableFiber.ts";
import { SqlStorage } from "./SqlStorage.ts";
import { StreamChunks } from "./StreamChunks.ts";

const WAKE_DELAY_MS = 30_000;
const RUNNING_STALE_MS = 15 * 60 * 1000;
const DEFAULT_MAX_ATTEMPTS = 3;
const DEFAULT_SUBMISSION_TIMEOUT_MS = DEFAULT_MAX_ATTEMPTS * RUNNING_STALE_MS;

const CREATE_TURN_JOURNALS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_turn_journals (
  submission_id      TEXT PRIMARY KEY,
  session_key        TEXT NOT NULL,
  kind               TEXT NOT NULL,
  attempt_id         TEXT NOT NULL,
  run_id             TEXT NOT NULL,
  operation_id       TEXT,
  turn_id            TEXT,
  phase              TEXT NOT NULL,
  phase_order        INTEGER NOT NULL,
  revision           INTEGER NOT NULL,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL,
  checkpoint_leaf_id TEXT,
  tool_request_json  TEXT,
  stream_key         TEXT,
  stream_consumed_at INTEGER,
  committed          INTEGER NOT NULL DEFAULT 0,
  committed_leaf_id  TEXT
)`;

const CREATE_SUBMISSIONS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_conversation_submissions (
  sequence              INTEGER PRIMARY KEY AUTOINCREMENT,
  submission_id         TEXT NOT NULL UNIQUE,
  session_key           TEXT NOT NULL,
  kind                  TEXT NOT NULL,
  run_id                TEXT NOT NULL,
  agent_name            TEXT NOT NULL DEFAULT 'default',
  conversation_id       TEXT,
  message_id            TEXT,
  parent_message_id     TEXT,
  payload               TEXT NOT NULL,
  status                TEXT NOT NULL,
  accepted_at           INTEGER NOT NULL,
  attempt_id            TEXT,
  started_at            INTEGER,
  settled_at            INTEGER,
  abort_requested_at    INTEGER,
  input_applied_at      INTEGER,
  attempt_count         INTEGER NOT NULL DEFAULT 0,
  max_attempts          INTEGER NOT NULL DEFAULT ${DEFAULT_MAX_ATTEMPTS},
  last_error            TEXT,
  timeout_at            INTEGER NOT NULL DEFAULT 0,
  lease_expires_at      INTEGER NOT NULL DEFAULT 0,
  error                 TEXT,
  terminal_event_key    TEXT,
  terminal_event_json   TEXT,
  terminal_event_offset TEXT
)`;

const CREATE_SUBMISSIONS_STATUS_INDEX = `
CREATE INDEX IF NOT EXISTS denora_agent_conversation_submissions_status_sequence_idx
ON denora_agent_conversation_submissions (status, sequence ASC)`;

const CREATE_SESSION_LIFECYCLES_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_conversation_session_lifecycles (
  session_key     TEXT PRIMARY KEY,
  conversation_id TEXT NOT NULL,
  status          TEXT NOT NULL,
  updated_at      INTEGER NOT NULL
)`;

const CREATE_ATTEMPT_MARKERS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_attempt_markers (
  attempt_id    TEXT PRIMARY KEY,
  submission_id TEXT NOT NULL,
  name          TEXT NOT NULL,
  status        TEXT NOT NULL,
  snapshot_json TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  max_attempts  INTEGER NOT NULL DEFAULT ${DEFAULT_MAX_ATTEMPTS},
  last_error    TEXT,
  started_at    INTEGER NOT NULL,
  updated_at    INTEGER NOT NULL,
  completed_at  INTEGER
)`;

const CREATE_ATTEMPT_MARKERS_UNFINISHED_INDEX = `
CREATE INDEX IF NOT EXISTS denora_agent_attempt_markers_unfinished_idx
ON denora_agent_attempt_markers (status, started_at ASC)
WHERE status IN ('running')`;

const SUBMISSION_ATTEMPT_MARKER_NAME = "agent-conversation-submission";

export interface AdmitRunResult {
  readonly admitted: boolean;
}

export interface SubmissionTerminalResult {
  readonly event: unknown;
}

export interface ReconcileInput {
  readonly pi: PiRuntimeInterface;
  readonly scheduleWake: (delayMs: number) => Effect.Effect<void, EventStorageFailed>;
}

export interface AbortConversationInput {
  readonly reason?: string | undefined;
}

export interface AbortConversationResult {
  readonly abortedSubmissions: number;
  readonly needsWake: boolean;
  readonly wakeDelayMs: number;
}

export type ConversationLifecycleState =
  | "active"
  | "archiving"
  | "archived"
  | "deleting"
  | "deleted";

export interface ReconcileResult {
  readonly needsWake: boolean;
  readonly wakeDelayMs: number;
}

export interface Interface {
  readonly admitSubmission: (
    input: CreateConversationSubmissionInput,
  ) => Effect.Effect<AdmitRunResult, EventStreamError>;
  readonly abortConversation: (
    input?: AbortConversationInput,
  ) => Effect.Effect<AbortConversationResult, EventStreamError>;
  readonly setConversationLifecycle: (input: {
    readonly conversationId: string;
    readonly status: ConversationLifecycleState;
  }) => Effect.Effect<AbortConversationResult, EventStreamError>;
  readonly getSubmissionTerminal: (
    submissionId: string,
  ) => Effect.Effect<SubmissionTerminalResult | null, EventStreamError>;
  readonly reconcile: (input: ReconcileInput) => Effect.Effect<ReconcileResult, EventStreamError>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/AgentConversationCoordinator",
) {}

export const sqliteLayer: Layer.Layer<
  Service,
  EventStorageFailed,
  | SqlStorage.Service
  | EventStreamStoreService
  | AgentConversationSessionStore.Service
  | StreamChunks.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sql = yield* SqlStorage.Service;
    const store = yield* EventStreamStoreService;
    const sessions = yield* AgentConversationSessionStore.Service;
    const streamChunks = yield* StreamChunks.Service;
    const coordinator = yield* makeSqliteAgentConversationCoordinator(
      sql,
      store,
      sessions,
      streamChunks,
    );
    return Service.of(coordinator);
  }),
);

export const makeSqliteAgentConversationCoordinator = Effect.fn(
  "AgentConversationCoordinator.makeSqliteAgentConversationCoordinator",
)(function* (
  sql: Cloudflare.SqlStorage,
  store: EventStreamStore,
  sessionStore: AgentConversationSessionStore.Interface,
  streamChunks: StreamChunks.StreamChunkStore,
): Effect.fn.Return<Interface, EventStorageFailed> {
  yield* ensureTables(sql);
  const durableFibers = yield* DurableFiber.makeSqlite(sql);
  const activeAttempts = new Map<string, AbortController>();

  const persistAssistantStreamChunk = Effect.fn(
    "AgentConversationCoordinator.persistAssistantStreamChunk",
  )(function* (
    writer: StreamChunks.StreamChunkWriter,
    event: StreamChunks.AssistantStreamChunkEvent,
    submission: Submission,
  ): Effect.fn.Return<void> {
    yield* writer.write(event).pipe(
      Effect.catch((error) =>
        Effect.logWarning("agent private stream chunk write failed", {
          runId: submission.runId,
          submissionId: submission.submissionId,
          attemptId: submission.attemptId,
          streamKey: writer.streamKey,
          error,
        }),
      ),
    );
  });

  const closeStreamWriter = (
    writer: StreamChunks.StreamChunkWriter,
    submission: Submission,
  ): Effect.Effect<void> =>
    writer.close().pipe(
      Effect.catch((error) =>
        Effect.logWarning("agent private stream chunk close failed", {
          runId: submission.runId,
          submissionId: submission.submissionId,
          attemptId: submission.attemptId,
          streamKey: writer.streamKey,
          error,
        }),
      ),
    );

  const deleteTerminalOutboxStreamChunks = Effect.fn(
    "AgentConversationCoordinator.deleteTerminalOutboxStreamChunks",
  )(function* (outbox: TerminalOutbox): Effect.fn.Return<void, EventStreamError> {
    const journal = yield* readTurnJournal(outbox.submissionId);
    const streamKey = journal?.streamKey;
    if (streamKey === undefined) return;

    yield* streamChunks.deleteStreamChunkSegments(streamKey).pipe(
      Effect.mapError(
        (cause) =>
          new EventStorageFailed({
            operation: "delete terminal outbox stream chunks",
            cause,
          }),
      ),
    );
    yield* markStreamChunksConsumed(outbox.submissionId, streamKey);
  });

  const admitSubmission = Effect.fn("AgentConversationCoordinator.admitSubmission")(function* (
    input: CreateConversationSubmissionInput,
  ): Effect.fn.Return<AdmitRunResult, EventStreamError> {
    yield* rejectInactiveConversation(input.conversationId, "admit agent conversation submission");
    yield* validateSubmissionPayload(input.input);
    const payload = yield* stringify(input.input);
    yield* validateSerializedSubmissionPayload(payload);
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `INSERT OR IGNORE INTO denora_agent_conversation_submissions
         (submission_id, session_key, kind, run_id, agent_name, conversation_id, message_id,
          parent_message_id, payload, status, accepted_at)
         VALUES (?, ?, 'message', ?, ?, ?, ?, ?, ?, 'queued', ?)
          RETURNING ${submissionColumns}`,
        input.submissionId,
        sessionKey(input.conversationId),
        input.runId,
        input.agentName,
        input.conversationId ?? null,
        input.triggerMessageId ?? null,
        input.parentMessageId ?? null,
        payload,
        Date.now(),
      )
      .pipe(storageFailure("admit agent conversation submission"));
    const inserted = yield* cursor.toArray().pipe(storageFailure("collect admitted submission"));
    if (inserted[0] !== undefined) return { admitted: true };

    const existing = yield* readSubmission(input.submissionId);
    if (existing === null) {
      return yield* new EventStorageFailed({
        operation: "admit agent conversation submission",
        cause: new Error("Submission insert returned no row and no existing submission."),
      });
    }
    if (existing.payload !== payload || existing.parentMessageId !== input.parentMessageId) {
      return yield* new EventStorageFailed({
        operation: "admit agent conversation submission",
        cause: new Error(`Submission ${input.submissionId} already has conflicting input.`),
      });
    }
    return { admitted: false };
  });

  const abortConversation = Effect.fn("AgentConversationCoordinator.abortConversation")(function* (
    input: AbortConversationInput = {},
  ): Effect.fn.Return<AbortConversationResult, EventStreamError> {
    const message = input.reason ?? "Agent conversation was aborted by the user.";
    const abortable = yield* listAbortableSubmissions();
    let abortedSubmissions = 0;
    for (const submission of abortable) {
      abortedSubmissions += 1;
      if (submission.status === "running") {
        const controller = activeAttempts.get(submission.submissionId);
        if (controller !== undefined) {
          yield* markAbortRequested(submission);
          controller.abort(new Error(message));
          continue;
        }
      }
      yield* reserveTerminal(submission, yield* makeInterruptedResult(submission, message), {
        attemptId: submission.attemptId ?? `abort:${crypto.randomUUID()}`,
        fromStatuses: ["queued", "running"],
        markerStatus: "interrupted",
      });
    }
    yield* publishTerminalOutboxes();
    const unsettled = yield* hasUnsettledSubmissions();
    return { abortedSubmissions, needsWake: unsettled, wakeDelayMs: WAKE_DELAY_MS };
  });

  const setConversationLifecycle = Effect.fn(
    "AgentConversationCoordinator.setConversationLifecycle",
  )(function* (input: {
    readonly conversationId: string;
    readonly status: ConversationLifecycleState;
  }): Effect.fn.Return<AbortConversationResult, EventStreamError> {
    const key = sessionKey(input.conversationId);
    yield* sql
      .exec(
        `INSERT INTO denora_agent_conversation_session_lifecycles
         (session_key, conversation_id, status, updated_at)
         VALUES (?, ?, ?, ?)
         ON CONFLICT(session_key) DO UPDATE SET
           conversation_id = excluded.conversation_id,
           status = excluded.status,
           updated_at = excluded.updated_at`,
        key,
        input.conversationId,
        input.status,
        Date.now(),
      )
      .pipe(storageFailure("set conversation lifecycle"), Effect.asVoid);
    const abortedSubmissions =
      input.status === "active"
        ? 0
        : yield* interruptActiveAttemptsForSession(
            key,
            inactiveConversationMessage(input.conversationId, input.status),
          );
    yield* settleInactiveSubmissions();
    yield* publishTerminalOutboxes();
    const unsettled = yield* hasUnsettledSubmissions();
    return { abortedSubmissions, needsWake: unsettled, wakeDelayMs: WAKE_DELAY_MS };
  });

  const reconcile = Effect.fn("AgentConversationCoordinator.reconcile")(function* (
    input: ReconcileInput,
  ): Effect.fn.Return<ReconcileResult, EventStreamError> {
    while (true) {
      // Recovery is intentionally ordered: terminal outbox publication always wins
      // before any branch that could invoke or requeue model work.
      yield* publishTerminalOutboxes();
      yield* settleInactiveSubmissions();
      yield* publishTerminalOutboxes();
      yield* recoverInterruptedDurableFibers();
      yield* publishTerminalOutboxes();
      yield* reconcileUnfinishedAttemptMarkers();
      yield* publishTerminalOutboxes();
      yield* interruptExpiredRunningSubmissions();
      yield* publishTerminalOutboxes();
      yield* settleQueuedRetryExhaustedSubmissions();
      yield* publishTerminalOutboxes();

      const claim = yield* claimNextSubmission();
      if (claim === null) break;
      yield* processClaimedSubmission(claim, input.pi);
      yield* publishTerminalOutboxes();
    }

    const unsettled = yield* hasUnsettledSubmissions();
    if (unsettled) yield* input.scheduleWake(WAKE_DELAY_MS);
    return { needsWake: unsettled, wakeDelayMs: WAKE_DELAY_MS };
  });

  const getSubmissionTerminal = Effect.fn("AgentConversationCoordinator.getSubmissionTerminal")(
    function* (
      submissionId: string,
    ): Effect.fn.Return<SubmissionTerminalResult | null, EventStreamError> {
      const submission = yield* readSubmission(submissionId);
      if (submission?.status !== "settled") return null;
      const cursor = yield* sql
        .exec<TerminalEventRow>(
          `SELECT terminal_event_json
         FROM denora_agent_conversation_submissions
         WHERE submission_id = ? AND status = 'settled' AND terminal_event_json IS NOT NULL
         LIMIT 1`,
          submissionId,
        )
        .pipe(storageFailure("read settled submission terminal"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect settled terminal"));
      const row = rows[0];
      if (row === undefined) return null;
      const event = yield* Effect.try({
        try: () => JSON.parse(row.terminal_event_json) as unknown,
        catch: (cause) => new EventStorageFailed({ operation: "parse settled terminal", cause }),
      });
      return { event };
    },
  );

  const readSubmission = Effect.fn("AgentConversationCoordinator.readSubmission")(function* (
    submissionId: string,
  ): Effect.fn.Return<Submission | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
          FROM denora_agent_conversation_submissions
         WHERE submission_id = ?
         LIMIT 1`,
        submissionId,
      )
      .pipe(storageFailure("read agent conversation submission"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect agent conversation submission"));
    return rows[0] === undefined ? null : parseSubmission(rows[0]);
  });

  const listAbortableSubmissions = Effect.fn(
    "AgentConversationCoordinator.listAbortableSubmissions",
  )(function* (): Effect.fn.Return<ReadonlyArray<Submission>, EventStorageFailed> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
         FROM denora_agent_conversation_submissions
         WHERE status IN ('queued', 'running')
         ORDER BY sequence ASC`,
      )
      .pipe(storageFailure("list abortable agent conversation submissions"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect abortable submissions"));
    return rows.map(parseSubmission);
  });

  const markAbortRequested = (submission: Submission): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET abort_requested_at = COALESCE(abort_requested_at, ?)
         WHERE submission_id = ? AND status = 'running'`,
        Date.now(),
        submission.submissionId,
      )
      .pipe(storageFailure("mark agent conversation submission abort requested"), Effect.asVoid);

  const markSubmissionInputApplied = (
    submission: Submission,
  ): Effect.Effect<void, EventStorageFailed> =>
    Effect.gen(function* () {
      const cursor = yield* sql
        .exec<Row>(
          `UPDATE denora_agent_conversation_submissions
           SET input_applied_at = COALESCE(input_applied_at, ?)
           WHERE submission_id = ? AND status = 'running' AND attempt_id = ?
           RETURNING 1 AS value`,
          Date.now(),
          submission.submissionId,
          submission.attemptId ?? "",
        )
        .pipe(storageFailure("mark conversation submission input applied"));
      const rows = yield* cursor
        .toArray()
        .pipe(storageFailure("collect conversation submission input marker"));
      if (rows[0] !== undefined) return;
      return yield* new EventStorageFailed({
        operation: "mark conversation submission input applied",
        cause: new Error(
          `Submission ${submission.submissionId} lost ownership before input application.`,
        ),
      });
    });

  const hasUnsettledSubmissions = Effect.fn("AgentConversationCoordinator.hasUnsettledSubmissions")(
    function* (): Effect.fn.Return<boolean, EventStorageFailed> {
      const cursor = yield* sql
        .exec<Row>(
          `SELECT 1 AS value
            FROM denora_agent_conversation_submissions
           WHERE status IN ('queued', 'running', 'terminalizing')
           LIMIT 1`,
        )
        .pipe(storageFailure("detect unsettled agent conversation submissions"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect unsettled submissions"));
      return rows[0] !== undefined;
    },
  );

  const claimNextSubmission = Effect.fn("AgentConversationCoordinator.claimNextSubmission")(
    function* (): Effect.fn.Return<Submission | null, EventStorageFailed> {
      const cursor = yield* sql
        .exec<SubmissionRow>(
          `UPDATE denora_agent_conversation_submissions
           SET status = 'running', attempt_id = ?, started_at = ?,
               attempt_count = attempt_count + 1,
               timeout_at = CASE WHEN timeout_at = 0 THEN ? ELSE timeout_at END,
               lease_expires_at = ?
            WHERE submission_id = (
             SELECT current.submission_id
              FROM denora_agent_conversation_submissions AS current
             WHERE current.status = 'queued'
               AND current.attempt_count < current.max_attempts
               AND NOT EXISTS (
                 SELECT 1
                   FROM denora_agent_conversation_submissions AS earlier
                  WHERE earlier.session_key = current.session_key
                    AND earlier.status IN ('queued', 'running', 'terminalizing')
                    AND earlier.sequence < current.sequence
               )
             ORDER BY current.sequence ASC
             LIMIT 1
           )
           RETURNING ${submissionColumns}`,
          crypto.randomUUID(),
          Date.now(),
          Date.now() + DEFAULT_SUBMISSION_TIMEOUT_MS,
          Date.now() + RUNNING_STALE_MS,
        )
        .pipe(storageFailure("claim agent conversation submission"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect claimed submission"));
      return rows[0] === undefined ? null : parseSubmission(rows[0]);
    },
  );

  const processClaimedSubmission = Effect.fn(
    "AgentConversationCoordinator.processClaimedSubmission",
  )(function* (
    submission: Submission,
    pi: PiRuntimeInterface,
  ): Effect.fn.Return<void, EventStreamError> {
    if (submission.attemptId === undefined) return;
    const controller = new AbortController();
    const claimedSnapshot = attemptSnapshot(submission, {
      phase: "claimed",
      submissionId: submission.submissionId,
      attemptId: submission.attemptId,
      runId: submission.runId,
    });
    yield* durableFibers.startManaged(
      {
        fiberId: submission.attemptId,
        idempotencyKey: submissionAttemptIdempotencyKey(submission),
        name: SUBMISSION_ATTEMPT_MARKER_NAME,
        metadata: {
          submissionId: submission.submissionId,
          attemptId: submission.attemptId,
          runId: submission.runId,
          agentName: submission.agentName,
          conversationId: submission.conversationId ?? null,
        },
        initialSnapshot: claimedSnapshot,
        signal: controller.signal,
      },
      (fiber) =>
        Effect.gen(function* () {
          activeAttempts.set(submission.submissionId, controller);
          yield* startAttemptMarker(submission, {
            phase: "claimed",
            submissionId: submission.submissionId,
            attemptId: submission.attemptId,
            runId: submission.runId,
          });
          if (yield* settleIfInactiveSubmission(submission)) return;
          const completed = yield* reconstructCompletedRunResult(submission);
          if (completed !== null) {
            yield* stashAttemptSnapshot(submission, fiber, {
              phase: "terminal_reserved",
              isError: completed.isError,
            });
            yield* reserveTerminal(submission, completed);
            return;
          }
          const prepared = yield* prepareSubmissionForExecution(submission);
          yield* appendAppliedUserMessage(submission, prepared);
          yield* markSubmissionInputApplied(submission);
          yield* recordTurnJournalPhase(submission, "before_provider");
          yield* stashAttemptSnapshot(submission, fiber, { phase: "before_provider" });
          const streamKey = streamChunkKey(submission);
          const streamWriter = StreamChunks.makeStreamChunkWriter(streamChunks, streamKey);
          yield* recordTurnJournalPhase(submission, "provider_started", { streamKey });
          yield* stashAttemptSnapshot(submission, fiber, { phase: "provider_started", streamKey });
          const execution = yield* AgentRunLifecycle.executeConversationSubmissionAttempt(store, {
            runId: submission.runId,
            agentName: submission.agentName,
            conversationId: submission.conversationId ?? "unknown",
            submissionId: submission.submissionId,
            triggerMessageId: submission.messageId ?? "unknown",
            input: prepared.input,
            pi,
            initialAssistantMessageIndex: prepared.nextAssistantMessageIndex,
            beforeEmitEvent: (event) =>
              rejectInactiveSubmission(submission, `emit ${event.type} event`),
            onAssistantStreamEvent: (event) =>
              persistAssistantStreamChunk(streamWriter, event, submission),
            onCheckpoint: (checkpoint) => {
              const conversationId = submission.conversationId ?? "unknown";
              switch (checkpoint.type) {
                case "assistant_message_started":
                  return Effect.gen(function* () {
                    yield* rejectInactiveSubmission(submission, "record assistant message start");
                    yield* sessionStore.recordAssistantMessageStarted({
                      conversationId,
                      runId: checkpoint.runId,
                      submissionId: submission.submissionId,
                      messageIndex: checkpoint.messageIndex,
                    });
                    yield* stashAttemptSnapshot(submission, fiber, {
                      phase: "provider_started",
                      messageIndex: checkpoint.messageIndex,
                    });
                  });
                case "assistant_text_part_completed":
                  return Effect.gen(function* () {
                    yield* rejectInactiveSubmission(submission, "record assistant text checkpoint");
                    yield* sessionStore.recordAssistantTextPartCompleted({
                      conversationId,
                      runId: checkpoint.runId,
                      submissionId: submission.submissionId,
                      messageIndex: checkpoint.messageIndex,
                      contentIndex: checkpoint.contentIndex,
                      text: checkpoint.text,
                    });
                    yield* stashAttemptSnapshot(submission, fiber, {
                      phase: "provider_started",
                      messageIndex: checkpoint.messageIndex,
                      contentIndex: checkpoint.contentIndex,
                    });
                  });
                case "assistant_message_completed":
                  return Effect.gen(function* () {
                    yield* rejectInactiveSubmission(
                      submission,
                      "record assistant message completion",
                    );
                    yield* sessionStore.recordAssistantMessageCompleted({
                      conversationId,
                      runId: checkpoint.runId,
                      submissionId: submission.submissionId,
                      messageIndex: checkpoint.messageIndex,
                      parts: checkpoint.message.content,
                      plainText: assistantPlainText(checkpoint.message.content),
                    });
                    if (assistantMessageHasToolCall(checkpoint.message)) {
                      yield* recordTurnJournalPhase(submission, "tool_request_recorded", {
                        checkpointLeafId: assistantMessageCheckpointLeafId(checkpoint),
                        toolRequest: toolRequestFromAssistantMessage(checkpoint.message),
                      });
                      yield* stashAttemptSnapshot(submission, fiber, {
                        phase: "tool_request_recorded",
                        messageIndex: checkpoint.messageIndex,
                      });
                    } else {
                      yield* recordTurnJournalPhase(submission, "committed", {
                        committedLeafId: assistantMessageCheckpointLeafId(checkpoint),
                      });
                      yield* stashAttemptSnapshot(submission, fiber, {
                        phase: "committed",
                        messageIndex: checkpoint.messageIndex,
                      });
                    }
                  });
                case "tool_call_started":
                  return Effect.gen(function* () {
                    yield* rejectInactiveSubmission(submission, "record tool call checkpoint");
                    yield* sessionStore.recordToolCallCheckpoint({
                      conversationId,
                      runId: checkpoint.runId,
                      submissionId: submission.submissionId,
                      toolCallId: checkpoint.toolCallId,
                      name: checkpoint.toolName,
                      args: checkpoint.args,
                    });
                    yield* recordTurnJournalPhase(submission, "tool_request_recorded", {
                      checkpointLeafId: checkpoint.checkpointId,
                      toolRequest: toolRequestFromCheckpoint(checkpoint),
                    });
                    yield* stashAttemptSnapshot(submission, fiber, {
                      phase: "tool_request_recorded",
                      toolCallId: checkpoint.toolCallId,
                      toolName: checkpoint.toolName,
                    });
                  });
                case "tool_result_completed":
                  return Effect.gen(function* () {
                    yield* rejectInactiveSubmission(submission, "record tool result checkpoint");
                    yield* sessionStore.recordToolResultCheckpoint({
                      conversationId,
                      runId: checkpoint.runId,
                      submissionId: submission.submissionId,
                      toolCallId: checkpoint.toolCallId,
                      name: checkpoint.toolName,
                      result: checkpoint.result,
                      isError: checkpoint.isError,
                    });
                    yield* recordTurnJournalPhase(submission, "committed", {
                      committedLeafId: checkpoint.checkpointId,
                    });
                    yield* stashAttemptSnapshot(submission, fiber, {
                      phase: "committed",
                      toolCallId: checkpoint.toolCallId,
                      toolName: checkpoint.toolName,
                      isError: checkpoint.isError,
                    });
                  });
              }
            },
            signal: controller.signal,
          }).pipe(
            Effect.ensuring(closeStreamWriter(streamWriter, submission)),
            Effect.catch((error) =>
              Effect.gen(function* () {
                yield* Effect.logError(
                  "agent run attempt execution failed before terminal outbox",
                  {
                    runId: submission.runId,
                    submissionId: submission.submissionId,
                    attemptId: submission.attemptId,
                    error,
                  },
                );
                yield* stashAttemptSnapshot(submission, fiber, { phase: "execution_failed" });
                return yield* makeInterruptedResult(submission, errorMessage(error));
              }),
            ),
          );
          if (yield* settleIfInactiveSubmission(submission)) return;
          yield* sessionStore.finishRun({
            conversationId: submission.conversationId ?? "unknown",
            runId: submission.runId,
            submissionId: submission.submissionId,
            isError: execution.isError,
            result: execution.result,
          });
          yield* stashAttemptSnapshot(submission, fiber, {
            phase: "terminal_reserved",
            isError: execution.isError,
            ...(execution.error?.message !== undefined ? { error: execution.error.message } : {}),
          });
          yield* reserveTerminal(submission, execution, {
            markerStatus: controller.signal.aborted ? "interrupted" : undefined,
          });
        }).pipe(
          Effect.catch((error) =>
            Effect.gen(function* () {
              yield* settleAttemptMarker(submission, "failed", {
                phase: "failed",
                error: error instanceof Error ? error.message : String(error),
              }).pipe(Effect.catch(() => Effect.void));
              return yield* error;
            }),
          ),
          Effect.ensuring(Effect.sync(() => activeAttempts.delete(submission.submissionId))),
        ),
    );
  });

  const interruptExpiredRunningSubmissions = Effect.fn(
    "AgentConversationCoordinator.interruptExpiredRunningSubmissions",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const now = Date.now();
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
          FROM denora_agent_conversation_submissions
         WHERE status = 'running'
           AND (
             (lease_expires_at > 0 AND lease_expires_at <= ?)
             OR (timeout_at > 0 AND timeout_at <= ?)
             OR (started_at IS NOT NULL AND started_at <= ?)
           )
          ORDER BY sequence ASC`,
        now,
        now,
        now - RUNNING_STALE_MS,
      )
      .pipe(storageFailure("list expired running submissions"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect expired running submissions"));
    for (const row of rows) {
      const submission = parseSubmission(row);
      const active = activeAttempts.get(submission.submissionId);
      if (active !== undefined) {
        if (isTimedOut(submission)) active.abort(new Error(timeoutErrorMessage(submission)));
        continue;
      }
      yield* reconcileInterruptedSubmission(submission);
    }
  });

  const requeueInterruptedSubmission = (
    submission: Submission,
    lastError: string,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET status = 'queued', attempt_id = NULL, started_at = NULL,
             lease_expires_at = 0, last_error = ?
         WHERE submission_id = ? AND status = 'running' AND attempt_id = ?
           AND attempt_count < max_attempts`,
        lastError,
        submission.submissionId,
        submission.attemptId ?? "",
      )
      .pipe(storageFailure("requeue interrupted conversation submission"), Effect.asVoid);

  const failInterruptedSubmission = (
    submission: Submission,
    message: string,
    phase: string,
    options: { readonly markerStatus?: TerminalAttemptMarkerStatus | undefined } = {},
  ): Effect.Effect<void, EventStreamError> =>
    Effect.gen(function* () {
      yield* reserveTerminal(submission, yield* makeInterruptedResult(submission, message), {
        markerStatus: options.markerStatus ?? "failed",
      });
      yield* settleAttemptMarker(submission, options.markerStatus ?? "failed", {
        phase,
        error: message,
      });
    });

  const settleQueuedRetryExhaustedSubmissions = Effect.fn(
    "AgentConversationCoordinator.settleQueuedRetryExhaustedSubmissions",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
          FROM denora_agent_conversation_submissions
         WHERE status = 'queued' AND attempt_count >= max_attempts
         ORDER BY sequence ASC`,
      )
      .pipe(storageFailure("list retry-exhausted queued submissions"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect retry-exhausted queued submissions"));
    for (const submission of rows.map(parseSubmission)) {
      yield* reserveTerminal(
        submission,
        yield* makeInterruptedResult(submission, retryExhaustedMessage(submission)),
        {
          attemptId: `exhausted:${crypto.randomUUID()}`,
          fromStatuses: ["queued"],
          markerStatus: "failed",
        },
      );
    }
  });

  const settleInactiveSubmissions = Effect.fn(
    "AgentConversationCoordinator.settleInactiveSubmissions",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const cursor = yield* sql
      .exec<SubmissionWithLifecycleRow>(
        `SELECT ${submissionColumnsFor("s")}, l.status AS lifecycle_status
          FROM denora_agent_conversation_submissions AS s
          INNER JOIN denora_agent_conversation_session_lifecycles AS l
             ON l.session_key = s.session_key
         WHERE s.status IN ('queued', 'running')
           AND l.status IN ('archiving', 'archived', 'deleting', 'deleted')
         ORDER BY s.sequence ASC`,
      )
      .pipe(storageFailure("list inactive conversation submissions"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect inactive conversation submissions"));
    for (const row of rows) {
      const submission = parseSubmission(row);
      const lifecycleStatus = row.lifecycle_status;
      const message = inactiveConversationMessage(
        submission.conversationId ?? "unknown",
        lifecycleStatus,
      );
      const active = activeAttempts.get(submission.submissionId);
      if (active !== undefined) {
        yield* markAbortRequested(submission);
        active.abort(new Error(message));
        continue;
      }
      yield* reserveTerminal(submission, yield* makeInterruptedResult(submission, message), {
        attemptId: submission.attemptId ?? `lifecycle:${crypto.randomUUID()}`,
        fromStatuses: ["queued", "running"],
        markerStatus: "interrupted",
      });
    }
  });

  const interruptActiveAttemptsForSession = Effect.fn(
    "AgentConversationCoordinator.interruptActiveAttemptsForSession",
  )(function* (key: string, message: string): Effect.fn.Return<number, EventStorageFailed> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
          FROM denora_agent_conversation_submissions
         WHERE session_key = ? AND status = 'running'
         ORDER BY sequence ASC`,
        key,
      )
      .pipe(storageFailure("list active lifecycle-interrupted submissions"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect active lifecycle-interrupted submissions"));
    let aborted = 0;
    for (const row of rows) {
      const submission = parseSubmission(row);
      const active = activeAttempts.get(submission.submissionId);
      if (active === undefined) continue;
      aborted += 1;
      yield* markAbortRequested(submission);
      active.abort(new Error(message));
    }
    return aborted;
  });

  const publishTerminalOutboxes = Effect.fn("AgentConversationCoordinator.publishTerminalOutboxes")(
    function* (): Effect.fn.Return<void, EventStreamError> {
      const outboxes = yield* listPendingTerminalOutboxes();
      for (const outbox of outboxes) {
        const streamPath = agentStreamPath(outbox.agentName, outbox.conversationId);
        const existing =
          outbox.offset === undefined
            ? yield* readAppendedEvent(streamPath, outbox.eventKey)
            : null;
        const event =
          existing?.event ??
          (outbox.offset === undefined
            ? yield* reindexEventForAppend(streamPath, outbox.event)
            : outbox.event);
        if (event !== outbox.event) yield* updateTerminalEvent(outbox, event);
        const offset =
          existing?.offset ??
          outbox.offset ??
          (yield* store.appendEventOnce(streamPath, outbox.eventKey, event));
        yield* recordTerminalOffset(outbox, offset);
        const idleKey = idleEventKey(outbox.submissionId);
        const existingIdle = yield* readAppendedEvent(streamPath, idleKey);
        if (existingIdle === null) {
          const idleIndex = yield* nextStreamEventIndex(streamPath);
          yield* store.appendEventOnce(streamPath, idleKey, idleEvent(outbox, idleIndex));
        }
        yield* deleteTerminalOutboxStreamChunks(outbox);
        yield* finalizeTerminal(outbox);
      }
    },
  );

  const listPendingTerminalOutboxes = Effect.fn(
    "AgentConversationCoordinator.listPendingTerminalOutboxes",
  )(function* (): Effect.fn.Return<ReadonlyArray<TerminalOutbox>, EventStorageFailed> {
    const cursor = yield* sql
      .exec<TerminalOutboxRow>(
        `SELECT submission_id, run_id, agent_name, conversation_id, attempt_id, terminal_event_key,
                  terminal_event_json, terminal_event_offset
            FROM denora_agent_conversation_submissions
           WHERE status = 'terminalizing'
             AND terminal_event_key IS NOT NULL
             AND terminal_event_json IS NOT NULL
           ORDER BY sequence ASC`,
      )
      .pipe(storageFailure("list terminal outboxes"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect terminal outboxes"));
    return yield* Effect.forEach(rows, parseTerminalOutbox);
  });

  const reserveTerminal = (
    submission: Submission,
    execution: ExecuteRunAttemptResult,
    options: {
      readonly attemptId?: string | undefined;
      readonly fromStatuses?: ReadonlyArray<SubmissionStatus> | undefined;
      readonly markerStatus?: TerminalAttemptMarkerStatus | undefined;
    } = {},
  ): Effect.Effect<void, EventStreamError> =>
    stringify(execution.terminalEvent).pipe(
      Effect.flatMap((eventJson) =>
        Effect.gen(function* () {
          const attemptId = options.attemptId ?? submission.attemptId ?? "";
          const fromStatuses = options.fromStatuses ?? ["running"];
          const statusPlaceholders = fromStatuses.map(() => "?").join(", ");
          const cursor = yield* sql
            .exec<Row>(
              `UPDATE denora_agent_conversation_submissions
               SET status = 'terminalizing', attempt_id = COALESCE(attempt_id, ?),
                    terminal_event_key = ?, terminal_event_json = ?, error = ?, last_error = ?
               WHERE submission_id = ? AND status IN (${statusPlaceholders})
                 AND (attempt_id = ? OR attempt_id IS NULL)
               RETURNING 1 AS value`,
              attemptId,
              terminalEventKey(submission.submissionId),
              eventJson,
              execution.isError ? (execution.error?.message ?? "Agent run failed.") : null,
              execution.isError ? (execution.error?.message ?? "Agent run failed.") : null,
              submission.submissionId,
              ...fromStatuses,
              attemptId,
            )
            .pipe(storageFailure("reserve terminal outbox"));
          const rows = yield* cursor
            .toArray()
            .pipe(storageFailure("collect terminal outbox reservation"));
          if (rows[0] !== undefined) {
            yield* recordTurnJournalPhase(submission, "terminal_reserved");
            yield* settleAttemptMarker(
              submission,
              options.markerStatus ?? attemptMarkerTerminalStatus(execution),
              {
                phase: "terminal_reserved",
                isError: execution.isError,
                ...(execution.error?.message !== undefined
                  ? { error: execution.error.message }
                  : {}),
              },
            );
          }
        }),
      ),
    );

  const recordTerminalOffset = (
    outbox: TerminalOutbox,
    offset: string,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET terminal_event_offset = COALESCE(terminal_event_offset, ?)
         WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
           AND terminal_event_key = ?`,
        offset,
        outbox.submissionId,
        outbox.attemptId,
        outbox.eventKey,
      )
      .pipe(storageFailure("record terminal event offset"), Effect.asVoid);

  const readAppendedEvent = Effect.fn("AgentConversationCoordinator.readAppendedEvent")(function* (
    streamPath: string,
    eventKey: string,
  ): Effect.fn.Return<
    { readonly offset: string; readonly event: unknown } | null,
    EventStorageFailed
  > {
    const cursor = yield* sql
      .exec<EventKeyRow>(
        `SELECT seq, data FROM denora_event_stream_keys WHERE path = ? AND key = ? LIMIT 1`,
        streamPath,
        eventKey,
      )
      .pipe(storageFailure("read appended terminal event key"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect appended terminal event key"));
    const row = rows[0];
    if (row === undefined) return null;
    const event = yield* Effect.try({
      try: () => JSON.parse(row.data) as unknown,
      catch: (cause) =>
        new EventStorageFailed({ operation: "parse appended terminal event", cause }),
    });
    return { offset: formatOffset(row.seq), event };
  });

  const updateTerminalEvent = (
    outbox: TerminalOutbox,
    event: unknown,
  ): Effect.Effect<void, EventStreamError> =>
    stringify(event).pipe(
      Effect.flatMap((eventJson) =>
        sql
          .exec(
            `UPDATE denora_agent_conversation_submissions
             SET terminal_event_json = ?
             WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
               AND terminal_event_key = ? AND terminal_event_offset IS NULL`,
            eventJson,
            outbox.submissionId,
            outbox.attemptId,
            outbox.eventKey,
          )
          .pipe(storageFailure("update terminal event"), Effect.asVoid),
      ),
    );

  const nextStreamEventIndex = Effect.fn("AgentConversationCoordinator.nextStreamEventIndex")(
    function* (streamPath: string): Effect.fn.Return<number, EventStreamError> {
      yield* store.createStream(streamPath);
      const meta = yield* store.getStreamMeta(streamPath);
      return (yield* parseOffset(meta?.nextOffset ?? "-1")) + 1;
    },
  );

  const reindexEventForAppend = Effect.fn("AgentConversationCoordinator.reindexEventForAppend")(
    function* (streamPath: string, event: unknown): Effect.fn.Return<unknown, EventStreamError> {
      const eventIndex = yield* nextStreamEventIndex(streamPath);
      return eventWithIndex(event, eventIndex);
    },
  );

  const finalizeTerminal = (outbox: TerminalOutbox): Effect.Effect<void, EventStorageFailed> =>
    Effect.gen(function* () {
      const cursor = yield* sql
        .exec<Row>(
          `UPDATE denora_agent_conversation_submissions
           SET status = 'settled', settled_at = ?
           WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
             AND terminal_event_key = ? AND terminal_event_offset IS NOT NULL
           RETURNING 1 AS value`,
          Date.now(),
          outbox.submissionId,
          outbox.attemptId,
          outbox.eventKey,
        )
        .pipe(storageFailure("finalize terminal submission"));
      const rows = yield* cursor
        .toArray()
        .pipe(storageFailure("collect finalized terminal submission"));
      if (rows[0] !== undefined)
        yield* recordTurnJournalPhase(
          {
            submissionId: outbox.submissionId,
            sessionKey: sessionKey(outbox.conversationId),
            kind: "message",
            attemptId: outbox.attemptId,
            runId: outbox.runId,
          },
          "settled",
        );
    });

  const markStreamChunksConsumed = (
    submissionId: string,
    streamKey: string,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_turn_journals
         SET stream_consumed_at = COALESCE(stream_consumed_at, ?)
         WHERE submission_id = ? AND stream_key = ?`,
        Date.now(),
        submissionId,
        streamKey,
      )
      .pipe(storageFailure("mark stream chunks consumed"), Effect.asVoid);

  const recordTurnJournalPhase = (
    submission: Pick<Submission, "submissionId" | "sessionKey" | "kind" | "attemptId" | "runId">,
    phase: TurnJournalPhase,
    options: {
      readonly checkpointLeafId?: string | undefined;
      readonly committedLeafId?: string | undefined;
      readonly streamKey?: string | undefined;
      readonly toolRequest?: unknown;
    } = {},
  ): Effect.Effect<void, EventStorageFailed> =>
    Effect.gen(function* () {
      const attemptId = submission.attemptId ?? "";
      const phaseOrder = turnJournalPhaseOrder(phase);
      const now = Date.now();
      const toolRequest =
        options.toolRequest === undefined
          ? undefined
          : yield* parseJournaledToolRequest(options.toolRequest, "parse tool request");
      const mergedToolRequest =
        toolRequest === undefined
          ? undefined
          : yield* mergeWithPersistedToolRequest(submission.submissionId, toolRequest);
      const toolRequestJson =
        mergedToolRequest === undefined
          ? null
          : yield* stringify(mergedToolRequest).pipe(
              Effect.mapError(
                (cause) => new EventStorageFailed({ operation: "serialize tool request", cause }),
              ),
            );
      const committed = phase === "committed" ? 1 : 0;
      yield* sql
        .exec(
          `INSERT INTO denora_agent_turn_journals
           (submission_id, session_key, kind, attempt_id, run_id, operation_id, turn_id,
            phase, phase_order, revision, created_at, updated_at, checkpoint_leaf_id,
            tool_request_json, stream_key, stream_consumed_at, committed, committed_leaf_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, NULL, ?, ?)
           ON CONFLICT(submission_id) DO UPDATE SET
             attempt_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.attempt_id
               WHEN denora_agent_turn_journals.phase_order < excluded.phase_order
               THEN excluded.attempt_id
               ELSE denora_agent_turn_journals.attempt_id
             END,
             run_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.run_id
               WHEN denora_agent_turn_journals.phase_order < excluded.phase_order
               THEN excluded.run_id
               ELSE denora_agent_turn_journals.run_id
             END,
             operation_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.operation_id
               WHEN denora_agent_turn_journals.phase_order < excluded.phase_order
               THEN excluded.operation_id
               ELSE COALESCE(denora_agent_turn_journals.operation_id, excluded.operation_id)
             END,
             turn_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.turn_id
               WHEN denora_agent_turn_journals.phase_order < excluded.phase_order
               THEN excluded.turn_id
               ELSE COALESCE(denora_agent_turn_journals.turn_id, excluded.turn_id)
             END,
             phase = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.phase
               WHEN denora_agent_turn_journals.phase_order < excluded.phase_order
               THEN excluded.phase
               ELSE denora_agent_turn_journals.phase
             END,
             phase_order = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.phase_order
               ELSE MAX(denora_agent_turn_journals.phase_order, excluded.phase_order)
             END,
             revision = denora_agent_turn_journals.revision + 1,
             updated_at = excluded.updated_at,
             checkpoint_leaf_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.checkpoint_leaf_id
               ELSE COALESCE(excluded.checkpoint_leaf_id, denora_agent_turn_journals.checkpoint_leaf_id)
             END,
             tool_request_json = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.tool_request_json
               ELSE COALESCE(excluded.tool_request_json, denora_agent_turn_journals.tool_request_json)
             END,
             stream_key = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.stream_key
               ELSE COALESCE(excluded.stream_key, denora_agent_turn_journals.stream_key)
             END,
             stream_consumed_at = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN NULL
               ELSE denora_agent_turn_journals.stream_consumed_at
             END,
             committed = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.committed
               WHEN excluded.committed = 1
               THEN 1
               ELSE denora_agent_turn_journals.committed
             END,
             committed_leaf_id = CASE
               WHEN excluded.phase = 'before_provider'
                AND denora_agent_turn_journals.attempt_id != excluded.attempt_id
               THEN excluded.committed_leaf_id
               ELSE COALESCE(excluded.committed_leaf_id, denora_agent_turn_journals.committed_leaf_id)
             END
           WHERE (excluded.phase = 'before_provider'
                    AND denora_agent_turn_journals.attempt_id != excluded.attempt_id)
              OR denora_agent_turn_journals.phase_order < excluded.phase_order
              OR excluded.checkpoint_leaf_id IS NOT NULL
              OR excluded.tool_request_json IS NOT NULL
              OR excluded.stream_key IS NOT NULL
              OR excluded.committed = 1
              OR excluded.committed_leaf_id IS NOT NULL`,
          submission.submissionId,
          submission.sessionKey,
          submission.kind,
          attemptId,
          submission.runId,
          submission.runId,
          submission.runId,
          phase,
          phaseOrder,
          now,
          now,
          options.checkpointLeafId ?? null,
          toolRequestJson,
          options.streamKey ?? null,
          committed,
          options.committedLeafId ?? null,
        )
        .pipe(storageFailure("record turn journal phase"), Effect.asVoid);
    });

  const startAttemptMarker = (
    submission: Submission,
    snapshot: Record<string, unknown>,
  ): Effect.Effect<void, EventStreamError> =>
    stringify(attemptSnapshot(submission, snapshot)).pipe(
      Effect.flatMap((snapshotJson) => {
        const now = Date.now();
        return sql
          .exec(
            `INSERT INTO denora_agent_attempt_markers
             (attempt_id, submission_id, name, status, snapshot_json, attempt_count, max_attempts, last_error, started_at, updated_at, completed_at)
              VALUES (?, ?, ?, 'running', ?, ?, ?, ?, ?, ?, NULL)
              ON CONFLICT(attempt_id) DO UPDATE SET
                submission_id = excluded.submission_id,
                name = excluded.name,
                status = 'running',
                snapshot_json = excluded.snapshot_json,
                attempt_count = excluded.attempt_count,
                max_attempts = excluded.max_attempts,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at,
                completed_at = NULL`,
            submission.attemptId ?? "",
            submission.submissionId,
            SUBMISSION_ATTEMPT_MARKER_NAME,
            snapshotJson,
            submission.attemptCount,
            submission.maxAttempts,
            submission.lastError ?? null,
            now,
            now,
          )
          .pipe(storageFailure("start agent attempt marker"), Effect.asVoid);
      }),
    );

  const updateAttemptSnapshot = (
    submission: Submission,
    snapshot: Record<string, unknown>,
  ): Effect.Effect<void, EventStreamError> =>
    stringify(attemptSnapshot(submission, snapshot)).pipe(
      Effect.flatMap((snapshotJson) =>
        sql
          .exec(
            `UPDATE denora_agent_attempt_markers
             SET snapshot_json = ?, updated_at = ?
             WHERE attempt_id = ? AND status = 'running'`,
            snapshotJson,
            Date.now(),
            submission.attemptId ?? "",
          )
          .pipe(storageFailure("update agent attempt marker snapshot"), Effect.asVoid),
      ),
    );

  const stashAttemptSnapshot = (
    submission: Submission,
    fiber: DurableFiber.FiberContext,
    snapshot: Record<string, unknown>,
  ): Effect.Effect<void, EventStreamError> =>
    Effect.gen(function* () {
      yield* fiber.stash(attemptSnapshot(submission, snapshot));
      yield* updateAttemptSnapshot(submission, snapshot);
    });

  const settleAttemptMarker = (
    submission: Submission,
    status: TerminalAttemptMarkerStatus,
    snapshot: Record<string, unknown>,
  ): Effect.Effect<void, EventStreamError> =>
    stringify(attemptSnapshot(submission, snapshot)).pipe(
      Effect.flatMap((snapshotJson) =>
        sql
          .exec(
            `UPDATE denora_agent_attempt_markers
             SET status = ?, snapshot_json = ?, last_error = ?, updated_at = ?, completed_at = COALESCE(completed_at, ?)
              WHERE attempt_id = ? AND status = 'running'`,
            status,
            snapshotJson,
            typeof snapshot.error === "string" ? snapshot.error : null,
            Date.now(),
            Date.now(),
            submission.attemptId ?? "",
          )
          .pipe(storageFailure("settle agent attempt marker"), Effect.asVoid),
      ),
    );

  const reconcileUnfinishedAttemptMarkers = Effect.fn(
    "AgentConversationCoordinator.reconcileUnfinishedAttemptMarkers",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const cursor = yield* sql
      .exec<AttemptMarkerRow>(
        `SELECT ${attemptMarkerColumns}
         FROM denora_agent_attempt_markers
         WHERE status = 'running'
         ORDER BY started_at ASC`,
      )
      .pipe(storageFailure("list unfinished agent attempt markers"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect unfinished agent attempt markers"));
    for (const marker of rows.map(parseAttemptMarker)) {
      if (activeAttempts.has(marker.submissionId)) continue;
      const submission = yield* readSubmission(marker.submissionId);
      if (submission === null) {
        yield* markOrphanAttemptMarkerInterrupted(marker);
        continue;
      }
      if (submission.attemptId !== marker.attemptId) {
        yield* markAttemptMarkerInterrupted(marker, {
          phase: "superseded",
          submissionStatus: submission.status,
          currentAttemptId: submission.attemptId ?? null,
        });
        continue;
      }
      switch (submission.status) {
        case "settled":
          yield* settleAttemptMarker(
            submission,
            submission.error === undefined ? "completed" : "failed",
            {
              phase: "settled_recovered",
              submissionStatus: submission.status,
            },
          );
          break;
        case "terminalizing":
          yield* settleAttemptMarker(
            submission,
            submission.error === undefined ? "completed" : "failed",
            {
              phase: "terminalizing_recovered",
              submissionStatus: submission.status,
            },
          );
          break;
        case "running":
          yield* reconcileInterruptedSubmission(submission);
          break;
        case "queued":
          yield* markAttemptMarkerInterrupted(marker, {
            phase: "queued_recovered",
            submissionStatus: submission.status,
          });
          break;
      }
    }
  });

  const recoverInterruptedDurableFibers = Effect.fn(
    "AgentConversationCoordinator.recoverInterruptedDurableFibers",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    yield* durableFibers.recoverInterrupted((fiber) =>
      Effect.gen(function* () {
        const submissionId = durableFiberSubmissionId(fiber);
        if (submissionId === undefined) return;
        const submission = yield* readSubmission(submissionId);
        if (submission === null) return;
        if (submission.status !== "running" || submission.attemptId !== fiber.fiberId) return;
        yield* reconcileInterruptedSubmission(submission);
      }),
    );
  });

  const reconcileInterruptedSubmission = Effect.fn(
    "AgentConversationCoordinator.reconcileInterruptedSubmission",
  )(function* (submission: Submission): Effect.fn.Return<void, EventStreamError> {
    const decision = yield* decideInterruptedRecovery(submission);
    yield* applyInterruptedRecoveryDecision(submission, decision);
  });

  const decideInterruptedRecovery = Effect.fn(
    "AgentConversationCoordinator.decideInterruptedRecovery",
  )(function* (
    submission: Submission,
  ): Effect.fn.Return<InterruptedRecoveryDecision, EventStreamError> {
    if (submission.status === "terminalizing" && submission.attemptId !== undefined) {
      return { _tag: "PublishTerminalOutbox" };
    }

    const progress = yield* sessionStore.inspectSubmissionProgress({
      conversationId: submission.conversationId ?? "unknown",
      runId: submission.runId,
      submissionId: submission.submissionId,
    });
    const journal = yield* readTurnJournal(submission.submissionId);
    if (progress.assistantCompleted !== null) {
      return { _tag: "ReserveCompletedAssistant", completed: progress.assistantCompleted };
    }

    const interruptedToolRequest = yield* interruptedToolRequestFromJournal(journal);
    if (interruptedToolRequest !== null) {
      if (submission.attemptCount >= submission.maxAttempts) {
        return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
      }
      if (isTimedOut(submission)) {
        return { _tag: "FailTimedOut", message: timeoutErrorMessage(submission) };
      }
      return { _tag: "RepairInterruptedToolResults", toolRequest: interruptedToolRequest };
    }

    if (progress.toolResultCompletedWithoutAssistant) {
      if (submission.attemptCount >= submission.maxAttempts) {
        return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
      }
      return { _tag: "ContinueAfterToolResult" };
    }

    if (isTimedOut(submission)) {
      return { _tag: "FailTimedOut", message: timeoutErrorMessage(submission) };
    }

    const recoveredStream = yield* recoverableInterruptedStreamFromJournal(journal);
    if (recoveredStream !== null) {
      if (submission.attemptCount >= submission.maxAttempts) {
        return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
      }
      return { _tag: "RecoverInterruptedAssistantStream", recoveredStream };
    }

    const inputApplied =
      submission.inputAppliedAt !== undefined ||
      progress.inputApplied ||
      (journal?.phaseOrder ?? 0) >= turnJournalPhaseOrder("before_provider");
    const assistantStarted =
      progress.assistantStarted ||
      journal?.phase === "tool_request_recorded" ||
      journal?.phase === "committed";
    if (inputApplied && !assistantStarted && submission.abortRequestedAt === undefined) {
      if (submission.attemptCount >= submission.maxAttempts) {
        return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
      }
      return { _tag: "RetryAppliedInput", message: staleAttemptMessage(submission) };
    }

    if (!inputApplied) {
      if (assistantStarted) {
        return {
          _tag: "FailInterruptedAfterInput",
          message: interruptedAfterInputMessage,
        };
      }
      if (submission.attemptCount >= submission.maxAttempts) {
        return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
      }
      return {
        _tag: "RequeueBeforeInput",
        message: preInputStaleAttemptMessage(submission),
      };
    }

    if (submission.attemptCount >= submission.maxAttempts) {
      return { _tag: "FailRetryExhausted", message: retryExhaustedMessage(submission) };
    }

    return {
      _tag: "FailInterruptedAfterInput",
      message: interruptedAfterInputMessage,
    };
  });

  const applyInterruptedRecoveryDecision = Effect.fn(
    "AgentConversationCoordinator.applyInterruptedRecoveryDecision",
  )(function* (
    submission: Submission,
    decision: InterruptedRecoveryDecision,
  ): Effect.fn.Return<void, EventStreamError> {
    switch (decision._tag) {
      case "PublishTerminalOutbox":
        yield* publishTerminalOutboxes();
        return;
      case "ReserveCompletedAssistant":
        yield* reserveTerminal(
          submission,
          yield* makeCompletedResult(submission, decision.completed.assistantText),
        );
        return;
      case "ContinueAfterToolResult":
        yield* requeueInterruptedSubmission(submission, toolResultContinuationMessage);
        yield* settleAttemptMarker(submission, "interrupted", {
          phase: "requeued_after_tool_result",
          error: toolResultContinuationMessage,
        });
        return;
      case "RepairInterruptedToolResults":
        yield* sessionStore.repairInterruptedToolResults({
          conversationId: submission.conversationId ?? "unknown",
          runId: submission.runId,
          submissionId: submission.submissionId,
          toolRequest: decision.toolRequest,
        });
        yield* requeueInterruptedSubmission(submission, interruptedToolRepairMessage);
        yield* settleAttemptMarker(submission, "interrupted", {
          phase: "requeued_after_interrupted_tool_repair",
          error: interruptedToolRepairMessage,
        });
        return;
      case "RecoverInterruptedAssistantStream":
        yield* sessionStore.recordRecoveredInterruptedStream({
          conversationId: submission.conversationId ?? "unknown",
          runId: submission.runId,
          submissionId: submission.submissionId,
          streamKey: decision.recoveredStream.streamKey,
          recovered: decision.recoveredStream.recovered,
        });
        yield* markStreamChunksConsumed(
          submission.submissionId,
          decision.recoveredStream.streamKey,
        );
        yield* streamChunks.deleteStreamChunkSegments(decision.recoveredStream.streamKey).pipe(
          Effect.mapError(
            (cause) =>
              new EventStorageFailed({
                operation: "delete recovered interrupted stream chunks",
                cause,
              }),
          ),
        );
        yield* requeueInterruptedSubmission(submission, interruptedStreamRecoveryMessage);
        yield* settleAttemptMarker(submission, "interrupted", {
          phase: "requeued_after_interrupted_stream_recovery",
          error: interruptedStreamRecoveryMessage,
        });
        return;
      case "RetryAppliedInput":
        yield* requeueInterruptedSubmission(submission, decision.message);
        yield* settleAttemptMarker(submission, "interrupted", {
          phase: "requeued_after_input_application",
          error: decision.message,
        });
        return;
      case "RequeueBeforeInput":
        yield* requeueInterruptedSubmission(submission, decision.message);
        yield* settleAttemptMarker(submission, "interrupted", {
          phase: "requeued_before_input_application",
          error: decision.message,
        });
        return;
      case "FailTimedOut":
        yield* failInterruptedSubmission(submission, decision.message, "timeout_terminal_reserved");
        return;
      case "FailRetryExhausted":
        yield* failInterruptedSubmission(
          submission,
          decision.message,
          "retry_exhausted_terminal_reserved",
        );
        return;
      case "FailInterruptedAfterInput":
        yield* failInterruptedSubmission(
          submission,
          decision.message,
          "interrupted_terminal_reserved",
          {
            markerStatus: "interrupted",
          },
        );
        return;
    }
  });

  const readTurnJournal = Effect.fn("AgentConversationCoordinator.readTurnJournal")(function* (
    submissionId: string,
  ): Effect.fn.Return<TurnJournal | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<TurnJournalRow>(
        `SELECT submission_id, attempt_id, phase, phase_order, revision, stream_key,
                stream_consumed_at, tool_request_json, committed, committed_leaf_id
         FROM denora_agent_turn_journals
         WHERE submission_id = ?
         LIMIT 1`,
        submissionId,
      )
      .pipe(storageFailure("read agent turn journal"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect agent turn journal"));
    const row = rows[0];
    if (row === undefined) return null;
    return {
      submissionId: row.submission_id,
      attemptId: row.attempt_id,
      phase: row.phase,
      phaseOrder: row.phase_order,
      revision: row.revision,
      streamKey: row.stream_key ?? undefined,
      streamConsumedAt: row.stream_consumed_at ?? undefined,
      toolRequestJson: row.tool_request_json ?? undefined,
      committed: row.committed === 1,
      committedLeafId: row.committed_leaf_id ?? undefined,
    };
  });

  const mergeWithPersistedToolRequest = Effect.fn(
    "AgentConversationCoordinator.mergeWithPersistedToolRequest",
  )(function* (
    submissionId: string,
    toolRequest: AgentConversationSessionStore.JournaledToolRequest,
  ): Effect.fn.Return<AgentConversationSessionStore.JournaledToolRequest, EventStorageFailed> {
    const existing = yield* readTurnJournal(submissionId);
    if (existing?.toolRequestJson === undefined) return toolRequest;
    const persisted = yield* parseJournaledToolRequestJson(
      existing.toolRequestJson,
      "parse persisted tool request",
    );
    return mergeJournaledToolRequests(persisted, toolRequest);
  });

  const interruptedToolRequestFromJournal = Effect.fn(
    "AgentConversationCoordinator.interruptedToolRequestFromJournal",
  )(function* (
    journal: TurnJournal | null,
  ): Effect.fn.Return<
    AgentConversationSessionStore.JournaledToolRequest | null,
    EventStorageFailed
  > {
    if (
      journal?.phase !== "tool_request_recorded" ||
      journal.committed ||
      journal.toolRequestJson === undefined
    ) {
      return null;
    }
    return yield* parseJournaledToolRequestJson(
      journal.toolRequestJson,
      "parse interrupted tool request",
    );
  });

  const recoverableInterruptedStreamFromJournal = Effect.fn(
    "AgentConversationCoordinator.recoverableInterruptedStreamFromJournal",
  )(function* (
    journal: TurnJournal | null,
  ): Effect.fn.Return<RecoverableInterruptedStream | null, EventStreamError> {
    if (
      journal?.phase !== "provider_started" ||
      journal.committed ||
      journal.streamKey === undefined ||
      journal.streamConsumedAt !== undefined
    ) {
      return null;
    }

    const segments = yield* streamChunks.readStreamChunkSegments(journal.streamKey).pipe(
      Effect.mapError(
        (cause) =>
          new EventStorageFailed({
            operation: "read interrupted stream chunks",
            cause,
          }),
      ),
    );
    const recovered = StreamChunks.reconstructInterruptedStream(segments, journal.streamKey);
    if (recovered === null) return null;
    return { streamKey: journal.streamKey, recovered };
  });

  const markAttemptMarkerInterrupted = (
    marker: AttemptMarker,
    snapshot: Record<string, unknown>,
  ): Effect.Effect<void, EventStreamError> =>
    stringify({ ...marker.snapshot, ...snapshot }).pipe(
      Effect.flatMap((snapshotJson) =>
        sql
          .exec(
            `UPDATE denora_agent_attempt_markers
             SET status = 'interrupted', snapshot_json = ?, last_error = ?, updated_at = ?, completed_at = COALESCE(completed_at, ?)
              WHERE attempt_id = ? AND status = 'running'`,
            snapshotJson,
            typeof snapshot.error === "string" ? snapshot.error : null,
            Date.now(),
            Date.now(),
            marker.attemptId,
          )
          .pipe(storageFailure("interrupt agent attempt marker"), Effect.asVoid),
      ),
    );

  const markOrphanAttemptMarkerInterrupted = (
    marker: AttemptMarker,
  ): Effect.Effect<void, EventStreamError> =>
    markAttemptMarkerInterrupted(marker, { phase: "orphaned", submissionStatus: null });

  const prepareSubmissionForExecution = Effect.fn(
    "AgentConversationCoordinator.prepareSubmissionForExecution",
  )(function* (
    submission: Submission,
  ): Effect.fn.Return<AgentConversationSessionStore.RecordedSubmissionStarted, EventStreamError> {
    const conversationId = submission.conversationId;
    const messageId = submission.messageId;
    if (conversationId === undefined || messageId === undefined) {
      return yield* new EventStorageFailed({
        operation: "prepare conversation submission",
        cause: new Error("Conversation submission is missing its conversation or message id."),
      });
    }
    yield* rejectInactiveSubmission(submission, "prepare conversation submission");
    const payload = yield* Effect.try({
      try: () => submissionPayload(submission.input),
      catch: (cause) => new EventStorageFailed({ operation: "read submission payload", cause }),
    });
    return yield* sessionStore
      .recordSubmissionStarted({
        conversationId,
        userId: payload.userId,
        agentName: submission.agentName,
        messageId,
        submissionId: submission.submissionId,
        runId: submission.runId,
        parentMessageId: submission.parentMessageId,
        content: payload.submittedMessage,
      })
      .pipe(
        Effect.mapError(
          (cause) => new EventStorageFailed({ operation: "record conversation submission", cause }),
        ),
      );
  });

  const appendAppliedUserMessage = Effect.fn(
    "AgentConversationCoordinator.appendAppliedUserMessage",
  )(function* (
    submission: Submission,
    prepared: Pick<
      AgentConversationSessionStore.RecordedSubmissionStarted,
      "userMessage" | "userTurnId"
    >,
  ): Effect.fn.Return<void, EventStreamError> {
    const conversationId = submission.conversationId;
    if (conversationId === undefined) {
      return yield* new EventStorageFailed({
        operation: "append applied conversation user message",
        cause: new Error("Conversation submission is missing its conversation id."),
      });
    }
    yield* AgentRunLifecycle.appendConversationUserMessageApplied(store, {
      agentName: submission.agentName,
      conversationId,
      submissionId: submission.submissionId,
      userTurnId: prepared.userTurnId,
      message: prepared.userMessage,
    });
  });

  const rejectInactiveConversation = Effect.fn(
    "AgentConversationCoordinator.rejectInactiveConversation",
  )(function* (
    conversationId: string,
    operation: string,
  ): Effect.fn.Return<void, EventStorageFailed> {
    const status = yield* readConversationLifecycleStatus(conversationId);
    if (status === "active") return;
    return yield* new EventStorageFailed({
      operation,
      cause: new Error(inactiveConversationMessage(conversationId, status)),
    });
  });

  const rejectInactiveSubmission = Effect.fn(
    "AgentConversationCoordinator.rejectInactiveSubmission",
  )(function* (
    submission: Submission,
    operation: string,
  ): Effect.fn.Return<void, EventStorageFailed> {
    const status = yield* readSessionLifecycleStatus(submission.sessionKey);
    if (status === "active") return;
    const message = inactiveConversationMessage(submission.conversationId ?? "unknown", status);
    const active = activeAttempts.get(submission.submissionId);
    if (active !== undefined) {
      yield* markAbortRequested(submission);
      active.abort(new Error(message));
    }
    return yield* new EventStorageFailed({ operation, cause: new Error(message) });
  });

  const settleIfInactiveSubmission = Effect.fn(
    "AgentConversationCoordinator.settleIfInactiveSubmission",
  )(function* (submission: Submission): Effect.fn.Return<boolean, EventStreamError> {
    const status = yield* readSessionLifecycleStatus(submission.sessionKey);
    if (status === "active") return false;
    const message = inactiveConversationMessage(submission.conversationId ?? "unknown", status);
    yield* reserveTerminal(submission, yield* makeInterruptedResult(submission, message), {
      attemptId: submission.attemptId ?? `lifecycle:${crypto.randomUUID()}`,
      fromStatuses: ["queued", "running"],
      markerStatus: "interrupted",
    });
    return true;
  });

  const readConversationLifecycleStatus = (
    conversationId: string,
  ): Effect.Effect<ConversationLifecycleState, EventStorageFailed> =>
    readSessionLifecycleStatus(sessionKey(conversationId));

  const readSessionLifecycleStatus = Effect.fn(
    "AgentConversationCoordinator.readSessionLifecycleStatus",
  )(function* (key: string): Effect.fn.Return<ConversationLifecycleState, EventStorageFailed> {
    const cursor = yield* sql
      .exec<LifecycleRow>(
        `SELECT status
          FROM denora_agent_conversation_session_lifecycles
         WHERE session_key = ?
         LIMIT 1`,
        key,
      )
      .pipe(storageFailure("read conversation lifecycle"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect conversation lifecycle"));
    return rows[0]?.status ?? "active";
  });

  const reconstructCompletedRunResult = Effect.fn(
    "AgentConversationCoordinator.reconstructCompletedRunResult",
  )(function* (
    submission: Submission,
  ): Effect.fn.Return<ExecuteRunAttemptResult | null, EventStreamError> {
    const completed = yield* sessionStore.reconstructCompletedRun({
      conversationId: submission.conversationId ?? "unknown",
      runId: submission.runId,
    });
    if (completed === null) return null;
    return yield* makeCompletedResult(submission, completed.assistantText);
  });

  return {
    abortConversation,
    admitSubmission,
    setConversationLifecycle,
    getSubmissionTerminal,
    reconcile,
  } satisfies Interface;
});

const ensureTables = (sql: Cloudflare.SqlStorage): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    for (const [operation, statement] of [
      ["create agent turn journals table", CREATE_TURN_JOURNALS_TABLE],
      ["create agent conversation submissions table", CREATE_SUBMISSIONS_TABLE],
      ["create agent conversation submissions status index", CREATE_SUBMISSIONS_STATUS_INDEX],
      ["create agent conversation session lifecycles table", CREATE_SESSION_LIFECYCLES_TABLE],
      ["create agent attempt markers table", CREATE_ATTEMPT_MARKERS_TABLE],
      ["create agent attempt markers unfinished index", CREATE_ATTEMPT_MARKERS_UNFINISHED_INDEX],
    ] as const) {
      yield* sql.exec(statement).pipe(storageFailure(operation), Effect.asVoid);
    }
    yield* ensureColumn(sql, {
      table: "denora_agent_conversation_submissions",
      column: "input_applied_at",
      definition: "input_applied_at INTEGER",
    });
    for (const [column, definition] of [
      ["attempt_count", "attempt_count INTEGER NOT NULL DEFAULT 0"],
      ["max_attempts", `max_attempts INTEGER NOT NULL DEFAULT ${DEFAULT_MAX_ATTEMPTS}`],
      ["last_error", "last_error TEXT"],
      ["timeout_at", "timeout_at INTEGER NOT NULL DEFAULT 0"],
      ["lease_expires_at", "lease_expires_at INTEGER NOT NULL DEFAULT 0"],
      ["parent_message_id", "parent_message_id TEXT"],
    ] as const) {
      yield* ensureColumn(sql, {
        table: "denora_agent_conversation_submissions",
        column,
        definition,
      });
    }
    for (const [column, definition] of [
      ["attempt_count", "attempt_count INTEGER NOT NULL DEFAULT 0"],
      ["max_attempts", `max_attempts INTEGER NOT NULL DEFAULT ${DEFAULT_MAX_ATTEMPTS}`],
      ["last_error", "last_error TEXT"],
    ] as const) {
      yield* ensureColumn(sql, {
        table: "denora_agent_attempt_markers",
        column,
        definition,
      });
    }
    for (const [column, definition] of [
      ["operation_id", "operation_id TEXT"],
      ["turn_id", "turn_id TEXT"],
      ["checkpoint_leaf_id", "checkpoint_leaf_id TEXT"],
      ["tool_request_json", "tool_request_json TEXT"],
      ["stream_key", "stream_key TEXT"],
      ["stream_consumed_at", "stream_consumed_at INTEGER"],
      ["committed", "committed INTEGER NOT NULL DEFAULT 0"],
      ["committed_leaf_id", "committed_leaf_id TEXT"],
    ] as const) {
      yield* ensureColumn(sql, {
        table: "denora_agent_turn_journals",
        column,
        definition,
      });
    }
  });

const ensureColumn = (
  sql: Cloudflare.SqlStorage,
  input: { readonly table: string; readonly column: string; readonly definition: string },
): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    const cursor = yield* sql
      .exec<TableInfoRow>(`PRAGMA table_info(${input.table})`)
      .pipe(storageFailure(`inspect ${input.table} columns`));
    const rows = yield* cursor.toArray().pipe(storageFailure(`collect ${input.table} columns`));
    if (rows.some((row) => row.name === input.column)) return;
    yield* sql
      .exec(`ALTER TABLE ${input.table} ADD COLUMN ${input.definition}`)
      .pipe(storageFailure(`add ${input.table}.${input.column} column`), Effect.asVoid);
  });

const storageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) => new EventStorageFailed({ operation, cause })),
      Effect.provide(RuntimeContext.phantom),
    );

const stringify = (value: unknown): Effect.Effect<string, EventSerializationFailed> =>
  Effect.try({
    try: () => JSON.stringify(value),
    catch: (cause) => new EventSerializationFailed({ cause }),
  }).pipe(
    Effect.flatMap((data) =>
      data === undefined
        ? Effect.fail(
            new EventSerializationFailed({
              cause: new TypeError("Value is not JSON serializable"),
            }),
          )
        : Effect.succeed(data),
    ),
  );

const validateSubmissionPayload = (input: unknown): Effect.Effect<void, EventStorageFailed> =>
  Effect.try({
    try: () => assertAgentConversationContentWithinLimits(input),
    catch: (cause) =>
      new EventStorageFailed({ operation: "validate agent conversation submission", cause }),
  });

const validateSerializedSubmissionPayload = (
  payload: string,
): Effect.Effect<void, EventStorageFailed> =>
  Effect.try({
    try: () =>
      assertAgentConversationJsonWithinLimits(payload, "Agent conversation submission payload"),
    catch: (cause) =>
      new EventStorageFailed({ operation: "validate agent conversation submission", cause }),
  });

const makeInterruptedResult = Effect.fn("AgentConversationCoordinator.makeInterruptedResult")(
  function* (submission: Submission, message: string): Effect.fn.Return<ExecuteRunAttemptResult> {
    const timestamp = yield* Effect.sync(() => new Date().toISOString());
    const terminalEvent = {
      v: 3,
      type: "submission_settled",
      instanceId: submission.conversationId ?? "unknown",
      conversationId: submission.conversationId,
      agentName: submission.agentName,
      submissionId: submission.submissionId,
      timestamp,
      outcome: "failed",
      result: null,
      error: { message },
    };
    return { terminalEvent, durationMs: 0, isError: true, result: null, error: { message } };
  },
);

const makeCompletedResult = Effect.fn("AgentConversationCoordinator.makeCompletedResult")(
  function* (
    submission: Submission,
    assistantText: string,
  ): Effect.fn.Return<ExecuteRunAttemptResult> {
    const timestamp = yield* Effect.sync(() => new Date().toISOString());
    const result = { assistantText };
    const terminalEvent = {
      v: 3,
      type: "submission_settled",
      instanceId: submission.conversationId ?? "unknown",
      conversationId: submission.conversationId,
      agentName: submission.agentName,
      submissionId: submission.submissionId,
      timestamp,
      outcome: "completed",
      result,
    };
    return { terminalEvent, durationMs: 0, isError: false, result };
  },
);

const errorMessage = (error: EventStreamError): string =>
  error._tag === "EventStorageFailed"
    ? `Agent run storage failed during ${error.operation}.`
    : "Agent run execution failed.";

const staleAttemptMessage = (submission: Submission): string =>
  `Agent run attempt ${submission.attemptCount} was interrupted before completion and will be retried.`;

const preInputStaleAttemptMessage = (submission: Submission): string =>
  `Agent run attempt ${submission.attemptCount} was interrupted before user input was applied and will be retried.`;

const retryExhaustedMessage = (submission: Submission): string =>
  `Agent run exceeded its retry budget after ${submission.attemptCount} of ${submission.maxAttempts} attempts.`;

const timeoutErrorMessage = (submission: Submission): string =>
  `Agent run timed out after ${submission.attemptCount} of ${submission.maxAttempts} attempts.`;

const toolResultContinuationMessage =
  "Agent run recovered completed tool results and will continue without re-executing tools.";

const interruptedToolRepairMessage =
  "Agent run repaired interrupted tool results and will continue without re-executing tools.";

const interruptedStreamRecoveryMessage =
  "Agent run recovered an interrupted assistant stream and will continue from that partial response.";

const interruptedAfterInputMessage =
  "Agent run was interrupted after partial assistant progress and cannot be safely resumed yet. Please retry.";

const isTimedOut = (submission: Submission): boolean =>
  submission.timeoutAt > 0 && Date.now() >= submission.timeoutAt;

const assistantPlainText = (parts: ReadonlyArray<unknown>): string =>
  parts
    .flatMap((part) =>
      typeof part === "object" &&
      part !== null &&
      (part as { readonly type?: unknown }).type === "text" &&
      typeof (part as { readonly text?: unknown }).text === "string"
        ? [(part as { readonly text: string }).text]
        : [],
    )
    .join("");

const assistantMessageHasToolCall = (message: { readonly content: ReadonlyArray<unknown> }) =>
  message.content.some(
    (part) =>
      typeof part === "object" &&
      part !== null &&
      (part as { readonly type?: unknown }).type === "toolCall",
  );

const assistantMessageCheckpointLeafId = (checkpoint: {
  readonly runId: string;
  readonly messageIndex: number;
}): string => `assistant:${checkpoint.runId}:${checkpoint.messageIndex}`;

const toolRequestFromCheckpoint = (checkpoint: {
  readonly toolCallId: string;
  readonly toolName: string;
  readonly args: unknown;
}): unknown => ({
  toolCalls: [{ type: "toolCall", id: checkpoint.toolCallId, name: checkpoint.toolName }],
  argumentsByToolCallId: { [checkpoint.toolCallId]: checkpoint.args },
});

const toolRequestFromAssistantMessage = (message: {
  readonly content: ReadonlyArray<unknown>;
}): unknown => {
  const toolCalls: Array<{
    readonly type: "toolCall";
    readonly id: string;
    readonly name: string;
  }> = [];
  const argumentsByToolCallId: Record<string, unknown> = {};
  for (const part of message.content) {
    if (typeof part !== "object" || part === null) continue;
    const toolCall = part as {
      readonly type?: unknown;
      readonly id?: unknown;
      readonly name?: unknown;
      readonly arguments?: unknown;
    };
    if (
      toolCall.type !== "toolCall" ||
      typeof toolCall.id !== "string" ||
      typeof toolCall.name !== "string"
    ) {
      continue;
    }
    toolCalls.push({ type: "toolCall", id: toolCall.id, name: toolCall.name });
    if (toolCall.arguments !== undefined) argumentsByToolCallId[toolCall.id] = toolCall.arguments;
  }
  return {
    toolCalls,
    ...(Object.keys(argumentsByToolCallId).length === 0 ? {} : { argumentsByToolCallId }),
  };
};

const parseJournaledToolRequestJson = (
  value: string,
  operation: string,
): Effect.Effect<AgentConversationSessionStore.JournaledToolRequest, EventStorageFailed> =>
  Effect.try({
    try: () => JSON.parse(value) as unknown,
    catch: (cause) => new EventStorageFailed({ operation, cause }),
  }).pipe(Effect.flatMap((parsed) => parseJournaledToolRequest(parsed, operation)));

const parseJournaledToolRequest = (
  value: unknown,
  operation: string,
): Effect.Effect<AgentConversationSessionStore.JournaledToolRequest, EventStorageFailed> =>
  Effect.try({
    try: () => {
      if (typeof value !== "object" || value === null || Array.isArray(value)) {
        throw new Error("Tool request must be an object.");
      }
      const record = value as {
        readonly toolCalls?: unknown;
        readonly argumentsByToolCallId?: unknown;
      };
      if (!Array.isArray(record.toolCalls)) throw new Error("Tool request is missing toolCalls.");
      const toolCalls = record.toolCalls.map((toolCall) => {
        if (typeof toolCall !== "object" || toolCall === null || Array.isArray(toolCall)) {
          throw new Error("Tool call must be an object.");
        }
        const candidate = toolCall as {
          readonly type?: unknown;
          readonly id?: unknown;
          readonly name?: unknown;
        };
        if (
          candidate.type !== "toolCall" ||
          typeof candidate.id !== "string" ||
          typeof candidate.name !== "string"
        ) {
          throw new Error("Tool call is malformed.");
        }
        return { type: "toolCall" as const, id: candidate.id, name: candidate.name };
      });
      const argumentsByToolCallId =
        typeof record.argumentsByToolCallId === "object" &&
        record.argumentsByToolCallId !== null &&
        !Array.isArray(record.argumentsByToolCallId)
          ? { ...(record.argumentsByToolCallId as Record<string, unknown>) }
          : undefined;
      return {
        toolCalls,
        ...(argumentsByToolCallId === undefined ? {} : { argumentsByToolCallId }),
      };
    },
    catch: (cause) => new EventStorageFailed({ operation, cause }),
  });

const mergeJournaledToolRequests = (
  existing: AgentConversationSessionStore.JournaledToolRequest,
  next: AgentConversationSessionStore.JournaledToolRequest,
): AgentConversationSessionStore.JournaledToolRequest => {
  const seen = new Set(existing.toolCalls.map((toolCall) => toolCall.id));
  const toolCalls = existing.toolCalls.slice();
  for (const toolCall of next.toolCalls) {
    if (seen.has(toolCall.id)) continue;
    seen.add(toolCall.id);
    toolCalls.push(toolCall);
  }
  const argumentsByToolCallId: Record<string, unknown> = {};
  if (existing.argumentsByToolCallId !== undefined)
    Object.assign(argumentsByToolCallId, existing.argumentsByToolCallId);
  if (next.argumentsByToolCallId !== undefined)
    Object.assign(argumentsByToolCallId, next.argumentsByToolCallId);
  return {
    toolCalls,
    ...(Object.keys(argumentsByToolCallId).length === 0 ? {} : { argumentsByToolCallId }),
  };
};

const terminalEventKey = (submissionId: string): string =>
  `agent-conversation:${submissionId}:terminal`;

const submissionAttemptIdempotencyKey = (
  submission: Pick<Submission, "submissionId" | "attemptId">,
): string => `agent-conversation:${submission.submissionId}:${submission.attemptId ?? "unknown"}`;

const streamChunkKey = (submission: Pick<Submission, "submissionId" | "attemptId">): string =>
  `agent-conversation:${submission.submissionId}:${submission.attemptId ?? "unknown"}:stream`;

const durableFiberSubmissionId = (fiber: DurableFiber.RecoveryContext): string | undefined => {
  const metadataId = fiber.metadata.submissionId;
  if (typeof metadataId === "string") return metadataId;
  const snapshot = fiber.snapshot;
  if (typeof snapshot === "object" && snapshot !== null) {
    const snapshotId = (snapshot as { readonly submissionId?: unknown }).submissionId;
    if (typeof snapshotId === "string") return snapshotId;
  }
  return undefined;
};

const sessionKey = (conversationId: string | undefined): string =>
  `agent-session:${conversationId ?? "unknown"}:default`;

const inactiveConversationMessage = (
  conversationId: string,
  status: Exclude<ConversationLifecycleState, "active">,
): string => `Conversation ${conversationId} is ${status}; agent submissions are not accepted.`;

const submissionColumnsFor = (table: string): string =>
  submissionColumns
    .split(", ")
    .map((column) => `${table}.${column}`)
    .join(", ");

const submissionPayload = (
  value: unknown,
): { readonly userId: string; readonly submittedMessage: unknown } => {
  if (typeof value === "object" && value !== null) {
    const record = value as Record<string, unknown>;
    if (typeof record.userId === "string" && record.submittedMessage !== undefined) {
      return { userId: record.userId, submittedMessage: record.submittedMessage };
    }
  }
  throw new Error("Conversation submission payload is missing userId or submittedMessage.");
};

const submissionColumns =
  "sequence, submission_id, session_key, kind, run_id, agent_name, conversation_id, message_id, parent_message_id, payload, status, accepted_at, attempt_id, started_at, settled_at, abort_requested_at, input_applied_at, attempt_count, max_attempts, last_error, timeout_at, lease_expires_at, error, terminal_event_key, terminal_event_json, terminal_event_offset";

const parseSubmission = (row: SubmissionRow): Submission => ({
  sequence: row.sequence,
  submissionId: row.submission_id,
  sessionKey: row.session_key,
  kind: row.kind,
  runId: row.run_id,
  agentName: row.agent_name,
  conversationId: row.conversation_id ?? undefined,
  messageId: row.message_id ?? undefined,
  parentMessageId: row.parent_message_id ?? undefined,
  payload: row.payload,
  input: JSON.parse(row.payload) as unknown,
  status: row.status,
  acceptedAt: row.accepted_at,
  attemptId: row.attempt_id ?? undefined,
  startedAt: row.started_at ?? undefined,
  settledAt: row.settled_at ?? undefined,
  abortRequestedAt: row.abort_requested_at ?? undefined,
  inputAppliedAt: row.input_applied_at ?? undefined,
  attemptCount: row.attempt_count,
  maxAttempts: row.max_attempts,
  lastError: row.last_error ?? undefined,
  timeoutAt: row.timeout_at,
  leaseExpiresAt: row.lease_expires_at,
  error: row.error ?? undefined,
});

const attemptMarkerColumns =
  "attempt_id, submission_id, name, status, snapshot_json, attempt_count, max_attempts, last_error, started_at, updated_at, completed_at";

const parseAttemptMarker = (row: AttemptMarkerRow): AttemptMarker => ({
  attemptId: row.attempt_id,
  submissionId: row.submission_id,
  name: row.name,
  status: row.status,
  snapshot: parseAttemptMarkerSnapshot(row.snapshot_json),
  attemptCount: row.attempt_count,
  maxAttempts: row.max_attempts,
  lastError: row.last_error ?? undefined,
  startedAt: row.started_at,
  updatedAt: row.updated_at,
  completedAt: row.completed_at ?? undefined,
});

const parseAttemptMarkerSnapshot = (value: string | null): Record<string, unknown> => {
  if (value === null) return {};
  try {
    const parsed = JSON.parse(value) as unknown;
    return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
};

const attemptSnapshot = (
  submission: Pick<
    Submission,
    | "submissionId"
    | "attemptId"
    | "runId"
    | "agentName"
    | "conversationId"
    | "attemptCount"
    | "maxAttempts"
    | "lastError"
  >,
  snapshot: Record<string, unknown>,
): Record<string, unknown> => ({
  submissionId: submission.submissionId,
  attemptId: submission.attemptId ?? null,
  runId: submission.runId,
  agentName: submission.agentName,
  conversationId: submission.conversationId ?? null,
  attemptCount: submission.attemptCount,
  maxAttempts: submission.maxAttempts,
  lastError: submission.lastError ?? null,
  ...snapshot,
});

const attemptMarkerTerminalStatus = (
  execution: ExecuteRunAttemptResult,
): TerminalAttemptMarkerStatus => (execution.isError ? "failed" : "completed");

const idleEventKey = (submissionId: string): string => `agent-conversation:${submissionId}:idle`;

const idleEvent = (outbox: TerminalOutbox, eventIndex = idleEventIndex(outbox.event)): unknown => ({
  v: 3,
  type: "idle",
  instanceId: outbox.conversationId,
  conversationId: outbox.conversationId,
  agentName: outbox.agentName,
  submissionId: outbox.submissionId,
  eventIndex,
  timestamp: new Date().toISOString(),
});

const idleEventIndex = (event: unknown): number =>
  hasEventIndex(event) ? (event as { readonly eventIndex: number }).eventIndex + 1 : 0;

const hasEventIndex = (event: unknown): event is { readonly eventIndex: number } =>
  typeof event === "object" &&
  event !== null &&
  typeof (event as { readonly eventIndex?: unknown }).eventIndex === "number";

const eventWithIndex = (event: unknown, eventIndex: number): unknown =>
  typeof event === "object" && event !== null ? { ...event, eventIndex } : event;

const parseTerminalOutbox = (
  row: TerminalOutboxRow,
): Effect.Effect<TerminalOutbox, EventStorageFailed> =>
  Effect.try({
    try: () => ({
      submissionId: row.submission_id,
      runId: row.run_id,
      agentName: row.agent_name,
      conversationId: row.conversation_id,
      attemptId: row.attempt_id,
      eventKey: row.terminal_event_key,
      event: JSON.parse(row.terminal_event_json) as unknown,
      offset: row.terminal_event_offset ?? undefined,
    }),
    catch: (cause) => new EventStorageFailed({ operation: "parse terminal outbox", cause }),
  });

interface Row extends Record<string, Cloudflare.SqlStorageValue> {
  readonly value?: number;
}

interface TableInfoRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly name: string;
}

interface SubmissionRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly sequence: number;
  readonly submission_id: string;
  readonly session_key: string;
  readonly kind: SubmissionKind;
  readonly run_id: string;
  readonly agent_name: string;
  readonly conversation_id: string | null;
  readonly message_id: string | null;
  readonly parent_message_id: string | null;
  readonly payload: string;
  readonly status: SubmissionStatus;
  readonly accepted_at: number;
  readonly attempt_id: string | null;
  readonly started_at: number | null;
  readonly settled_at: number | null;
  readonly abort_requested_at: number | null;
  readonly input_applied_at: number | null;
  readonly attempt_count: number;
  readonly max_attempts: number;
  readonly last_error: string | null;
  readonly timeout_at: number;
  readonly lease_expires_at: number;
  readonly error: string | null;
  readonly terminal_event_key: string | null;
  readonly terminal_event_json: string | null;
  readonly terminal_event_offset: string | null;
}

interface SubmissionWithLifecycleRow extends SubmissionRow {
  readonly lifecycle_status: Exclude<ConversationLifecycleState, "active">;
}

interface LifecycleRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly status: ConversationLifecycleState;
}

interface TerminalOutboxRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly submission_id: string;
  readonly run_id: string;
  readonly agent_name: string;
  readonly conversation_id: string;
  readonly attempt_id: string;
  readonly terminal_event_key: string;
  readonly terminal_event_json: string;
  readonly terminal_event_offset: string | null;
}

interface TerminalEventRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly terminal_event_json: string;
}

interface EventKeyRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly seq: number;
  readonly data: string;
}

interface AttemptMarkerRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly attempt_id: string;
  readonly submission_id: string;
  readonly name: string;
  readonly status: AttemptMarkerStatus;
  readonly snapshot_json: string | null;
  readonly attempt_count: number;
  readonly max_attempts: number;
  readonly last_error: string | null;
  readonly started_at: number;
  readonly updated_at: number;
  readonly completed_at: number | null;
}

interface TurnJournalRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly submission_id: string;
  readonly attempt_id: string;
  readonly phase: TurnJournalPhase;
  readonly phase_order: number;
  readonly revision: number;
  readonly stream_key: string | null;
  readonly stream_consumed_at: number | null;
  readonly tool_request_json: string | null;
  readonly committed: number;
  readonly committed_leaf_id: string | null;
}

type SubmissionStatus = "queued" | "running" | "terminalizing" | "settled";
type SubmissionKind = "message" | "dispatch";
type AttemptMarkerStatus = "running" | TerminalAttemptMarkerStatus;
type TerminalAttemptMarkerStatus = "completed" | "failed" | "interrupted";
type TurnJournalPhase =
  | "before_provider"
  | "provider_started"
  | "tool_request_recorded"
  | "committed"
  | "terminal_reserved"
  | "settled";

const turnJournalPhaseOrder = (phase: TurnJournalPhase): number => {
  switch (phase) {
    case "before_provider":
      return 1;
    case "provider_started":
      return 2;
    case "tool_request_recorded":
      return 3;
    case "committed":
      return 4;
    case "terminal_reserved":
      return 5;
    case "settled":
      return 6;
  }
};

interface Submission {
  readonly sequence: number;
  readonly submissionId: string;
  readonly sessionKey: string;
  readonly kind: SubmissionKind;
  readonly runId: string;
  readonly agentName: string;
  readonly conversationId?: string | undefined;
  readonly messageId?: string | undefined;
  readonly parentMessageId?: string | undefined;
  readonly payload: string;
  readonly input: unknown;
  readonly status: SubmissionStatus;
  readonly acceptedAt: number;
  readonly attemptId?: string | undefined;
  readonly startedAt?: number | undefined;
  readonly settledAt?: number | undefined;
  readonly abortRequestedAt?: number | undefined;
  readonly inputAppliedAt?: number | undefined;
  readonly attemptCount: number;
  readonly maxAttempts: number;
  readonly lastError?: string | undefined;
  readonly timeoutAt: number;
  readonly leaseExpiresAt: number;
  readonly error?: string | undefined;
}

interface TerminalOutbox {
  readonly submissionId: string;
  readonly runId: string;
  readonly agentName: string;
  readonly conversationId: string;
  readonly attemptId: string;
  readonly eventKey: string;
  readonly event: unknown;
  readonly offset?: string | undefined;
}

interface AttemptMarker {
  readonly attemptId: string;
  readonly submissionId: string;
  readonly name: string;
  readonly status: AttemptMarkerStatus;
  readonly snapshot: Record<string, unknown>;
  readonly attemptCount: number;
  readonly maxAttempts: number;
  readonly lastError?: string | undefined;
  readonly startedAt: number;
  readonly updatedAt: number;
  readonly completedAt?: number | undefined;
}

interface TurnJournal {
  readonly submissionId: string;
  readonly attemptId: string;
  readonly phase: TurnJournalPhase;
  readonly phaseOrder: number;
  readonly revision: number;
  readonly streamKey?: string | undefined;
  readonly streamConsumedAt?: number | undefined;
  readonly toolRequestJson?: string | undefined;
  readonly committed: boolean;
  readonly committedLeafId?: string | undefined;
}

interface RecoverableInterruptedStream {
  readonly streamKey: string;
  readonly recovered: StreamChunks.ReconstructedInterruptedStream;
}

type InterruptedRecoveryDecision =
  | { readonly _tag: "PublishTerminalOutbox" }
  | {
      readonly _tag: "ReserveCompletedAssistant";
      readonly completed: AgentConversationSessionStore.CompletedAssistantRun;
    }
  | { readonly _tag: "ContinueAfterToolResult" }
  | {
      readonly _tag: "RepairInterruptedToolResults";
      readonly toolRequest: AgentConversationSessionStore.JournaledToolRequest;
    }
  | {
      readonly _tag: "RecoverInterruptedAssistantStream";
      readonly recoveredStream: RecoverableInterruptedStream;
    }
  | { readonly _tag: "RetryAppliedInput"; readonly message: string }
  | { readonly _tag: "RequeueBeforeInput"; readonly message: string }
  | { readonly _tag: "FailTimedOut"; readonly message: string }
  | { readonly _tag: "FailRetryExhausted"; readonly message: string }
  | { readonly _tag: "FailInterruptedAfterInput"; readonly message: string };

export * as AgentConversationCoordinator from "./AgentConversationCoordinator.ts";
