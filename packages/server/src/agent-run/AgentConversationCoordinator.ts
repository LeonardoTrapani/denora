import type * as Cloudflare from "alchemy/Cloudflare";
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
import { SqlStorage } from "./SqlStorage.ts";

const WAKE_DELAY_MS = 30_000;
const RUNNING_STALE_MS = 15 * 60 * 1000;

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
  payload               TEXT NOT NULL,
  status                TEXT NOT NULL,
  accepted_at           INTEGER NOT NULL,
  attempt_id            TEXT,
  started_at            INTEGER,
  settled_at            INTEGER,
  abort_requested_at    INTEGER,
  input_applied_at      INTEGER,
  error                 TEXT,
  terminal_event_key    TEXT,
  terminal_event_json   TEXT,
  terminal_event_offset TEXT
)`;

const CREATE_SUBMISSIONS_STATUS_INDEX = `
CREATE INDEX IF NOT EXISTS denora_agent_conversation_submissions_status_sequence_idx
ON denora_agent_conversation_submissions (status, sequence ASC)`;

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
  SqlStorage.Service | EventStreamStoreService | AgentConversationSessionStore.Service
> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const sql = yield* SqlStorage.Service;
    const store = yield* EventStreamStoreService;
    const sessions = yield* AgentConversationSessionStore.Service;
    const coordinator = yield* makeSqliteAgentConversationCoordinator(sql, store, sessions);
    return Service.of(coordinator);
  }),
);

export const makeSqliteAgentConversationCoordinator = Effect.fn(
  "AgentConversationCoordinator.makeSqliteAgentConversationCoordinator",
)(function* (
  sql: Cloudflare.SqlStorage,
  store: EventStreamStore,
  sessionStore: AgentConversationSessionStore.Interface,
): Effect.fn.Return<Interface, EventStorageFailed> {
  yield* ensureTables(sql);
  const activeAttempts = new Map<string, AbortController>();

  const admitSubmission = Effect.fn("AgentConversationCoordinator.admitSubmission")(function* (
    input: CreateConversationSubmissionInput,
  ): Effect.fn.Return<AdmitRunResult, EventStreamError> {
    const payload = yield* stringify(input.input);
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `INSERT OR IGNORE INTO denora_agent_conversation_submissions
         (submission_id, session_key, kind, run_id, agent_name, conversation_id, message_id, payload, status, accepted_at)
         VALUES (?, ?, 'message', ?, ?, ?, ?, ?, 'queued', ?)
         RETURNING ${submissionColumns}`,
        input.submissionId,
        sessionKey(input.conversationId),
        input.runId,
        input.agentName,
        input.conversationId ?? null,
        input.triggerMessageId ?? null,
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
    if (existing.payload !== payload) {
      return yield* new EventStorageFailed({
        operation: "admit agent conversation submission",
        cause: new Error(`Submission ${input.submissionId} already has a conflicting payload.`),
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
      });
    }
    yield* publishTerminalOutboxes();
    const unsettled = yield* hasUnsettledSubmissions();
    return { abortedSubmissions, needsWake: unsettled, wakeDelayMs: WAKE_DELAY_MS };
  });

  const reconcile = Effect.fn("AgentConversationCoordinator.reconcile")(function* (
    input: ReconcileInput,
  ): Effect.fn.Return<ReconcileResult, EventStreamError> {
    while (true) {
      yield* publishTerminalOutboxes();
      yield* interruptStaleRunningSubmissions();
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
           SET status = 'running', attempt_id = ?, started_at = ?
           WHERE submission_id = (
             SELECT current.submission_id
              FROM denora_agent_conversation_submissions AS current
             WHERE current.status = 'queued'
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
    activeAttempts.set(submission.submissionId, controller);
    try {
      const completed = yield* reconstructCompletedRunResult(submission);
      if (completed !== null) {
        yield* reserveTerminal(submission, completed);
        return;
      }
      const prepared = yield* prepareSubmissionForExecution(submission);
      yield* markSubmissionInputApplied(submission);
      const execution = yield* AgentRunLifecycle.executeConversationSubmissionAttempt(store, {
        runId: submission.runId,
        agentName: submission.agentName,
        conversationId: submission.conversationId ?? "unknown",
        submissionId: submission.submissionId,
        triggerMessageId: submission.messageId ?? "unknown",
        input: prepared.input,
        pi,
        signal: controller.signal,
      }).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            yield* Effect.logError("agent run attempt execution failed before terminal outbox", {
              runId: submission.runId,
              submissionId: submission.submissionId,
              attemptId: submission.attemptId,
              error,
            });
            return yield* makeInterruptedResult(submission, errorMessage(error));
          }),
        ),
      );
      yield* sessionStore.finishRun({
        conversationId: submission.conversationId ?? "unknown",
        runId: submission.runId,
        isError: execution.isError,
        result: execution.result,
      });
      yield* reserveTerminal(submission, execution);
    } finally {
      activeAttempts.delete(submission.submissionId);
    }
  });

  const interruptStaleRunningSubmissions = Effect.fn(
    "AgentConversationCoordinator.interruptStaleRunningSubmissions",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
          FROM denora_agent_conversation_submissions
         WHERE status = 'running' AND started_at IS NOT NULL AND started_at <= ?
         ORDER BY sequence ASC`,
        Date.now() - RUNNING_STALE_MS,
      )
      .pipe(storageFailure("list stale running submissions"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect stale running submissions"));
    for (const row of rows) {
      const submission = parseSubmission(row);
      const completed = yield* reconstructCompletedRunResult(submission);
      if (completed !== null) {
        yield* reserveTerminal(submission, completed);
        continue;
      }
      if (submission.inputAppliedAt !== undefined && submission.abortRequestedAt === undefined) {
        yield* requeueStaleAppliedSubmission(submission);
        continue;
      }
      yield* reserveTerminal(
        submission,
        yield* makeInterruptedResult(
          submission,
          "Agent run was interrupted because its Durable Object stopped before completion. Please retry.",
        ),
      );
    }
  });

  const requeueStaleAppliedSubmission = (
    submission: Submission,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET status = 'queued', attempt_id = NULL, started_at = NULL
         WHERE submission_id = ? AND status = 'running' AND attempt_id = ?
           AND input_applied_at IS NOT NULL`,
        submission.submissionId,
        submission.attemptId ?? "",
      )
      .pipe(storageFailure("requeue stale applied conversation submission"), Effect.asVoid);

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
        yield* finalizeTerminal(outbox);
        const idleIndex = yield* nextStreamEventIndex(streamPath);
        yield* store.appendEventOnce(
          streamPath,
          idleEventKey(outbox.submissionId),
          idleEvent(outbox, idleIndex),
        );
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
    } = {},
  ): Effect.Effect<void, EventStreamError> =>
    stringify(execution.terminalEvent).pipe(
      Effect.flatMap((eventJson) => {
        const attemptId = options.attemptId ?? submission.attemptId ?? "";
        const fromStatuses = options.fromStatuses ?? ["running"];
        const statusPlaceholders = fromStatuses.map(() => "?").join(", ");
        return sql
          .exec(
            `UPDATE denora_agent_conversation_submissions
             SET status = 'terminalizing', attempt_id = COALESCE(attempt_id, ?),
                 terminal_event_key = ?, terminal_event_json = ?, error = ?
             WHERE submission_id = ? AND status IN (${statusPlaceholders})
               AND (attempt_id = ? OR attempt_id IS NULL)`,
            attemptId,
            terminalEventKey(submission.submissionId),
            eventJson,
            execution.isError ? (execution.error?.message ?? "Agent run failed.") : null,
            submission.submissionId,
            ...fromStatuses,
            attemptId,
          )
          .pipe(storageFailure("reserve terminal outbox"), Effect.asVoid);
      }),
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
    sql
      .exec(
        `UPDATE denora_agent_conversation_submissions
         SET status = 'settled', settled_at = ?
         WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
           AND terminal_event_key = ? AND terminal_event_offset IS NOT NULL`,
        Date.now(),
        outbox.submissionId,
        outbox.attemptId,
        outbox.eventKey,
      )
      .pipe(storageFailure("finalize terminal submission"), Effect.asVoid);

  const prepareSubmissionForExecution = Effect.fn(
    "AgentConversationCoordinator.prepareSubmissionForExecution",
  )(function* (
    submission: Submission,
  ): Effect.fn.Return<{ readonly input: unknown }, EventStreamError> {
    const conversationId = submission.conversationId;
    const messageId = submission.messageId;
    if (conversationId === undefined || messageId === undefined) {
      return yield* new EventStorageFailed({
        operation: "prepare conversation submission",
        cause: new Error("Conversation submission is missing its conversation or message id."),
      });
    }
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
        content: payload.submittedMessage,
      })
      .pipe(
        Effect.mapError(
          (cause) => new EventStorageFailed({ operation: "record conversation submission", cause }),
        ),
      );
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
    getSubmissionTerminal,
    reconcile,
  } satisfies Interface;
});

const ensureTables = (sql: Cloudflare.SqlStorage): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    for (const [operation, statement] of [
      ["create agent conversation submissions table", CREATE_SUBMISSIONS_TABLE],
      ["create agent conversation submissions status index", CREATE_SUBMISSIONS_STATUS_INDEX],
    ] as const) {
      yield* sql.exec(statement).pipe(storageFailure(operation), Effect.asVoid);
    }
    yield* ensureColumn(sql, {
      table: "denora_agent_conversation_submissions",
      column: "input_applied_at",
      definition: "input_applied_at INTEGER",
    });
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
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

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

const makeInterruptedResult = Effect.fn("AgentConversationCoordinator.makeInterruptedResult")(
  function* (submission: Submission, message: string): Effect.fn.Return<ExecuteRunAttemptResult> {
    const timestamp = yield* Effect.sync(() => new Date().toISOString());
    const terminalEvent = {
      v: 3,
      type: "submission_settled",
      instanceId: submission.conversationId ?? "unknown",
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

const terminalEventKey = (submissionId: string): string =>
  `agent-conversation:${submissionId}:terminal`;

const sessionKey = (conversationId: string | undefined): string =>
  `agent-session:${conversationId ?? "unknown"}:default`;

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
  "sequence, submission_id, session_key, kind, run_id, agent_name, conversation_id, message_id, payload, status, accepted_at, attempt_id, started_at, settled_at, abort_requested_at, input_applied_at, error, terminal_event_key, terminal_event_json, terminal_event_offset";

const parseSubmission = (row: SubmissionRow): Submission => ({
  sequence: row.sequence,
  submissionId: row.submission_id,
  sessionKey: row.session_key,
  kind: row.kind,
  runId: row.run_id,
  agentName: row.agent_name,
  conversationId: row.conversation_id ?? undefined,
  messageId: row.message_id ?? undefined,
  payload: row.payload,
  input: JSON.parse(row.payload) as unknown,
  status: row.status,
  acceptedAt: row.accepted_at,
  attemptId: row.attempt_id ?? undefined,
  startedAt: row.started_at ?? undefined,
  settledAt: row.settled_at ?? undefined,
  abortRequestedAt: row.abort_requested_at ?? undefined,
  inputAppliedAt: row.input_applied_at ?? undefined,
  error: row.error ?? undefined,
});

const idleEventKey = (submissionId: string): string => `agent-conversation:${submissionId}:idle`;

const idleEvent = (outbox: TerminalOutbox, eventIndex = idleEventIndex(outbox.event)): unknown => ({
  v: 3,
  type: "idle",
  instanceId: outbox.conversationId,
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
  readonly payload: string;
  readonly status: SubmissionStatus;
  readonly accepted_at: number;
  readonly attempt_id: string | null;
  readonly started_at: number | null;
  readonly settled_at: number | null;
  readonly abort_requested_at: number | null;
  readonly input_applied_at: number | null;
  readonly error: string | null;
  readonly terminal_event_key: string | null;
  readonly terminal_event_json: string | null;
  readonly terminal_event_offset: string | null;
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

type SubmissionStatus = "queued" | "running" | "terminalizing" | "settled";
type SubmissionKind = "message" | "dispatch";

interface Submission {
  readonly sequence: number;
  readonly submissionId: string;
  readonly sessionKey: string;
  readonly kind: SubmissionKind;
  readonly runId: string;
  readonly agentName: string;
  readonly conversationId?: string | undefined;
  readonly messageId?: string | undefined;
  readonly payload: string;
  readonly input: unknown;
  readonly status: SubmissionStatus;
  readonly acceptedAt: number;
  readonly attemptId?: string | undefined;
  readonly startedAt?: number | undefined;
  readonly settledAt?: number | undefined;
  readonly abortRequestedAt?: number | undefined;
  readonly inputAppliedAt?: number | undefined;
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

export * as AgentConversationCoordinator from "./AgentConversationCoordinator.ts";
