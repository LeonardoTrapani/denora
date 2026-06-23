import type * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import {
  AgentRunLifecycle,
  type CreateRunInput,
  type ExecuteRunAttemptResult,
} from "./Lifecycle.ts";
import {
  type EventStreamError,
  type EventStreamStore,
  EventSerializationFailed,
  EventStorageFailed,
  runStreamPath,
} from "./EventStreamStore.ts";
import type { Interface as PiRuntimeInterface } from "../agent-loop/PiRuntime.ts";

const WAKE_DELAY_MS = 30_000;
const RUNNING_STALE_MS = 15 * 60 * 1000;

const CREATE_SUBMISSIONS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_run_submissions (
  sequence              INTEGER PRIMARY KEY AUTOINCREMENT,
  submission_id         TEXT NOT NULL UNIQUE,
  run_id                TEXT NOT NULL,
  payload               TEXT NOT NULL,
  status                TEXT NOT NULL,
  accepted_at           INTEGER NOT NULL,
  attempt_id            TEXT,
  started_at            INTEGER,
  settled_at            INTEGER,
  error                 TEXT,
  terminal_event_key    TEXT,
  terminal_event_json   TEXT,
  terminal_event_offset TEXT
)`;

const CREATE_SUBMISSIONS_STATUS_INDEX = `
CREATE INDEX IF NOT EXISTS denora_agent_run_submissions_status_sequence_idx
ON denora_agent_run_submissions (status, sequence ASC)`;

export interface AdmitRunResult {
  readonly admitted: boolean;
}

export interface ReconcileInput {
  readonly pi: PiRuntimeInterface;
  readonly scheduleWake: (delayMs: number) => Effect.Effect<void, EventStorageFailed>;
}

export interface ReconcileResult {
  readonly needsWake: boolean;
  readonly wakeDelayMs: number;
}

export interface Interface {
  readonly admitRun: (input: CreateRunInput) => Effect.Effect<AdmitRunResult, EventStreamError>;
  readonly reconcile: (input: ReconcileInput) => Effect.Effect<ReconcileResult, EventStreamError>;
}

export const makeSqliteAgentRunCoordinator = Effect.fn(
  "AgentRunCoordinator.makeSqliteAgentRunCoordinator",
)(function* (
  sql: Cloudflare.SqlStorage,
  store: EventStreamStore,
): Effect.fn.Return<Interface, EventStorageFailed> {
  yield* ensureTables(sql);

  const admitRun = Effect.fn("AgentRunCoordinator.admitRun")(function* (
    input: CreateRunInput,
  ): Effect.fn.Return<AdmitRunResult, EventStreamError> {
    const payload = yield* stringify(input.input);
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `INSERT OR IGNORE INTO denora_agent_run_submissions
         (submission_id, run_id, payload, status, accepted_at)
         VALUES (?, ?, ?, 'queued', ?)
         RETURNING ${submissionColumns}`,
        input.runId,
        input.runId,
        payload,
        Date.now(),
      )
      .pipe(storageFailure("admit agent run submission"));
    const inserted = yield* cursor.toArray().pipe(storageFailure("collect admitted submission"));
    if (inserted[0] !== undefined) return { admitted: true };

    const existing = yield* readSubmission(input.runId);
    if (existing === null) {
      return yield* new EventStorageFailed({
        operation: "admit agent run submission",
        cause: new Error("Submission insert returned no row and no existing submission."),
      });
    }
    if (existing.payload !== payload) {
      return yield* new EventStorageFailed({
        operation: "admit agent run submission",
        cause: new Error(`Agent run ${input.runId} already has a conflicting payload.`),
      });
    }
    return { admitted: false };
  });

  const reconcile = Effect.fn("AgentRunCoordinator.reconcile")(function* (
    input: ReconcileInput,
  ): Effect.fn.Return<ReconcileResult, EventStreamError> {
    yield* publishTerminalOutboxes();
    yield* interruptStaleRunningSubmissions();
    yield* publishTerminalOutboxes();

    if (yield* hasUnsettledSubmissions()) {
      yield* input.scheduleWake(WAKE_DELAY_MS);
    }

    const claim = yield* claimNextSubmission();
    if (claim !== null) {
      yield* processClaimedSubmission(claim, input.pi);
      yield* publishTerminalOutboxes();
    }

    const unsettled = yield* hasUnsettledSubmissions();
    return { needsWake: unsettled, wakeDelayMs: WAKE_DELAY_MS };
  });

  const readSubmission = Effect.fn("AgentRunCoordinator.readSubmission")(function* (
    submissionId: string,
  ): Effect.fn.Return<Submission | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
         FROM denora_agent_run_submissions
         WHERE submission_id = ?
         LIMIT 1`,
        submissionId,
      )
      .pipe(storageFailure("read agent run submission"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect agent run submission"));
    return rows[0] === undefined ? null : parseSubmission(rows[0]);
  });

  const hasUnsettledSubmissions = Effect.fn("AgentRunCoordinator.hasUnsettledSubmissions")(
    function* (): Effect.fn.Return<boolean, EventStorageFailed> {
      const cursor = yield* sql
        .exec<Row>(
          `SELECT 1 AS value
           FROM denora_agent_run_submissions
           WHERE status IN ('queued', 'running', 'terminalizing')
           LIMIT 1`,
        )
        .pipe(storageFailure("detect unsettled agent run submissions"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect unsettled submissions"));
      return rows[0] !== undefined;
    },
  );

  const claimNextSubmission = Effect.fn("AgentRunCoordinator.claimNextSubmission")(
    function* (): Effect.fn.Return<Submission | null, EventStorageFailed> {
      const cursor = yield* sql
        .exec<SubmissionRow>(
          `UPDATE denora_agent_run_submissions
           SET status = 'running', attempt_id = ?, started_at = ?
           WHERE submission_id = (
             SELECT submission_id
             FROM denora_agent_run_submissions
             WHERE status = 'queued'
             ORDER BY sequence ASC
             LIMIT 1
           )
           RETURNING ${submissionColumns}`,
          crypto.randomUUID(),
          Date.now(),
        )
        .pipe(storageFailure("claim agent run submission"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect claimed submission"));
      return rows[0] === undefined ? null : parseSubmission(rows[0]);
    },
  );

  const processClaimedSubmission = Effect.fn("AgentRunCoordinator.processClaimedSubmission")(
    function* (
      submission: Submission,
      pi: PiRuntimeInterface,
    ): Effect.fn.Return<void, EventStreamError> {
      if (submission.attemptId === undefined) return;
      const execution = yield* AgentRunLifecycle.executeRunAttempt(store, {
        runId: submission.runId,
        input: submission.input,
        pi,
      }).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            yield* Effect.logError("agent run attempt execution failed before terminal outbox", {
              runId: submission.runId,
              submissionId: submission.submissionId,
              attemptId: submission.attemptId,
              error,
            });
            return yield* makeInterruptedResult(submission.runId, errorMessage(error));
          }),
        ),
      );
      yield* reserveTerminal(submission, execution);
    },
  );

  const interruptStaleRunningSubmissions = Effect.fn(
    "AgentRunCoordinator.interruptStaleRunningSubmissions",
  )(function* (): Effect.fn.Return<void, EventStreamError> {
    const cursor = yield* sql
      .exec<SubmissionRow>(
        `SELECT ${submissionColumns}
         FROM denora_agent_run_submissions
         WHERE status = 'running' AND started_at IS NOT NULL AND started_at <= ?
         ORDER BY sequence ASC`,
        Date.now() - RUNNING_STALE_MS,
      )
      .pipe(storageFailure("list stale running submissions"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect stale running submissions"));
    for (const row of rows) {
      const submission = parseSubmission(row);
      yield* reserveTerminal(
        submission,
        yield* makeInterruptedResult(
          submission.runId,
          "Agent run was interrupted because its Durable Object stopped before completion. Please retry.",
        ),
      );
    }
  });

  const publishTerminalOutboxes = Effect.fn("AgentRunCoordinator.publishTerminalOutboxes")(
    function* (): Effect.fn.Return<void, EventStreamError> {
      const outboxes = yield* listPendingTerminalOutboxes();
      for (const outbox of outboxes) {
        const offset =
          outbox.offset ??
          (yield* store.appendEventOnce(
            runStreamPath(outbox.runId),
            outbox.eventKey,
            outbox.event,
          ));
        yield* recordTerminalOffset(outbox, offset);
        yield* finalizeTerminal(outbox);
        yield* store.closeStream(runStreamPath(outbox.runId));
      }
    },
  );

  const listPendingTerminalOutboxes = Effect.fn("AgentRunCoordinator.listPendingTerminalOutboxes")(
    function* (): Effect.fn.Return<ReadonlyArray<TerminalOutbox>, EventStorageFailed> {
      const cursor = yield* sql
        .exec<TerminalOutboxRow>(
          `SELECT submission_id, run_id, attempt_id, terminal_event_key,
                  terminal_event_json, terminal_event_offset
           FROM denora_agent_run_submissions
           WHERE status = 'terminalizing'
           ORDER BY sequence ASC`,
        )
        .pipe(storageFailure("list terminal outboxes"));
      const rows = yield* cursor.toArray().pipe(storageFailure("collect terminal outboxes"));
      return yield* Effect.forEach(rows, parseTerminalOutbox);
    },
  );

  const reserveTerminal = (
    submission: Submission,
    execution: ExecuteRunAttemptResult,
  ): Effect.Effect<void, EventStreamError> =>
    stringify(execution.terminalEvent).pipe(
      Effect.flatMap((eventJson) =>
        sql
          .exec(
            `UPDATE denora_agent_run_submissions
             SET status = 'terminalizing', terminal_event_key = ?, terminal_event_json = ?, error = ?
             WHERE submission_id = ? AND status = 'running' AND attempt_id = ?`,
            terminalEventKey(submission.submissionId),
            eventJson,
            execution.isError ? (execution.error?.message ?? "Agent run failed.") : null,
            submission.submissionId,
            submission.attemptId ?? "",
          )
          .pipe(storageFailure("reserve terminal outbox"), Effect.asVoid),
      ),
    );

  const recordTerminalOffset = (
    outbox: TerminalOutbox,
    offset: string,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_run_submissions
         SET terminal_event_offset = COALESCE(terminal_event_offset, ?)
         WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
           AND terminal_event_key = ?`,
        offset,
        outbox.submissionId,
        outbox.attemptId,
        outbox.eventKey,
      )
      .pipe(storageFailure("record terminal event offset"), Effect.asVoid);

  const finalizeTerminal = (outbox: TerminalOutbox): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_agent_run_submissions
         SET status = 'settled', settled_at = ?
         WHERE submission_id = ? AND status = 'terminalizing' AND attempt_id = ?
           AND terminal_event_key = ? AND terminal_event_offset IS NOT NULL`,
        Date.now(),
        outbox.submissionId,
        outbox.attemptId,
        outbox.eventKey,
      )
      .pipe(storageFailure("finalize terminal submission"), Effect.asVoid);

  return { admitRun, reconcile } satisfies Interface;
});

const ensureTables = (sql: Cloudflare.SqlStorage): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    for (const [operation, statement] of [
      ["create agent run submissions table", CREATE_SUBMISSIONS_TABLE],
      ["create agent run submissions status index", CREATE_SUBMISSIONS_STATUS_INDEX],
    ] as const) {
      yield* sql.exec(statement).pipe(storageFailure(operation), Effect.asVoid);
    }
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

const makeInterruptedResult = Effect.fn("AgentRunCoordinator.makeInterruptedResult")(function* (
  runId: string,
  message: string,
): Effect.fn.Return<ExecuteRunAttemptResult> {
  const timestamp = yield* Effect.sync(() => new Date().toISOString());
  const terminalEvent = {
    v: 3,
    type: "run_end",
    runId,
    eventIndex: 1,
    timestamp,
    isError: true,
    result: null,
    durationMs: 0,
    error: { message },
  };
  return { terminalEvent, durationMs: 0, isError: true, result: null, error: { message } };
});

const errorMessage = (error: EventStreamError): string =>
  error._tag === "EventStorageFailed"
    ? `Agent run storage failed during ${error.operation}.`
    : "Agent run execution failed.";

const terminalEventKey = (submissionId: string): string => `agent-run:${submissionId}:terminal`;

const submissionColumns =
  "sequence, submission_id, run_id, payload, status, accepted_at, attempt_id, started_at, settled_at, error, terminal_event_key, terminal_event_json, terminal_event_offset";

const parseSubmission = (row: SubmissionRow): Submission => ({
  sequence: row.sequence,
  submissionId: row.submission_id,
  runId: row.run_id,
  payload: row.payload,
  input: JSON.parse(row.payload) as unknown,
  status: row.status,
  acceptedAt: row.accepted_at,
  attemptId: row.attempt_id ?? undefined,
  startedAt: row.started_at ?? undefined,
  settledAt: row.settled_at ?? undefined,
  error: row.error ?? undefined,
});

const parseTerminalOutbox = (
  row: TerminalOutboxRow,
): Effect.Effect<TerminalOutbox, EventStorageFailed> =>
  Effect.try({
    try: () => ({
      submissionId: row.submission_id,
      runId: row.run_id,
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

interface SubmissionRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly sequence: number;
  readonly submission_id: string;
  readonly run_id: string;
  readonly payload: string;
  readonly status: SubmissionStatus;
  readonly accepted_at: number;
  readonly attempt_id: string | null;
  readonly started_at: number | null;
  readonly settled_at: number | null;
  readonly error: string | null;
  readonly terminal_event_key: string | null;
  readonly terminal_event_json: string | null;
  readonly terminal_event_offset: string | null;
}

interface TerminalOutboxRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly submission_id: string;
  readonly run_id: string;
  readonly attempt_id: string;
  readonly terminal_event_key: string;
  readonly terminal_event_json: string;
  readonly terminal_event_offset: string | null;
}

type SubmissionStatus = "queued" | "running" | "terminalizing" | "settled";

interface Submission {
  readonly sequence: number;
  readonly submissionId: string;
  readonly runId: string;
  readonly payload: string;
  readonly input: unknown;
  readonly status: SubmissionStatus;
  readonly acceptedAt: number;
  readonly attemptId?: string | undefined;
  readonly startedAt?: number | undefined;
  readonly settledAt?: number | undefined;
  readonly error?: string | undefined;
}

interface TerminalOutbox {
  readonly submissionId: string;
  readonly runId: string;
  readonly attemptId: string;
  readonly eventKey: string;
  readonly event: unknown;
  readonly offset?: string | undefined;
}

export * as AgentRunCoordinator from "./AgentRunCoordinator.ts";
