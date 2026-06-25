import type * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import { EventSerializationFailed, EventStorageFailed } from "./EventStreamStore.ts";

const CREATE_RUNS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_durable_fiber_runs (
  id            TEXT PRIMARY KEY,
  name          TEXT NOT NULL,
  snapshot_json TEXT,
  created_at    INTEGER NOT NULL
)`;

const CREATE_FIBERS_TABLE = `
CREATE TABLE IF NOT EXISTS denora_durable_fibers (
  fiber_id        TEXT PRIMARY KEY,
  idempotency_key TEXT,
  name            TEXT NOT NULL,
  status          TEXT NOT NULL,
  snapshot_json   TEXT,
  metadata_json   TEXT,
  error_message   TEXT,
  created_at      INTEGER NOT NULL,
  started_at      INTEGER,
  completed_at    INTEGER
)`;

const CREATE_FIBERS_IDEMPOTENCY_INDEX = `
CREATE UNIQUE INDEX IF NOT EXISTS denora_durable_fibers_idempotency_key_idx
ON denora_durable_fibers (idempotency_key)
WHERE idempotency_key IS NOT NULL`;

const CREATE_FIBERS_STATUS_INDEX = `
CREATE INDEX IF NOT EXISTS denora_durable_fibers_status_created_idx
ON denora_durable_fibers (status, created_at ASC)`;

export type FiberStatus = "pending" | "running" | "completed" | "failed" | "interrupted";

export interface FiberContext {
  readonly id: string;
  readonly signal: AbortSignal;
  readonly stash: (
    snapshot: unknown,
  ) => Effect.Effect<void, EventSerializationFailed | EventStorageFailed>;
}

export interface FiberInspection {
  readonly fiberId: string;
  readonly idempotencyKey?: string | undefined;
  readonly name: string;
  readonly status: FiberStatus;
  readonly snapshot: unknown;
  readonly metadata: Record<string, unknown>;
  readonly errorMessage?: string | undefined;
  readonly createdAt: number;
  readonly startedAt?: number | undefined;
  readonly completedAt?: number | undefined;
}

export interface StartResult {
  readonly accepted: boolean;
  readonly fiber: FiberInspection;
}

export interface StartInput {
  readonly fiberId: string;
  readonly idempotencyKey: string;
  readonly name: string;
  readonly metadata?: Record<string, unknown> | undefined;
  readonly initialSnapshot?: unknown;
  readonly signal?: AbortSignal | undefined;
}

export interface RecoveryContext extends FiberInspection {
  readonly recoveryReason: "interrupted";
}

export interface Interface {
  readonly startManaged: <A, E, R>(
    input: StartInput,
    run: (ctx: FiberContext) => Effect.Effect<A, E, R>,
  ) => Effect.Effect<StartResult, E | EventSerializationFailed | EventStorageFailed, R>;
  readonly recoverInterrupted: <E, R>(
    onRecovered: (ctx: RecoveryContext) => Effect.Effect<void, E, R>,
  ) => Effect.Effect<void, E | EventSerializationFailed | EventStorageFailed, R>;
}

export const makeSqlite = Effect.fn("DurableFiber.makeSqlite")(function* (
  sql: Cloudflare.SqlStorage,
): Effect.fn.Return<Interface, EventStorageFailed> {
  yield* ensureTables(sql);
  const activeFibers = new Set<string>();

  const startManaged = <A, E, R>(
    input: StartInput,
    run: (ctx: FiberContext) => Effect.Effect<A, E, R>,
  ): Effect.Effect<StartResult, E | EventSerializationFailed | EventStorageFailed, R> =>
    Effect.gen(function* () {
      if (input.fiberId.trim() === "") {
        return yield* new EventStorageFailed({
          operation: "start durable fiber",
          cause: new Error("fiberId must not be blank"),
        });
      }
      if (input.idempotencyKey.trim() === "") {
        return yield* new EventStorageFailed({
          operation: "start durable fiber",
          cause: new Error("idempotencyKey must not be blank"),
        });
      }

      const existingById = yield* readFiber(input.fiberId);
      const existingByKey = yield* readFiberByKey(input.idempotencyKey);
      if (
        existingById !== null &&
        existingByKey !== null &&
        existingById.fiber_id !== existingByKey.fiber_id
      ) {
        return yield* new EventStorageFailed({
          operation: "start durable fiber",
          cause: new Error("fiberId and idempotencyKey refer to different durable fibers"),
        });
      }

      const existing = existingById ?? existingByKey;
      if (existing !== null) return { accepted: false, fiber: yield* inspectFiber(existing) };

      const now = Date.now();
      yield* sql
        .exec(
          `INSERT INTO denora_durable_fibers
           (fiber_id, idempotency_key, name, status, snapshot_json, metadata_json,
            error_message, created_at, started_at, completed_at)
           VALUES (?, ?, ?, 'pending', NULL, ?, NULL, ?, NULL, NULL)`,
          input.fiberId,
          input.idempotencyKey,
          input.name,
          input.metadata === undefined ? null : yield* stringify(input.metadata),
          now,
        )
        .pipe(storageFailure("create durable fiber"), Effect.asVoid);

      yield* executeManaged(input, run);
      const completed = yield* readFiber(input.fiberId);
      if (completed === null) {
        return yield* new EventStorageFailed({
          operation: "inspect completed durable fiber",
          cause: new Error(`Durable fiber ${input.fiberId} disappeared.`),
        });
      }
      return { accepted: true, fiber: yield* inspectFiber(completed) };
    });

  const executeManaged = <A, E, R>(
    input: StartInput,
    run: (ctx: FiberContext) => Effect.Effect<A, E, R>,
  ): Effect.Effect<void, E | EventSerializationFailed | EventStorageFailed, R> =>
    Effect.gen(function* () {
      const now = Date.now();
      yield* sql
        .exec(
          `UPDATE denora_durable_fibers
           SET status = 'running', started_at = ?
           WHERE fiber_id = ? AND status = 'pending'`,
          now,
          input.fiberId,
        )
        .pipe(storageFailure("mark durable fiber running"), Effect.asVoid);
      const running = yield* readFiber(input.fiberId);
      if (running?.status !== "running") return;

      const signal = input.signal ?? new AbortController().signal;
      activeFibers.add(input.fiberId);
      yield* Effect.gen(function* () {
        yield* sql
          .exec(
            `INSERT INTO denora_durable_fiber_runs (id, name, snapshot_json, created_at)
             VALUES (?, ?, NULL, ?)`,
            input.fiberId,
            input.name,
            Date.now(),
          )
          .pipe(storageFailure("create durable fiber run"), Effect.asVoid);
        const stash = (snapshot: unknown) => writeSnapshot(input.fiberId, snapshot);
        if ("initialSnapshot" in input) yield* stash(input.initialSnapshot);
        yield* run({ id: input.fiberId, signal, stash });
        yield* settleFiber(input.fiberId, "completed");
      }).pipe(
        Effect.catch((error: E | EventSerializationFailed | EventStorageFailed) =>
          Effect.gen(function* () {
            yield* settleFiber(input.fiberId, "failed", fiberErrorMessage(error)).pipe(
              Effect.catch(() => Effect.void),
            );
            return yield* Effect.fail(error);
          }),
        ),
        Effect.ensuring(
          Effect.gen(function* () {
            activeFibers.delete(input.fiberId);
            yield* sql
              .exec(`DELETE FROM denora_durable_fiber_runs WHERE id = ?`, input.fiberId)
              .pipe(
                storageFailure("delete durable fiber run"),
                Effect.asVoid,
                Effect.catch(() => Effect.void),
              );
          }),
        ),
      );
    });

  const recoverInterrupted = <E, R>(
    onRecovered: (ctx: RecoveryContext) => Effect.Effect<void, E, R>,
  ): Effect.Effect<void, E | EventSerializationFailed | EventStorageFailed, R> =>
    Effect.gen(function* () {
      const runRows = yield* listRunRows();
      for (const runRow of runRows) {
        if (activeFibers.has(runRow.id)) continue;
        const fiber = yield* readFiber(runRow.id);
        if (fiber !== null && isTerminal(fiber.status)) {
          yield* deleteRun(runRow.id);
          continue;
        }
        if (fiber !== null) {
          yield* interruptFiber(fiber.fiber_id, runRow.snapshot_json);
          const interrupted = yield* readFiber(fiber.fiber_id);
          if (interrupted !== null) yield* onRecovered(yield* recoveryContext(interrupted));
        }
        yield* deleteRun(runRow.id);
      }

      const ledgerOnlyRows = yield* listLedgerOnlyRows();
      for (const row of ledgerOnlyRows) {
        if (activeFibers.has(row.fiber_id)) continue;
        yield* interruptFiber(row.fiber_id, row.snapshot_json);
        const interrupted = yield* readFiber(row.fiber_id);
        if (interrupted !== null) yield* onRecovered(yield* recoveryContext(interrupted));
      }
    });

  const readFiber = Effect.fn("DurableFiber.readFiber")(function* (
    fiberId: string,
  ): Effect.fn.Return<FiberRow | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<FiberRow>(
        `SELECT ${fiberColumns} FROM denora_durable_fibers WHERE fiber_id = ? LIMIT 1`,
        fiberId,
      )
      .pipe(storageFailure("read durable fiber"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect durable fiber"));
    return rows[0] ?? null;
  });

  const readFiberByKey = Effect.fn("DurableFiber.readFiberByKey")(function* (
    idempotencyKey: string,
  ): Effect.fn.Return<FiberRow | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<FiberRow>(
        `SELECT ${fiberColumns} FROM denora_durable_fibers WHERE idempotency_key = ? LIMIT 1`,
        idempotencyKey,
      )
      .pipe(storageFailure("read durable fiber by idempotency key"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect durable fiber by key"));
    return rows[0] ?? null;
  });

  const writeSnapshot = (
    fiberId: string,
    snapshot: unknown,
  ): Effect.Effect<void, EventSerializationFailed | EventStorageFailed> =>
    stringify(snapshot).pipe(
      Effect.flatMap((snapshotJson) =>
        Effect.gen(function* () {
          yield* sql
            .exec(
              `UPDATE denora_durable_fiber_runs SET snapshot_json = ? WHERE id = ?`,
              snapshotJson,
              fiberId,
            )
            .pipe(storageFailure("stash durable fiber run snapshot"), Effect.asVoid);
          yield* sql
            .exec(
              `UPDATE denora_durable_fibers SET snapshot_json = ? WHERE fiber_id = ?`,
              snapshotJson,
              fiberId,
            )
            .pipe(storageFailure("stash durable fiber snapshot"), Effect.asVoid);
        }),
      ),
    );

  const settleFiber = (
    fiberId: string,
    status: Extract<FiberStatus, "completed" | "failed">,
    errorMessage?: string | undefined,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_durable_fibers
         SET status = ?, error_message = ?, completed_at = COALESCE(completed_at, ?)
         WHERE fiber_id = ? AND status = 'running'`,
        status,
        errorMessage ?? null,
        Date.now(),
        fiberId,
      )
      .pipe(storageFailure("settle durable fiber"), Effect.asVoid);

  const interruptFiber = (
    fiberId: string,
    snapshotJson: string | null,
  ): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(
        `UPDATE denora_durable_fibers
         SET status = 'interrupted', snapshot_json = COALESCE(?, snapshot_json), completed_at = COALESCE(completed_at, ?)
         WHERE fiber_id = ? AND status IN ('pending', 'running')`,
        snapshotJson,
        Date.now(),
        fiberId,
      )
      .pipe(storageFailure("interrupt durable fiber"), Effect.asVoid);

  const deleteRun = (fiberId: string): Effect.Effect<void, EventStorageFailed> =>
    sql
      .exec(`DELETE FROM denora_durable_fiber_runs WHERE id = ?`, fiberId)
      .pipe(storageFailure("delete recovered durable fiber run"), Effect.asVoid);

  const listRunRows = Effect.fn("DurableFiber.listRunRows")(function* (): Effect.fn.Return<
    ReadonlyArray<RunRow>,
    EventStorageFailed
  > {
    const cursor = yield* sql
      .exec<RunRow>(
        `SELECT id, name, snapshot_json, created_at
         FROM denora_durable_fiber_runs
         ORDER BY created_at ASC`,
      )
      .pipe(storageFailure("list durable fiber runs"));
    return yield* cursor.toArray().pipe(storageFailure("collect durable fiber runs"));
  });

  const listLedgerOnlyRows = Effect.fn("DurableFiber.listLedgerOnlyRows")(
    function* (): Effect.fn.Return<ReadonlyArray<FiberRow>, EventStorageFailed> {
      const cursor = yield* sql
        .exec<FiberRow>(
          `SELECT f.fiber_id, f.idempotency_key, f.name, f.status, f.snapshot_json,
                f.metadata_json, f.error_message, f.created_at, f.started_at, f.completed_at
         FROM denora_durable_fibers AS f
         LEFT JOIN denora_durable_fiber_runs AS r ON r.id = f.fiber_id
         WHERE f.status IN ('pending', 'running') AND r.id IS NULL
         ORDER BY f.created_at ASC`,
        )
        .pipe(storageFailure("list ledger-only durable fibers"));
      return yield* cursor.toArray().pipe(storageFailure("collect ledger-only durable fibers"));
    },
  );

  const inspectFiber = (row: FiberRow): Effect.Effect<FiberInspection, EventSerializationFailed> =>
    Effect.gen(function* () {
      return {
        fiberId: row.fiber_id,
        idempotencyKey: row.idempotency_key ?? undefined,
        name: row.name,
        status: row.status,
        snapshot: yield* parseJson(row.snapshot_json),
        metadata: yield* parseJsonObject(row.metadata_json),
        errorMessage: row.error_message ?? undefined,
        createdAt: row.created_at,
        startedAt: row.started_at ?? undefined,
        completedAt: row.completed_at ?? undefined,
      } satisfies FiberInspection;
    });

  const recoveryContext = (
    row: FiberRow,
  ): Effect.Effect<RecoveryContext, EventSerializationFailed> =>
    inspectFiber(row).pipe(Effect.map((fiber) => ({ ...fiber, recoveryReason: "interrupted" })));

  return { startManaged, recoverInterrupted } satisfies Interface;
});

export const ensureTables = (sql: Cloudflare.SqlStorage): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    for (const [operation, statement] of [
      ["create durable fiber runs table", CREATE_RUNS_TABLE],
      ["create durable fibers table", CREATE_FIBERS_TABLE],
      ["create durable fibers idempotency index", CREATE_FIBERS_IDEMPOTENCY_INDEX],
      ["create durable fibers status index", CREATE_FIBERS_STATUS_INDEX],
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

const parseJson = (value: string | null): Effect.Effect<unknown, EventSerializationFailed> =>
  value === null
    ? Effect.succeed(null)
    : Effect.try({
        try: () => JSON.parse(value) as unknown,
        catch: (cause) => new EventSerializationFailed({ cause }),
      });

const parseJsonObject = (
  value: string | null,
): Effect.Effect<Record<string, unknown>, EventSerializationFailed> =>
  parseJson(value).pipe(
    Effect.map((parsed) =>
      typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
        ? (parsed as Record<string, unknown>)
        : {},
    ),
  );

const fiberErrorMessage = (error: unknown): string => {
  if (error instanceof EventStorageFailed)
    return `Durable fiber storage failed during ${error.operation}.`;
  if (error instanceof EventSerializationFailed)
    return "Durable fiber snapshot serialization failed.";
  if (error instanceof Error) return error.message;
  return String(error);
};

const isTerminal = (status: FiberStatus): boolean =>
  status === "completed" || status === "failed" || status === "interrupted";

const fiberColumns =
  "fiber_id, idempotency_key, name, status, snapshot_json, metadata_json, error_message, created_at, started_at, completed_at";

interface RunRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly id: string;
  readonly name: string;
  readonly snapshot_json: string | null;
  readonly created_at: number;
}

interface FiberRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly fiber_id: string;
  readonly idempotency_key: string | null;
  readonly name: string;
  readonly status: FiberStatus;
  readonly snapshot_json: string | null;
  readonly metadata_json: string | null;
  readonly error_message: string | null;
  readonly created_at: number;
  readonly started_at: number | null;
  readonly completed_at: number | null;
}

export * as DurableFiber from "./DurableFiber.ts";
