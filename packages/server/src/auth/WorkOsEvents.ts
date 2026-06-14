import type { User as WorkOsUser } from "@workos-inc/node";
import { and, eq, lte } from "drizzle-orm";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Db } from "../persistence/Db.ts";
import { schema } from "../persistence/schema.ts";
import { UserSync } from "./UserSync.ts";
import { WorkOsAuth } from "./WorkOsAuth.ts";

declare const crypto: { randomUUID(): string };

const CursorName = "users";
const LeaseMs = 4 * 60 * 1000;
const PageLimit = 100;
const UserEvents = ["user.created", "user.updated", "user.deleted"] as const;

type UserEventName = (typeof UserEvents)[number];

interface WorkOsUserEvent {
  readonly id: string;
  readonly event: UserEventName;
  readonly data: WorkOsUser;
  readonly createdAt: string;
}

export interface RunResult {
  readonly acquired: boolean;
  readonly processed: number;
  readonly cursor: string | null;
}

export interface Interface {
  readonly runOnce: Effect.Effect<RunResult, WorkOsEventsError | UserSync.UserSyncError>;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/WorkOsEvents") {}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* Db.Service;
    const auth = yield* WorkOsAuth.Service;

    return Service.of({
      runOnce: runOnceWithClient(db.client, auth.client),
    });
  }),
);

export const runOnce = Effect.fn("WorkOsEvents.runOnce")(function* () {
  const service = yield* Service;
  return yield* service.runOnce;
});

export class WorkOsEventsError extends Schema.TaggedErrorClass<WorkOsEventsError>()(
  "WorkOsEventsError",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export const runOnceWithClient = Effect.fn("WorkOsEvents.runOnceWithClient")(function* (
  client: Db.Client,
  workos: WorkOsAuth.Interface["client"],
): Effect.fn.Return<RunResult, WorkOsEventsError | UserSync.UserSyncError> {
  const lease = yield* acquireLease(client);
  if (!lease) return { acquired: false, processed: 0, cursor: null };

  return yield* Effect.gen(function* () {
    const cursor = yield* readCursor(client);
    const listed = yield* listEvents(workos, cursor);

    let processed = 0;
    let lastEventId = cursor;
    for (const event of listed.data as ReadonlyArray<WorkOsUserEvent>) {
      yield* processEvent(client, event);
      yield* writeCursor(client, event.id);
      processed += 1;
      lastEventId = event.id;
    }

    return {
      acquired: true,
      processed,
      cursor: lastEventId,
    };
  }).pipe(
    Effect.ensuring(
      releaseLease(client, lease).pipe(
        Effect.catch((error) => Effect.logWarning("Failed to release WorkOS event lease", error)),
      ),
    ),
  );
});

export const acquireLease = Effect.fn("WorkOsEvents.acquireLease")(function* (client: Db.Client) {
  const now = new Date();
  const nowIso = now.toISOString();
  const owner = crypto.randomUUID();
  const leasedUntil = new Date(now.getTime() + LeaseMs).toISOString();

  const rows = yield* client
    .insert(schema.workosEventSyncLocks)
    .values({
      name: CursorName,
      owner,
      leasedUntil,
      updatedAt: nowIso,
    })
    .onConflictDoUpdate({
      target: schema.workosEventSyncLocks.name,
      set: {
        owner,
        leasedUntil,
        updatedAt: nowIso,
      },
      where: lte(schema.workosEventSyncLocks.leasedUntil, nowIso),
    })
    .returning()
    .pipe(Effect.mapError((cause) => new WorkOsEventsError({ operation: "acquireLease", cause })));

  const row = rows[0];
  if (!row || row.owner !== owner) return null;
  return { owner };
});

export const releaseLease = Effect.fn("WorkOsEvents.releaseLease")(function* (
  client: Db.Client,
  lease: { readonly owner: string },
) {
  yield* client
    .delete(schema.workosEventSyncLocks)
    .where(
      and(
        eq(schema.workosEventSyncLocks.name, CursorName),
        eq(schema.workosEventSyncLocks.owner, lease.owner),
      ),
    )
    .pipe(Effect.mapError((cause) => new WorkOsEventsError({ operation: "releaseLease", cause })));
});

export const readCursor = Effect.fn("WorkOsEvents.readCursor")(function* (client: Db.Client) {
  const rows = yield* client
    .select()
    .from(schema.workosEventCursors)
    .where(eq(schema.workosEventCursors.name, CursorName))
    .limit(1)
    .pipe(Effect.mapError((cause) => new WorkOsEventsError({ operation: "readCursor", cause })));

  return rows[0]?.lastEventId ?? null;
});

export const writeCursor = Effect.fn("WorkOsEvents.writeCursor")(function* (
  client: Db.Client,
  lastEventId: string,
) {
  const now = new Date().toISOString();
  yield* client
    .insert(schema.workosEventCursors)
    .values({
      name: CursorName,
      lastEventId,
      updatedAt: now,
    })
    .onConflictDoUpdate({
      target: schema.workosEventCursors.name,
      set: {
        lastEventId,
        updatedAt: now,
      },
    })
    .pipe(Effect.mapError((cause) => new WorkOsEventsError({ operation: "writeCursor", cause })));
});

const listEvents = Effect.fn("WorkOsEvents.listEvents")(
  (workos: WorkOsAuth.Interface["client"], cursor: string | null) =>
    Effect.tryPromise({
      try: () =>
        workos.events.listEvents({
          events: [...UserEvents],
          ...(cursor ? { after: cursor } : {}),
          limit: PageLimit,
          order: "asc",
        }),
      catch: (cause) => new WorkOsEventsError({ operation: "listEvents", cause }),
    }),
);

const processEvent = Effect.fn("WorkOsEvents.processEvent")(function* (
  client: Db.Client,
  event: WorkOsUserEvent,
) {
  switch (event.event) {
    case "user.created":
    case "user.updated":
      yield* UserSync.syncUser(client, event.data);
      return;
    case "user.deleted":
      yield* UserSync.syncDeletedUser(client, event.data, event.createdAt);
      return;
  }
});

export * as WorkOsEvents from "./WorkOsEvents.ts";
