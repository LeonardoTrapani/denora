import { assert, layer } from "@effect/vitest";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as Effect from "effect/Effect";
import type { WorkOsAuth } from "../../src/auth/WorkOsAuth.ts";
import { WorkOsEvents } from "../../src/auth/WorkOsEvents.ts";
import { Db } from "../../src/persistence/Db.ts";
import { Database } from "../helpers/Database.ts";
import { makeWorkOsUser } from "../helpers/fixtures.ts";

// Builds a fake WorkOS client whose `events.listEvents` returns a fixed page.
// `calls` records every invocation so contention tests can assert it was never
// reached.
const makeWorkos = (
  data: ReadonlyArray<unknown>,
  calls: Array<unknown>,
): WorkOsAuth.Interface["client"] =>
  ({
    events: {
      listEvents: async (args: unknown) => {
        calls.push(args);
        return { data };
      },
    },
  }) as unknown as WorkOsAuth.Interface["client"];

const isoPlus = (ms: number) => new Date(Date.now() + ms).toISOString();

layer(Database.dbLayer)("WorkOsEvents", (it) => {
  it.effect("acquireLease returns owner once, then null while still valid", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      const first = yield* WorkOsEvents.acquireLease(db.client);
      assert.isNotNull(first);
      assert.isString(first!.owner);

      const second = yield* WorkOsEvents.acquireLease(db.client);
      assert.isNull(second);
    }),
  );

  it.effect("after releaseLease, acquireLease succeeds again", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      const first = yield* WorkOsEvents.acquireLease(db.client);
      assert.isNotNull(first);

      yield* WorkOsEvents.releaseLease(db.client, first!);

      const sql = yield* PgClient.PgClient;
      const afterRelease = yield* sql<{
        n: number;
      }>`select count(*)::int as n from workos_event_sync_locks`;
      assert.strictEqual(afterRelease[0]!.n, 0);

      const second = yield* WorkOsEvents.acquireLease(db.client);
      assert.isNotNull(second);
    }),
  );

  it.effect("expired lease is stolen on acquireLease", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;
      const sql = yield* PgClient.PgClient;

      const past = isoPlus(-60 * 1000);
      yield* sql`insert into workos_event_sync_locks (name, owner, leased_until, updated_at)
        values ('users', 'someone-else', ${past}, ${past})`;

      const stolen = yield* WorkOsEvents.acquireLease(db.client);
      assert.isNotNull(stolen);
      assert.notStrictEqual(stolen!.owner, "someone-else");

      const rows = yield* sql<{
        owner: string;
      }>`select owner from workos_event_sync_locks where name = 'users'`;
      assert.strictEqual(rows[0]!.owner, stolen!.owner);
    }),
  );

  it.effect("readCursor is null initially, then reflects writeCursor (upsert)", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      assert.isNull(yield* WorkOsEvents.readCursor(db.client));

      yield* WorkOsEvents.writeCursor(db.client, "evt_1");
      assert.strictEqual(yield* WorkOsEvents.readCursor(db.client), "evt_1");

      yield* WorkOsEvents.writeCursor(db.client, "evt_2");
      assert.strictEqual(yield* WorkOsEvents.readCursor(db.client), "evt_2");

      const sql = yield* PgClient.PgClient;
      const rows = yield* sql<{
        n: number;
      }>`select count(*)::int as n from workos_event_cursors`;
      assert.strictEqual(rows[0]!.n, 1);
    }),
  );

  it.effect("runOnceWithClient processes create, update, then soft-delete", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;
      const calls: Array<unknown> = [];

      // evt_3's user carries updatedAt >= evt_2's so the delete projection is
      // not considered stale and actually sets deleted_at (mirrors
      // UserSync.isStaleDeletedProjection).
      const deletedAt = "2027-02-01T00:00:00.000Z";
      const data = [
        {
          id: "evt_1",
          event: "user.created",
          data: makeWorkOsUser({ id: "user_a" }),
          createdAt: "2026-01-01T00:00:00.000Z",
        },
        {
          id: "evt_2",
          event: "user.updated",
          data: makeWorkOsUser({
            id: "user_a",
            email: "new@x.com",
            updatedAt: "2027-01-01T00:00:00.000Z",
          }),
          createdAt: "2027-01-01T00:00:00.000Z",
        },
        {
          // The delete event carries its own full user snapshot, so the upsert
          // applies its payload too; keep the updated email here so the row
          // reflects update + soft-delete rather than reverting to the default.
          id: "evt_3",
          event: "user.deleted",
          data: makeWorkOsUser({ id: "user_a", email: "new@x.com", updatedAt: deletedAt }),
          createdAt: deletedAt,
        },
      ];

      const result = yield* WorkOsEvents.runOnceWithClient(db.client, makeWorkos(data, calls));
      assert.deepStrictEqual(result, { acquired: true, processed: 3, cursor: "evt_3" });
      assert.strictEqual(calls.length, 1);

      const sql = yield* PgClient.PgClient;
      const userRows = yield* sql<{
        email: string;
        deleted_at: string | null;
        workos_deleted_at: string | null;
      }>`select email, deleted_at, workos_deleted_at from users where workos_user_id = 'user_a'`;
      assert.strictEqual(userRows.length, 1);
      assert.strictEqual(userRows[0]!.email, "new@x.com");
      assert.strictEqual(userRows[0]!.deleted_at, deletedAt);
      assert.strictEqual(userRows[0]!.workos_deleted_at, deletedAt);

      assert.strictEqual(yield* WorkOsEvents.readCursor(db.client), "evt_3");

      // Lease released via Effect.ensuring after the run completes.
      const lockRows = yield* sql<{
        n: number;
      }>`select count(*)::int as n from workos_event_sync_locks`;
      assert.strictEqual(lockRows[0]!.n, 0);
    }),
  );

  it.effect("runOnceWithClient with no events processes nothing", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;
      const calls: Array<unknown> = [];

      const result = yield* WorkOsEvents.runOnceWithClient(db.client, makeWorkos([], calls));
      assert.strictEqual(result.acquired, true);
      assert.strictEqual(result.processed, 0);
      assert.strictEqual(result.cursor, null);
      assert.strictEqual(calls.length, 1);

      assert.isNull(yield* WorkOsEvents.readCursor(db.client));
    }),
  );

  it.effect("contention: a valid foreign lease blocks the run and skips listEvents", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;
      const sql = yield* PgClient.PgClient;

      const future = isoPlus(60 * 1000);
      yield* sql`insert into workos_event_sync_locks (name, owner, leased_until, updated_at)
        values ('users', 'other-worker', ${future}, ${future})`;

      const calls: Array<unknown> = [];
      const exploding = {
        events: {
          listEvents: async () => {
            throw new Error("listEvents must not be called under contention");
          },
        },
      } as unknown as WorkOsAuth.Interface["client"];

      const result = yield* WorkOsEvents.runOnceWithClient(db.client, exploding);
      assert.deepStrictEqual(result, { acquired: false, processed: 0, cursor: null });
      assert.strictEqual(calls.length, 0);

      // Foreign lease left intact (not released by the blocked run).
      const rows = yield* sql<{
        owner: string;
      }>`select owner from workos_event_sync_locks where name = 'users'`;
      assert.strictEqual(rows.length, 1);
      assert.strictEqual(rows[0]!.owner, "other-worker");
    }),
  );
});
