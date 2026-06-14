import { assert, layer } from "@effect/vitest";
import * as PgClient from "@effect/sql-pg/PgClient";
import * as Effect from "effect/Effect";
import { UserSync } from "../../src/auth/UserSync.ts";
import { Db } from "../../src/persistence/Db.ts";
import { Database } from "../helpers/Database.ts";
import { makeWorkOsUser } from "../helpers/fixtures.ts";

interface UserRow {
  readonly id: string;
  readonly workos_user_id: string;
  readonly email: string;
  readonly email_verified: boolean;
  readonly name: string | null;
  readonly first_name: string | null;
  readonly last_name: string | null;
  readonly profile_picture_url: string | null;
  readonly locale: string | null;
  readonly last_sign_in_at: string | null;
  readonly workos_created_at: string;
  readonly workos_updated_at: string;
  readonly deleted_at: string | null;
  readonly workos_deleted_at: string | null;
  readonly created_at: string;
  readonly updated_at: string;
}

const readRows = Effect.fn(function* (workosUserId: string) {
  const sql = yield* PgClient.PgClient;
  return yield* sql<UserRow>`select * from users where workos_user_id = ${workosUserId}`;
});

const countUsers = Effect.fn(function* () {
  const sql = yield* PgClient.PgClient;
  const rows = yield* sql<{ readonly n: number }>`select count(*)::int as n from users`;
  return rows[0]?.n ?? 0;
});

layer(Database.dbLayer)("UserSync", (it) => {
  it.effect(
    "syncUser inserts a new row and maps WorkOS fields onto a DenoraUser with a uuid id",
    () =>
      Effect.gen(function* () {
        yield* Database.truncateAll;
        const db = yield* Db.Service;

        const workosUser = makeWorkOsUser({
          id: "user_insert",
          email: "ada@example.com",
          emailVerified: true,
          name: "Ada Lovelace",
          firstName: "Ada",
          lastName: "Lovelace",
          createdAt: "2026-01-01T00:00:00.000Z",
          updatedAt: "2026-01-01T00:00:00.000Z",
        });

        const user = yield* UserSync.syncUser(db.client, workosUser);

        assert.strictEqual(user.workosUserId, "user_insert");
        assert.strictEqual(user.email, "ada@example.com");
        assert.strictEqual(user.emailVerified, true);
        assert.strictEqual(user.name, "Ada Lovelace");
        assert.strictEqual(user.firstName, "Ada");
        assert.strictEqual(user.lastName, "Lovelace");
        assert.strictEqual(user.profilePictureUrl, null);
        assert.strictEqual(user.locale, null);

        // A generated uuid id (v4 layout), not the WorkOS id.
        assert.notStrictEqual(user.id, "user_insert");
        assert.match(user.id, /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

        assert.strictEqual(yield* countUsers(), 1);

        const rows = yield* readRows("user_insert");
        assert.strictEqual(rows.length, 1);
        const row = rows[0]!;
        assert.strictEqual(row.id, user.id);
        assert.strictEqual(row.email, "ada@example.com");
        assert.strictEqual(row.workos_created_at, "2026-01-01T00:00:00.000Z");
        assert.strictEqual(row.workos_updated_at, "2026-01-01T00:00:00.000Z");
        // syncUser is the active-projection path: deletion columns must be cleared.
        assert.strictEqual(row.deleted_at, null);
        assert.strictEqual(row.workos_deleted_at, null);
      }),
  );

  it.effect("syncUser with a newer updatedAt updates in place (count stays 1, id unchanged)", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      const first = yield* UserSync.syncUser(
        db.client,
        makeWorkOsUser({
          id: "user_update",
          email: "old@example.com",
          updatedAt: "2026-01-01T00:00:00.000Z",
        }),
      );

      const second = yield* UserSync.syncUser(
        db.client,
        makeWorkOsUser({
          id: "user_update",
          email: "new@example.com",
          updatedAt: "2026-02-01T00:00:00.000Z",
        }),
      );

      assert.strictEqual(yield* countUsers(), 1);
      // Upsert keeps the original primary key while updating mutable fields.
      assert.strictEqual(second.id, first.id);
      assert.strictEqual(second.email, "new@example.com");

      const rows = yield* readRows("user_update");
      assert.strictEqual(rows.length, 1);
      const row = rows[0]!;
      assert.strictEqual(row.email, "new@example.com");
      assert.strictEqual(row.workos_updated_at, "2026-02-01T00:00:00.000Z");
    }),
  );

  it.effect(
    "syncUser with an older updatedAt is a stale projection: existing row is preserved",
    () =>
      Effect.gen(function* () {
        yield* Database.truncateAll;
        const db = yield* Db.Service;

        const current = yield* UserSync.syncUser(
          db.client,
          makeWorkOsUser({
            id: "user_stale",
            email: "feb@example.com",
            updatedAt: "2026-02-01T00:00:00.000Z",
          }),
        );

        const staleWorkosUser = makeWorkOsUser({
          id: "user_stale",
          email: "jan@example.com",
          updatedAt: "2026-01-01T00:00:00.000Z",
        });

        // Sanity check the staleness predicate directly. The drizzle-typed row
        // shape is camelCase, so build it explicitly (raw PgClient rows are
        // snake_case and would not satisfy isStaleActiveProjection).
        assert.isTrue(
          UserSync.isStaleActiveProjection(
            {
              workosUpdatedAt: "2026-02-01T00:00:00.000Z",
              workosDeletedAt: null,
            } as Parameters<typeof UserSync.isStaleActiveProjection>[0],
            staleWorkosUser,
          ),
        );

        const result = yield* UserSync.syncUser(db.client, staleWorkosUser);

        // Stale path returns the existing projection unchanged.
        assert.strictEqual(result.id, current.id);
        assert.strictEqual(result.email, "feb@example.com");

        assert.strictEqual(yield* countUsers(), 1);
        const rows = yield* readRows("user_stale");
        const row = rows[0]!;
        assert.strictEqual(row.email, "feb@example.com");
        assert.strictEqual(row.workos_updated_at, "2026-02-01T00:00:00.000Z");
      }),
  );

  it.effect("syncDeletedUser sets deleted_at and workos_deleted_at to deletedAt", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      const workosUser = makeWorkOsUser({
        id: "user_delete",
        email: "gone@example.com",
        updatedAt: "2026-01-01T00:00:00.000Z",
      });

      yield* UserSync.syncUser(db.client, workosUser);

      const deletedAt = "2026-03-01T00:00:00.000Z";
      yield* UserSync.syncDeletedUser(db.client, workosUser, deletedAt);

      assert.strictEqual(yield* countUsers(), 1);
      const rows = yield* readRows("user_delete");
      const row = rows[0]!;
      assert.strictEqual(row.deleted_at, deletedAt);
      assert.strictEqual(row.workos_deleted_at, deletedAt);
    }),
  );

  it.effect("syncUser is idempotent: syncing an identical user twice keeps count at 1", () =>
    Effect.gen(function* () {
      yield* Database.truncateAll;
      const db = yield* Db.Service;

      const workosUser = makeWorkOsUser({
        id: "user_idem",
        email: "same@example.com",
        updatedAt: "2026-01-01T00:00:00.000Z",
      });

      const first = yield* UserSync.syncUser(db.client, workosUser);
      const second = yield* UserSync.syncUser(db.client, workosUser);

      assert.strictEqual(yield* countUsers(), 1);
      assert.strictEqual(second.id, first.id);
      assert.strictEqual(second.email, "same@example.com");
    }),
  );
});
