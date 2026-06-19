import * as fs from "node:fs";
import * as path from "node:path";
import { fileURLToPath } from "node:url";
import * as PgClient from "@effect/sql-pg/PgClient";
import { PostgreSqlContainer, type StartedPostgreSqlContainer } from "@testcontainers/postgresql";
import * as PgDrizzle from "drizzle-orm/effect-postgres";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import { Db } from "../../src/persistence/Db.ts";

// One throwaway Postgres per suite, mirroring the Effect team's own
// `@testcontainers/postgresql` pattern (effect-smol packages/sql/pg/test).
export class PgContainer extends Context.Service<PgContainer, StartedPostgreSqlContainer>()(
  "test/PgContainer",
) {}

export const containerLayer: Layer.Layer<PgContainer> = Layer.effect(
  PgContainer,
  Effect.acquireRelease(
    Effect.promise(() => new PostgreSqlContainer("postgres:alpine").start()),
    (container) => Effect.promise(() => container.stop()),
  ),
);

const pgClientLayer = Layer.unwrap(
  Effect.gen(function* () {
    const container = yield* PgContainer;
    return PgClient.layer({ url: Redacted.make(container.getConnectionUri()) });
  }),
).pipe(Layer.provide(containerLayer));

const migrationsDir = fileURLToPath(new URL("../../migrations", import.meta.url));

// Drizzle emits one directory per migration with a `migration.sql` whose
// statements are separated by `--> statement-breakpoint`. We replay them
// directly rather than via drizzle's migrator, which expects the flat
// `meta/_journal.json` layout this project does not use.
const applyMigrations = Effect.gen(function* () {
  const sql = yield* PgClient.PgClient;
  const dirs = fs
    .readdirSync(migrationsDir, { withFileTypes: true })
    .filter((entry) => entry.isDirectory())
    .map((entry) => entry.name)
    .sort();

  for (const dir of dirs) {
    const file = path.join(migrationsDir, dir, "migration.sql");
    if (!fs.existsSync(file)) continue;

    const statements = fs
      .readFileSync(file, "utf8")
      .split("--> statement-breakpoint")
      .map((statement) => statement.trim())
      .filter((statement) => statement.length > 0);

    for (const statement of statements) {
      yield* sql.unsafe(statement);
    }
  }
});

// Provides `Db.Service` backed by a real effect-postgres client (the same
// `drizzle-orm/effect-postgres` integration production uses via alchemy), with
// the schema migrated. `provideMerge` keeps `PgClient`/`SqlClient` visible to
// tests for truncation and direct assertions.
export const dbLayer = Layer.effect(
  Db.Service,
  Effect.gen(function* () {
    yield* applyMigrations;
    const client = yield* PgDrizzle.makeWithDefaults();
    return Db.Service.of({ client });
  }),
).pipe(Layer.provideMerge(pgClientLayer));

export const truncateAll = Effect.gen(function* () {
  const sql = yield* PgClient.PgClient;
  const rows = yield* sql<{
    readonly tablename: string;
  }>`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`;

  if (rows.length === 0) return;

  const tables = rows.map((row) => `"${row.tablename.replaceAll('"', '""')}"`).join(", ");
  yield* sql.unsafe(`TRUNCATE TABLE ${tables} RESTART IDENTITY CASCADE`);
});

export * as Database from "./Database.ts";
