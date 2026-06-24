import { DatabaseSync } from "node:sqlite";
import type * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

export interface TestSqliteStorage {
  readonly sql: Cloudflare.SqlStorage;
  readonly close: () => void;
}

export const makeSqliteStorage = (): TestSqliteStorage => {
  const db = new DatabaseSync(":memory:");
  const sql = {
    raw: db as unknown,
    databaseSize: 0,
    exec<T extends Record<string, Cloudflare.SqlStorageValue>>(
      query: string,
      ...bindings: Array<unknown>
    ) {
      return Effect.sync(() => {
        const statement = db.prepare(query);
        const all = statement.all as (
          this: typeof statement,
          ...values: Array<unknown>
        ) => Array<T>;
        const rows = all.call(statement, ...bindings);
        let cursor = 0;
        return {
          columnNames: rows[0] === undefined ? [] : Object.keys(rows[0]),
          rowsRead: Effect.succeed(rows.length),
          rowsWritten: Effect.succeed(0),
          next: () =>
            Effect.sync(() => {
              const value = rows[cursor++];
              return value === undefined
                ? { done: true as const }
                : { done: false as const, value };
            }),
          toArray: () => Effect.succeed(rows),
          one: () =>
            Effect.sync(() => {
              const row = rows[0];
              if (row === undefined) throw new Error("Expected one SQLite row.");
              return row;
            }),
          raw: () => {
            throw new Error("raw SQLite cursor reads are not implemented in tests.");
          },
        } as unknown as Cloudflare.SqlCursor<T>;
      });
    },
  } as Cloudflare.SqlStorage;

  return { sql, close: () => db.close() };
};

export class Service extends Context.Service<Service, TestSqliteStorage>()("test/SqliteStorage") {}

export const layer: Layer.Layer<Service> = Layer.effect(
  Service,
  Effect.acquireRelease(Effect.sync(makeSqliteStorage), (storage) =>
    Effect.sync(() => storage.close()),
  ).pipe(Effect.map(Service.of)),
);

export * as SqliteStorage from "./SqliteStorage.ts";
