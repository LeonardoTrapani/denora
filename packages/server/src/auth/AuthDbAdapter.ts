import type { BetterAuthOptions } from "better-auth";
import {
  type AdapterFactoryConfig,
  type AdapterFactoryCustomizeAdapterCreator,
  createAdapterFactory,
} from "better-auth/adapters";
import {
  and,
  asc,
  type Column,
  count as countRows,
  desc,
  eq,
  gt,
  gte,
  ilike,
  inArray,
  isNotNull,
  isNull,
  like,
  lt,
  lte,
  ne,
  notInArray,
  or,
  type SQL,
  sql,
} from "drizzle-orm";
import * as Effect from "effect/Effect";
import type { Db } from "../persistence/Db.ts";
import { AuthRequestContext } from "./AuthRequestContext.ts";

/**
 * A custom better-auth adapter that runs against the alchemy-managed
 * effect-postgres `Db` client instead of opening its own connection pool.
 *
 * Why not `@better-auth/drizzle-adapter`? It needs `drizzle-orm@^0.45` and a
 * Promise-returning drizzle, while this repo is on `drizzle-orm@1.0-rc` with the
 * `drizzle-orm/effect-postgres` integration (query builders return Effects). The
 * official adapter is also confirmed broken on drizzle v1. Reusing the existing
 * `Db` client keeps a single Hyperdrive connection managed by alchemy's
 * per-request scope — no second pool to lifecycle on Workers.
 *
 * The query translation mirrors the official drizzle adapter's operator mapping,
 * specialized to a single Postgres dialect, with each query Effect run on the
 * request context captured in `AuthRequestContext`.
 */

// The generated better-auth drizzle tables, keyed by model name (`user`,
// `session`, `account`, `verification`, …). Inherently dynamic — model and field
// names arrive as strings — so columns are indexed dynamically, as in the
// official adapter.
type AuthTable = Record<string, unknown>;
export type AuthTableSchema = Record<string, AuthTable>;

// The effect-postgres client is reached through alchemy's dynamic proxy, so the
// drizzle query builder is described structurally with just the chains this
// adapter uses; each terminal node is an Effect we run via `run`. Row data is
// `any` at this boundary (as in the official adapter) — better-auth re-types it.
// The query requirement is `never` (as alchemy types its proxy queries);
// `ExecutionContext` is supplied at runtime by the captured request context.
type Query = Effect.Effect<any, unknown, never>;
type WithReturning = Query & { returning: () => Query };
type DynamicSelect = Query & {
  where: (condition: SQL) => DynamicSelect;
  orderBy: (column: SQL) => DynamicSelect;
  limit: (n: number) => DynamicSelect;
  offset: (n: number) => DynamicSelect;
};
interface DrizzleClient {
  insert: (table: unknown) => { values: (data: unknown) => { returning: () => Query } };
  update: (table: unknown) => {
    set: (data: unknown) => { where: (condition: SQL | undefined) => WithReturning };
  };
  delete: (table: unknown) => { where: (condition: SQL | undefined) => WithReturning };
  select: (projection?: Record<string, unknown>) => {
    from: (table: unknown) => Query & {
      where: (condition: SQL | undefined) => Query & { limit: (n: number) => Query };
      $dynamic: () => DynamicSelect;
    };
  };
}

// Mirrors better-auth's `CleanedWhere` (`Required<Where>`), whose property types
// still include `undefined`. Kept local to avoid importing a better-auth
// internal type path.
type WhereOperator =
  | "eq"
  | "ne"
  | "lt"
  | "lte"
  | "gt"
  | "gte"
  | "in"
  | "not_in"
  | "contains"
  | "starts_with"
  | "ends_with";
interface CleanWhere {
  operator: WhereOperator | undefined;
  value: string | number | boolean | string[] | number[] | Date | null;
  field: string;
  connector: "AND" | "OR" | undefined;
  mode: "sensitive" | "insensitive" | undefined;
}

const run = (operation: string, model: string, query: Query): Promise<any> =>
  Effect.runPromiseWith(AuthRequestContext.current())(
    Effect.gen(function* () {
      const startedAt = Date.now();
      yield* Effect.logInfo("better-auth adapter query started", { model, operation });
      const result = yield* query;
      yield* Effect.logInfo("better-auth adapter query finished", {
        durationMs: Date.now() - startedAt,
        model,
        operation,
        rows: Array.isArray(result) ? result.length : undefined,
      });
      return result;
    }),
  );

const asRows = (value: unknown): any[] => (Array.isArray(value) ? value : []);

const isStringValue = (value: unknown): value is string => typeof value === "string";
const isStringArray = (value: unknown): value is string[] =>
  Array.isArray(value) && value.every(isStringValue);

export const makeAuthDbAdapter = (client: Db.Client, tableSchema: AuthTableSchema) => {
  const db = client as unknown as DrizzleClient;

  // The CRUD creator, parameterized over a handle so the same logic can be used
  // with any compatible drizzle/effect-postgres client handle.
  const makeCreator =
    (handle: DrizzleClient): AdapterFactoryCustomizeAdapterCreator =>
    ({ getFieldName }) => {
      const getModel = (model: string): AuthTable => {
        const table = tableSchema[model];
        if (table === undefined) {
          throw new Error(`[Denora Auth Adapter] model "${model}" was not found in the schema`);
        }
        return table;
      };

      const getColumn = (model: string, field: string): Column => {
        const column = getModel(model)[getFieldName({ model, field })];
        if (column === undefined) {
          throw new Error(
            `[Denora Auth Adapter] field "${field}" was not found on model "${model}"`,
          );
        }
        return column as Column;
      };

      const toCondition = (model: string, where: CleanWhere): SQL => {
        const column = getColumn(model, where.field);
        const { operator, value, mode } = where;
        const insensitive =
          mode === "insensitive" && (isStringValue(value) || isStringArray(value));

        switch (operator) {
          case "in":
            if (!Array.isArray(value)) {
              throw new Error(`[Denora Auth Adapter] "in" on "${where.field}" needs an array`);
            }
            return inArray(column, value);
          case "not_in":
            if (!Array.isArray(value)) {
              throw new Error(`[Denora Auth Adapter] "not_in" on "${where.field}" needs an array`);
            }
            return notInArray(column, value);
          case "contains":
            return insensitive
              ? ilike(column, `%${String(value)}%`)
              : like(column, `%${String(value)}%`);
          case "starts_with":
            return insensitive
              ? ilike(column, `${String(value)}%`)
              : like(column, `${String(value)}%`);
          case "ends_with":
            return insensitive
              ? ilike(column, `%${String(value)}`)
              : like(column, `%${String(value)}`);
          case "lt":
            return lt(column, value);
          case "lte":
            return lte(column, value);
          case "gt":
            return gt(column, value);
          case "gte":
            return gte(column, value);
          case "ne":
            if (value === null) return isNotNull(column);
            return insensitive ? sql`lower(${column}) <> lower(${value})` : ne(column, value);
          default:
            if (value === null) return isNull(column);
            return insensitive ? sql`lower(${column}) = lower(${value})` : eq(column, value);
        }
      };

      const buildWhere = (
        model: string,
        where: readonly CleanWhere[] | undefined,
      ): SQL | undefined => {
        if (where === undefined || where.length === 0) return undefined;
        if (where.length === 1) return toCondition(model, where[0]!);

        const ands = where
          .filter((clause) => (clause.connector ?? "AND") === "AND")
          .map((clause) => toCondition(model, clause));
        const ors = where
          .filter((clause) => clause.connector === "OR")
          .map((clause) => toCondition(model, clause));

        const parts: SQL[] = [];
        if (ands.length > 0) parts.push(and(...ands)!);
        if (ors.length > 0) parts.push(or(...ors)!);
        return parts.length === 1 ? parts[0]! : and(...parts)!;
      };

      const projection = (model: string, select: string[] | undefined) => {
        if (select === undefined || select.length === 0) return undefined;
        const fields: Record<string, unknown> = {};
        for (const field of select)
          fields[getFieldName({ model, field })] = getColumn(model, field);
        return fields;
      };

      return {
        create: async ({ model, data }) => {
          const rows = asRows(
            await run("create", model, handle.insert(getModel(model)).values(data).returning()),
          );
          return rows[0];
        },

        findOne: async ({ model, where, select }) => {
          const rows = asRows(
            await run(
              "findOne",
              model,
              handle
                .select(projection(model, select))
                .from(getModel(model))
                .where(buildWhere(model, where))
                .limit(1),
            ),
          );
          return rows[0] ?? null;
        },

        findMany: async ({ model, where, limit, select, sortBy, offset }) => {
          let query = handle.select(projection(model, select)).from(getModel(model)).$dynamic();
          const condition = buildWhere(model, where);
          if (condition !== undefined) query = query.where(condition);
          if (sortBy !== undefined) {
            const direction = sortBy.direction === "desc" ? desc : asc;
            query = query.orderBy(direction(getColumn(model, sortBy.field)));
          }
          if (limit !== undefined) query = query.limit(limit);
          if (offset !== undefined) query = query.offset(offset);
          return asRows(await run("findMany", model, query));
        },

        count: async ({ model, where }) => {
          const rows = asRows(
            await run(
              "count",
              model,
              handle
                .select({ value: countRows() })
                .from(getModel(model))
                .where(buildWhere(model, where)),
            ),
          );
          return Number(rows[0]?.value ?? 0);
        },

        update: async ({ model, where, update }) => {
          const rows = asRows(
            await run(
              "update",
              model,
              handle
                .update(getModel(model))
                .set(update)
                .where(buildWhere(model, where))
                .returning(),
            ),
          );
          return rows[0] ?? null;
        },

        updateMany: async ({ model, where, update }) => {
          const rows = asRows(
            await run(
              "updateMany",
              model,
              handle
                .update(getModel(model))
                .set(update)
                .where(buildWhere(model, where))
                .returning(),
            ),
          );
          return rows.length;
        },

        delete: async ({ model, where }) => {
          await run(
            "delete",
            model,
            handle.delete(getModel(model)).where(buildWhere(model, where)),
          );
        },

        deleteMany: async ({ model, where }) => {
          const rows = asRows(
            await run(
              "deleteMany",
              model,
              handle.delete(getModel(model)).where(buildWhere(model, where)).returning(),
            ),
          );
          return rows.length;
        },
      };
    };

  const baseConfig = {
    adapterId: "denora-effect-postgres",
    adapterName: "Denora Effect Postgres Adapter",
    supportsUUIDs: true,
    supportsJSON: true,
    supportsArrays: true,
    supportsDates: true,
    supportsBooleans: true,
    customTransformOutput: ({ data, fieldAttributes }) =>
      fieldAttributes.type === "date" && data !== null && data !== undefined
        ? new Date(data as string | number | Date)
        : data,
    // Alchemy's effect-postgres transaction state is fiber-local. Better Auth's
    // transaction callback is Promise-based, so replaying query Effects through a
    // separate runPromise boundary can lose that transaction state and hang OAuth
    // callbacks. Let Better Auth run those multi-step operations sequentially.
    transaction: false,
  } satisfies AdapterFactoryConfig;

  const factory = createAdapterFactory({
    config: baseConfig,
    adapter: makeCreator(db),
  });

  return (options: BetterAuthOptions) => factory(options);
};

export * as AuthDbAdapter from "./AuthDbAdapter.ts";
