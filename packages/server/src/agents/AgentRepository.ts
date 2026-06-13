import { and, asc, eq } from "drizzle-orm";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { schema } from "../persistence/schema.ts";

declare const crypto: { randomUUID(): string };

export type Agent = typeof schema.agents.$inferSelect;
export type DbClient = any;

export interface CreateInput {
  readonly userId: string;
  readonly name: string;
  readonly handle: string;
}

export interface Interface {
  readonly listForUser: (userId: string) => Effect.Effect<ReadonlyArray<Agent>>;
  readonly createForUser: (input: CreateInput) => Effect.Effect<Agent, HandleTaken>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/AgentRepository",
) {}

export class HandleTaken extends Schema.TaggedErrorClass<HandleTaken>()("AgentHandleTaken", {
  handle: Schema.String,
}) {}

export const layer = (db: DbClient): Layer.Layer<Service> =>
  Layer.succeed(
    Service,
    Service.of({
      listForUser: (userId) =>
        db
          .select()
          .from(schema.agents)
          .where(eq(schema.agents.userId, userId))
          .orderBy(asc(schema.agents.createdAt))
          .pipe(Effect.orDie) as Effect.Effect<ReadonlyArray<Agent>>,

      createForUser: (input) =>
        Effect.gen(function* () {
          const existing = yield* db
            .select({ id: schema.agents.id })
            .from(schema.agents)
            .where(
              and(eq(schema.agents.userId, input.userId), eq(schema.agents.handle, input.handle)),
            )
            .limit(1)
            .pipe(Effect.orDie) as Effect.Effect<ReadonlyArray<{ readonly id: string }>>;

          if (existing.length > 0) {
            return yield* new HandleTaken({ handle: input.handle });
          }

          const agent = {
            id: crypto.randomUUID(),
            userId: input.userId,
            name: input.name,
            handle: input.handle,
            createdAt: new Date().toISOString(),
          } satisfies Agent;

          yield* db.insert(schema.agents).values(agent).pipe(Effect.orDie) as Effect.Effect<void>;

          return agent;
        }),
    }),
  );

export * as AgentRepository from "./AgentRepository.ts";
