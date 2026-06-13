import * as Cloudflare from "alchemy/Cloudflare";
import * as Drizzle from "alchemy/Drizzle";
import { and, asc, eq } from "drizzle-orm";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Headers from "effect/unstable/http/Headers";
import * as Etag from "effect/unstable/http/Etag";
import * as HttpPlatform from "effect/unstable/http/HttpPlatform";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { Agent, AgentHandleTaken, AgentList, DenoraApi, Health } from "./Api.ts";
import { DenoraHyperdrive } from "./Db.ts";
import * as schema from "./schema.ts";

declare const crypto: { randomUUID(): string };

const DevelopmentUserId = "dev-user";
const DevelopmentUserHeader = "x-denora-dev-user-id";

type AgentRow = typeof schema.agents.$inferSelect;

const getCurrentUserId = Effect.gen(function* () {
  const request = yield* HttpServerRequest.HttpServerRequest;

  return Headers.get(request.headers, DevelopmentUserHeader).pipe(
    Option.getOrElse(() => DevelopmentUserId),
  );
});

const toAgent = (row: AgentRow) =>
  new Agent({
    id: row.id,
    userId: row.userId,
    name: row.name,
    handle: row.handle,
    createdAt: row.createdAt,
  });

export default class Server extends Cloudflare.Worker<Server>()(
  "Server",
  {
    main: import.meta.filename,
    compatibility: {
      flags: ["nodejs_compat"],
    },
  },
  Effect.gen(function* () {
    const hyperdrive = yield* Cloudflare.Hyperdrive.bind(DenoraHyperdrive);
    const db = yield* Drizzle.postgres(hyperdrive.connectionString);

    const systemHandlers = HttpApiBuilder.group(DenoraApi, "System", (handlers) =>
      handlers.handle("health", () => Effect.succeed(new Health({ status: "ok" }))),
    );

    const agentHandlers = HttpApiBuilder.group(DenoraApi, "Agents", (handlers) =>
      handlers
        .handle("listAgents", () =>
          Effect.gen(function* () {
            const userId = yield* getCurrentUserId;
            const rows = yield* db
              .select()
              .from(schema.agents)
              .where(eq(schema.agents.userId, userId))
              .orderBy(asc(schema.agents.createdAt))
              .pipe(Effect.orDie);

            return new AgentList({ agents: rows.map(toAgent) });
          }),
        )
        .handle("createAgent", ({ payload }) =>
          Effect.gen(function* () {
            const userId = yield* getCurrentUserId;
            const existing = yield* db
              .select({ id: schema.agents.id })
              .from(schema.agents)
              .where(
                and(eq(schema.agents.userId, userId), eq(schema.agents.handle, payload.handle)),
              )
              .limit(1)
              .pipe(Effect.orDie);

            if (existing.length > 0) {
              return yield* new AgentHandleTaken({ handle: payload.handle });
            }

            const agent = new Agent({
              id: crypto.randomUUID(),
              userId,
              name: payload.name,
              handle: payload.handle,
              createdAt: new Date().toISOString(),
            });

            yield* db.insert(schema.agents).values(agent).pipe(Effect.orDie);

            return agent;
          }),
        ),
    );

    return {
      fetch: HttpApiBuilder.layer(DenoraApi).pipe(
        Layer.provide(systemHandlers),
        Layer.provide(agentHandlers),
        Layer.provide([HttpPlatform.layer, Etag.layer]),
        Layer.provide(
          HttpRouter.cors({
            allowedOrigins: ["*"],
            allowedMethods: ["GET", "POST", "OPTIONS"],
            allowedHeaders: ["Content-Type", "Authorization", DevelopmentUserHeader],
          }),
        ),
        HttpRouter.toHttpEffect,
      ),
    };
  }).pipe(Effect.provide(Cloudflare.HyperdriveBindingLive)),
) {}
