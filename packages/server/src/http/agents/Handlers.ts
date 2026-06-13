import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { AgentRepository } from "../../agents/AgentRepository.ts";
import { Authorization } from "../../auth/Authorization.ts";
import { DenoraApi } from "../Api.ts";
import { Agent, AgentList } from "./Schema.ts";

const toAgent = (row: AgentRepository.Agent) =>
  new Agent({
    id: row.id,
    userId: row.userId,
    name: row.name,
    handle: row.handle,
    createdAt: row.createdAt,
  });

export const layer = HttpApiBuilder.group(
  DenoraApi,
  "Agents",
  Effect.fn("@denora/server/AgentsHandlers")(function* (handlers) {
    const agents = yield* AgentRepository.Service;

    return handlers
      .handle("listAgents", () =>
        Effect.gen(function* () {
          const user = yield* Authorization.CurrentUser;
          const rows = yield* agents.listForUser(user.id);

          return new AgentList({ agents: rows.map(toAgent) });
        }),
      )
      .handle("createAgent", ({ payload }) =>
        Effect.gen(function* () {
          const user = yield* Authorization.CurrentUser;
          const agent = yield* agents.createForUser({
            userId: user.id,
            name: payload.name,
            handle: payload.handle,
          });

          return toAgent(agent);
        }),
      );
  }),
);

export * as AgentsHandlers from "./Handlers.ts";
