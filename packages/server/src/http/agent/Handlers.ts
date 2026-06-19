import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { Unauthorized } from "../../auth/User.ts";
import { AgentThreads } from "../../agent/Threads.ts";
import { DenoraApi } from "../Api.ts";

export const layer = HttpApiBuilder.group(
  DenoraApi,
  "Agent",
  Effect.fnUntraced(function* (handlers) {
    const threads = yield* AgentThreads.Service;

    return handlers.handle("sendAgentMessage", ({ params, payload }) =>
      Effect.gen(function* () {
        const user = yield* AuthorizationApi.CurrentUser;
        if (params.agentId !== user.id) {
          return yield* new Unauthorized({ message: "Agent does not belong to current user" });
        }

        return yield* threads.send({
          agentId: params.agentId,
          threadId: params.threadId,
          message: payload.message,
        });
      }),
    );
  }),
);

export * as AgentHandlers from "./Handlers.ts";
