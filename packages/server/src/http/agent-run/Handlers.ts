import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as HttpApiBuilder from "effect/unstable/httpapi/HttpApiBuilder";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import { AgentRuns } from "../../agent-run/AgentRuns.ts";
import { AuthorizationApi } from "../../auth/AuthorizationApi.ts";
import { DenoraApi } from "../Api.ts";
import { CreateAgentRunResponse } from "./Api.ts";

export const layer = HttpApiBuilder.group(DenoraApi, "AgentRun", (handlers) =>
  handlers.handle("createAgentRun", ({ payload }) =>
    Effect.gen(function* () {
      const request = yield* HttpServerRequest.HttpServerRequest;
      const user = yield* AuthorizationApi.CurrentUser;
      const agentRuns = yield* AgentRuns.Service;
      const runId = payload.runId ?? `run_${crypto.randomUUID()}`;
      const created = yield* agentRuns.create({
        runId,
        userId: user.id,
        conversationId: payload.conversationId,
        triggerMessageId: payload.triggerMessageId,
        input: payload.input,
      });

      return createAgentRunResponse({
        runId,
        streamUrl: streamUrl(request, runId),
        streamPath: created.streamPath,
        offset: created.offset,
      });
    }),
  ),
);

const createAgentRunResponse = Schema.decodeUnknownSync(CreateAgentRunResponse);

const streamUrl = (request: HttpServerRequest.HttpServerRequest, runId: string): string => {
  const path = `/runs/${encodeURIComponent(runId)}`;
  const url = HttpServerRequest.toURL(request);
  if (Option.isNone(url)) return path;
  const next = new URL(url.value.toString());
  next.pathname = path;
  next.search = "";
  return next.toString();
};

export * as AgentRunHandlers from "./Handlers.ts";
