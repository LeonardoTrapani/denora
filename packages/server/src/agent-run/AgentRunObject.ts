import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { type EventStreamError, makeSqliteEventStreamStore } from "./EventStreamStore.ts";
import { AgentRunLifecycle, type CreateRunInput, type CreateRunResult } from "./Lifecycle.ts";
import { handleRunObjectRequest, internalErrorResponse } from "./StreamProtocol.ts";

export interface Shape {
  readonly create: (input: CreateRunInput) => Effect.Effect<CreateRunResult, EventStreamError>;
}

export class AgentRunObject extends Cloudflare.DurableObjectNamespace<AgentRunObject, Shape>()(
  "AgentRunObject",
) {}

export const AgentRunObjectLive = AgentRunObject.make(
  Effect.succeed(
    Effect.gen(function* () {
      const state = yield* Cloudflare.DurableObjectState;
      // Stream storage is required for every request; initialization failure means
      // this Durable Object instance has unavailable or corrupt SQLite state.
      const store = yield* makeSqliteEventStreamStore(state.storage.sql).pipe(Effect.orDie);

      return {
        create: (input: CreateRunInput) => AgentRunLifecycle.createRun(store, input),
        fetch: Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          const webRequest = yield* HttpServerRequest.toWeb(request);
          const response = yield* handleRunObjectRequest(store, webRequest).pipe(
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                const traceId = crypto.randomUUID();
                yield* Effect.logError("agent run object stream request failed", {
                  traceId,
                  cause,
                });
                return internalErrorResponse(traceId);
              }),
            ),
          );
          return HttpServerResponse.fromWeb(response);
        }),
      };
    }),
  ),
);

export * as AgentRunObjectModule from "./AgentRunObject.ts";
