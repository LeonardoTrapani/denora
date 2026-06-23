import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { PiAgentModel } from "../agent-loop/PiAgentModel.ts";
import { PiRuntime } from "../agent-loop/PiRuntime.ts";
import {
  type EventStreamError,
  EventStorageFailed,
  makeSqliteEventStreamStore,
} from "./EventStreamStore.ts";
import { makeSqliteAgentRunCoordinator } from "./AgentRunCoordinator.ts";
import { AgentRunLifecycle, type CreateRunInput, type CreateRunResult } from "./Lifecycle.ts";
import { handleRunObjectRequest, internalErrorResponse } from "./StreamProtocol.ts";

export interface Shape {
  readonly create: (input: CreateRunInput) => Effect.Effect<CreateRunResult, EventStreamError>;
  readonly alarm: () => Effect.Effect<void, EventStreamError>;
}

export class AgentRunObject extends Cloudflare.DurableObjectNamespace<AgentRunObject, Shape>()(
  "AgentRunObject",
) {}

export const AiGateway = Cloudflare.AiGateway("DenoraAiGateway", {
  collectLogs: true,
});

export const AgentRunObjectLive = AgentRunObject.make(
  Effect.succeed(
    Effect.gen(function* () {
      const aiGateway = yield* Cloudflare.AiGateway.bind(AiGateway);
      const piLayer = PiRuntime.layer.pipe(
        Layer.provide(PiAgentModel.layer()),
        Layer.provide(PiAgentModel.aiGatewayLayerFromClient(aiGateway)),
      );

      return yield* Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        const pi = yield* PiRuntime.Service;
        // Stream storage is required for every request; initialization failure means
        // this Durable Object instance has unavailable or corrupt SQLite state.
        const store = yield* makeSqliteEventStreamStore(state.storage.sql).pipe(Effect.orDie);
        const coordinator = yield* makeSqliteAgentRunCoordinator(state.storage.sql, store).pipe(
          Effect.orDie,
        );

        return {
          create: (input: CreateRunInput) =>
            Effect.gen(function* () {
              const created = yield* AgentRunLifecycle.createRun(store, input);
              const admission = yield* coordinator.admitRun(input);
              if (created.created || admission.admitted) yield* scheduleRunAlarm(state);
              return created;
            }),
          alarm: () =>
            Effect.gen(function* () {
              const result = yield* coordinator.reconcile({
                pi,
                scheduleWake: (delayMs) => scheduleRunAlarm(state, delayMs),
              });
              if (result.needsWake) yield* scheduleRunAlarm(state, result.wakeDelayMs);
            }),
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
      }).pipe(Effect.provide(piLayer));
    }).pipe(Effect.provide(Cloudflare.AiGatewayBindingLive)),
  ),
);

const durableObjectStorageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

const scheduleRunAlarm = (
  state: Cloudflare.DurableObjectState["Service"],
  delayMs = 0,
): Effect.Effect<void, EventStorageFailed> =>
  state.storage
    .setAlarm(Date.now() + delayMs)
    .pipe(durableObjectStorageFailure("schedule agent run alarm"));

export * as AgentRunObjectModule from "./AgentRunObject.ts";
