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

        return {
          create: (input: CreateRunInput) =>
            AgentRunLifecycle.startRun(store, {
              ...input,
              scheduleExecution: scheduleRunAlarm(state, input),
            }),
          alarm: () =>
            Effect.gen(function* () {
              const scheduled = yield* state.storage
                .get<CreateRunInput>(RUN_ALARM_STORAGE_KEY)
                .pipe(durableObjectStorageFailure("read scheduled agent run input"));
              if (scheduled === undefined) {
                yield* Effect.logError("agent run alarm fired without persisted run input");
                return;
              }
              yield* AgentRunLifecycle.executeRun(store, { ...scheduled, pi });
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

const RUN_ALARM_STORAGE_KEY = "agent-run/alarm-input";

const durableObjectStorageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

const scheduleRunAlarm = (
  state: Cloudflare.DurableObjectState["Service"],
  input: CreateRunInput,
): ((runId: string) => Effect.Effect<void, EventStorageFailed>) =>
  Effect.fn("AgentRunObject.scheduleRunAlarm")(function* () {
    yield* state.storage
      .put(RUN_ALARM_STORAGE_KEY, input)
      .pipe(durableObjectStorageFailure("persist scheduled agent run input"));
    yield* state.storage
      .setAlarm(Date.now())
      .pipe(durableObjectStorageFailure("schedule agent run alarm"));
  });

export * as AgentRunObjectModule from "./AgentRunObject.ts";
