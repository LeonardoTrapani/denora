import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { PiAgentModel } from "../agent-loop/PiAgentModel.ts";
import { PiRuntime } from "../agent-loop/PiRuntime.ts";
import { Db } from "../persistence/Db.ts";
import { AgentRunPersistence } from "./AgentRunPersistence.ts";
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
        const persistence = yield* AgentRunPersistence.Service;
        // Stream storage is required for every request; initialization failure means
        // this Durable Object instance has unavailable or corrupt SQLite state.
        const sqliteStore = yield* makeSqliteEventStreamStore(state.storage.sql).pipe(Effect.orDie);
        const store = AgentRunPersistence.mirrorEventStreamStore(sqliteStore, persistence);

        return {
          create: (input: CreateRunInput) =>
            AgentRunLifecycle.startRun(store, {
              ...input,
              scheduleExecution: scheduleRunAlarm(state, input.runId),
            }),
          alarm: () =>
            Effect.gen(function* () {
              const runId = yield* state.storage
                .get<string>(RUN_ID_STORAGE_KEY)
                .pipe(durableObjectStorageFailure("read scheduled agent run id"));
              if (runId === undefined) {
                yield* Effect.logError("agent run alarm fired without a persisted run id");
                return;
              }
              const input = yield* persistence.getRunInput(runId).pipe(
                Effect.catch((error) =>
                  Effect.gen(function* () {
                    yield* Effect.logError("agent run input recovery failed", { runId, error });
                    return undefined;
                  }),
                ),
              );
              yield* AgentRunLifecycle.executeRun(store, { runId, input, pi });
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
      }).pipe(
        Effect.provide(
          Layer.mergeAll(
            piLayer,
            AgentRunPersistence.layer.pipe(Layer.provide(Db.hyperdriveLayer)),
          ),
        ),
      );
    }).pipe(Effect.provide(Cloudflare.AiGatewayBindingLive)),
  ),
);

const RUN_ID_STORAGE_KEY = "agent-run/run-id";

const durableObjectStorageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

const scheduleRunAlarm = (
  state: Cloudflare.DurableObjectState["Service"],
  runId: string,
): (() => Effect.Effect<void, EventStorageFailed>) =>
  Effect.fn("AgentRunObject.scheduleRunAlarm")(function* () {
    yield* state.storage
      .put(RUN_ID_STORAGE_KEY, runId)
      .pipe(durableObjectStorageFailure("persist scheduled agent run id"));
    yield* state.storage
      .setAlarm(Date.now())
      .pipe(durableObjectStorageFailure("schedule agent run alarm"));
  });

export * as AgentRunObjectModule from "./AgentRunObject.ts";
