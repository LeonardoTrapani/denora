import * as Cloudflare from "alchemy/Cloudflare";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { PiAgentModel } from "../agent-loop/PiAgentModel.ts";
import { PiRuntime } from "../agent-loop/PiRuntime.ts";
import { type EventStreamError, EventStorageFailed, EventStreamStore } from "./EventStreamStore.ts";
import { AgentConversationSessionStore } from "./AgentConversationSessionStore.ts";
import {
  AgentConversationCoordinator,
  type AbortConversationResult,
  type ConversationLifecycleState,
  type Interface as AgentConversationCoordinatorInterface,
} from "./AgentConversationCoordinator.ts";
import {
  AgentRunLifecycle,
  type CreateConversationSubmissionInput,
  type CreateConversationSubmissionResult,
} from "./Lifecycle.ts";
import { SqlStorage } from "./SqlStorage.ts";
import { handleConversationObjectRequest, internalErrorResponse } from "./StreamProtocol.ts";

export interface Shape {
  readonly abortConversation: (input?: {
    readonly reason?: string | undefined;
  }) => Effect.Effect<AbortConversationResult, EventStreamError>;
  readonly setConversationLifecycle: (input: {
    readonly conversationId: string;
    readonly status: ConversationLifecycleState;
  }) => Effect.Effect<AbortConversationResult, EventStreamError>;
  readonly submitMessage: (
    input: CreateConversationSubmissionInput,
  ) => Effect.Effect<
    CreateConversationSubmissionResult & { readonly result?: unknown },
    EventStreamError
  >;
  readonly alarm: () => Effect.Effect<void, EventStreamError>;
}

export class AgentConversationObject extends Cloudflare.DurableObjectNamespace<
  AgentConversationObject,
  Shape
>()("AgentConversationObject") {}

export const AiGateway = Cloudflare.AiGateway("DenoraAiGateway", {
  collectLogs: true,
});

export const AgentConversationObjectLive = AgentConversationObject.make(
  Effect.succeed(
    Effect.gen(function* () {
      const aiGateway = yield* Cloudflare.AiGateway.bind(AiGateway);
      const piLayer = PiRuntime.layer.pipe(
        Layer.provide(PiAgentModel.layer()),
        Layer.provide(PiAgentModel.aiGatewayLayerFromClient(aiGateway)),
      );

      return yield* Effect.gen(function* () {
        const state = yield* Cloudflare.DurableObjectState;
        const persistenceLayer = AgentConversationCoordinator.sqliteLayer.pipe(
          Layer.provideMerge(EventStreamStore.sqliteLayer),
          Layer.provideMerge(AgentConversationSessionStore.sqliteLayer),
          Layer.provide(SqlStorage.layer(state.storage.sql)),
          Layer.orDie,
        );
        const objectLayer = Layer.merge(piLayer, persistenceLayer);

        return yield* Effect.gen(function* () {
          const pi = yield* PiRuntime.Service;
          const store = yield* EventStreamStore.Service;
          const coordinator = yield* AgentConversationCoordinator.Service;

          return {
            abortConversation: (input?: { readonly reason?: string | undefined }) =>
              Effect.gen(function* () {
                const result = yield* coordinator.abortConversation(input);
                if (result.needsWake) yield* scheduleRunAlarm(state, result.wakeDelayMs);
                return result;
              }),
            setConversationLifecycle: (input: {
              readonly conversationId: string;
              readonly status: ConversationLifecycleState;
            }) =>
              Effect.gen(function* () {
                const result = yield* coordinator.setConversationLifecycle(input);
                if (result.needsWake) yield* scheduleRunAlarm(state, result.wakeDelayMs);
                return result;
              }),
            submitMessage: (input: CreateConversationSubmissionInput) =>
              Effect.gen(function* () {
                const admission = yield* coordinator.admitSubmission(input);
                const created = yield* AgentRunLifecycle.createConversationSubmission(store, input);
                if (input.waitForResult) {
                  const result = yield* waitForSubmissionResult(
                    input.submissionId,
                    coordinator,
                    pi,
                    (delayMs) => scheduleRunAlarm(state, delayMs),
                  );
                  return { ...created, result };
                }
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
              const response = yield* handleConversationObjectRequest(store, webRequest).pipe(
                Effect.catchCause((cause) =>
                  Effect.gen(function* () {
                    const traceId = crypto.randomUUID();
                    yield* Effect.logError("agent conversation object stream request failed", {
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
        }).pipe(Effect.provide(objectLayer));
      });
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
    .pipe(durableObjectStorageFailure("schedule agent conversation alarm"));

const waitForSubmissionResult = Effect.fn("AgentConversationObject.waitForSubmissionResult")(
  function* (
    submissionId: string,
    coordinator: AgentConversationCoordinatorInterface,
    pi: PiRuntime.Interface,
    scheduleWake: (delayMs: number) => Effect.Effect<void, EventStorageFailed>,
  ): Effect.fn.Return<unknown, EventStreamError> {
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      yield* coordinator.reconcile({ pi, scheduleWake });
      const terminal = yield* coordinator.getSubmissionTerminal(submissionId);
      if (terminal !== null) return terminalResult(terminal.event);
      yield* Effect.sleep("10 millis");
    }
    return yield* new EventStorageFailed({
      operation: "wait for attached agent result",
      cause: new Error(`Timed out waiting for submission ${submissionId}.`),
    });
  },
);

const terminalResult = (event: unknown): unknown => {
  if (typeof event === "object" && event !== null && "result" in event) {
    return (event as { readonly result?: unknown }).result ?? null;
  }
  return null;
};

export * as AgentConversationObjectModule from "./AgentConversationObject.ts";
