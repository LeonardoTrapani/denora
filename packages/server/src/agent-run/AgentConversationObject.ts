import * as Cloudflare from "alchemy/Cloudflare";
import { RuntimeContext } from "alchemy";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpMiddleware from "effect/unstable/http/HttpMiddleware";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { PiAgentProvider } from "../agent-loop/PiAgentProvider.ts";
import { PiRuntime } from "../agent-loop/PiRuntime.ts";
import { ServerConfig } from "../config/ServerConfig.ts";
import { Telemetry } from "../observability/Telemetry.ts";
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
  type CreateRunInput,
  type CreateRunResult,
} from "./Lifecycle.ts";
import { SqlStorage } from "./SqlStorage.ts";
import { StreamChunks } from "./StreamChunks.ts";
import { handleAgentConversationObjectRequest, internalErrorResponse } from "./StreamProtocol.ts";

const AGENT_RUN_RECONCILE_EVENT_ID = "agent-run:reconcile";
const AGENT_RUN_RECONCILE_RETRY_DELAY_MS = 30_000;

export interface Shape {
  readonly createRun: (input: CreateRunInput) => Effect.Effect<CreateRunResult, EventStreamError>;
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

export class AgentConversationObject extends Cloudflare.DurableObject<
  AgentConversationObject,
  Shape
>()("AgentConversationObject") {}

export const AgentConversationObjectLive = AgentConversationObject.make<never>(
  Effect.gen(function* () {
    const state = yield* Cloudflare.DurableObjectState;
    const piLayer = PiRuntime.layer.pipe(
      Layer.provide(PiAgentProvider.defaultLayer),
      Layer.provide(ServerConfig.defaultLayer),
      Layer.orDie,
    );
    const persistenceLayer = AgentConversationCoordinator.sqliteLayer.pipe(
      Layer.provideMerge(EventStreamStore.sqliteLayer),
      Layer.provideMerge(AgentConversationSessionStore.sqliteLayer),
      Layer.provideMerge(StreamChunks.sqliteLayer),
      Layer.provide(SqlStorage.layer(state.storage.sql)),
      Layer.orDie,
    );
    const objectLayer = Layer.mergeAll(piLayer, persistenceLayer, Telemetry.layer);

    return yield* Effect.succeed(
      Effect.gen(function* () {
        const pi = yield* PiRuntime.Service;
        const store = yield* EventStreamStore.Service;
        const coordinator = yield* AgentConversationCoordinator.Service;

        return {
          createRun: Effect.fn("AgentConversationObject.createRun")(function* (
            input: CreateRunInput,
          ) {
            yield* Effect.annotateCurrentSpan({ "denora.agent_run.id": input.runId });
            if (input.conversationId !== undefined) {
              yield* Effect.annotateCurrentSpan({
                "denora.conversation.id": input.conversationId,
              });
            }

            return yield* AgentRunLifecycle.startRun(store, {
              ...input,
              scheduleExecution: (runId) =>
                AgentRunLifecycle.executeRun(store, { ...input, runId, pi }).pipe(
                  Effect.catch((error) =>
                    Effect.logError("standalone agent run execution failed", { runId, error }),
                  ),
                  Effect.forkDetach({ startImmediately: true }),
                  Effect.asVoid,
                ),
            });
          }),
          abortConversation: Effect.fn("AgentConversationObject.abortConversation")(
            function* (input?: { readonly reason?: string | undefined }) {
              const result = yield* coordinator.abortConversation(input);
              if (result.needsWake)
                yield* scheduleAgentRunReconcile(state, result.wakeDelayMs, "abort_conversation");
              return result;
            },
          ),
          setConversationLifecycle: Effect.fn("AgentConversationObject.setConversationLifecycle")(
            function* (input: {
              readonly conversationId: string;
              readonly status: ConversationLifecycleState;
            }) {
              yield* Effect.annotateCurrentSpan({
                "denora.conversation.id": input.conversationId,
                "denora.conversation.status": input.status,
              });

              const result = yield* coordinator.setConversationLifecycle(input);
              if (result.needsWake)
                yield* scheduleAgentRunReconcile(
                  state,
                  result.wakeDelayMs,
                  "conversation_lifecycle",
                );
              return result;
            },
          ),
          submitMessage: Effect.fn("AgentConversationObject.submitMessage")(function* (
            input: CreateConversationSubmissionInput,
          ) {
            yield* Effect.annotateCurrentSpan({
              "denora.agent.name": input.agentName,
              "denora.agent_run.id": input.runId,
              "denora.conversation.id": input.conversationId,
              "denora.submission.id": input.submissionId,
            });

            const created = yield* AgentRunLifecycle.createConversationSubmission(store, input);
            const admission = yield* coordinator.admitSubmission(input);
            if (created.created || admission.admitted)
              yield* scheduleAgentRunReconcile(state, 0, "submission_admitted");
            if (input.waitForResult) {
              const result = yield* waitForSubmissionResult(
                input.submissionId,
                coordinator,
                pi,
                (delayMs) => scheduleAgentRunReconcile(state, delayMs, "wait_for_result"),
              );
              return { ...created, result };
            }
            return created;
          }),
          alarm: Effect.fn("AgentConversationObject.alarm")(function* () {
            const fired = yield* Cloudflare.processScheduledEvents.pipe(
              Effect.provideService(Cloudflare.DurableObjectState, state),
              Effect.provide(RuntimeContext.phantom),
            );
            if (fired.some((event) => event.id === AGENT_RUN_RECONCILE_EVENT_ID)) {
              yield* reconcileAgentConversation(state, coordinator, pi);
            }
          }),
          fetch: Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const webRequest = yield* HttpServerRequest.toWeb(request);
            const response = yield* handleAgentConversationObjectRequest(store, webRequest).pipe(
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
          }).pipe(HttpMiddleware.tracer, HttpMiddleware.logger),
        };
      }).pipe(Effect.provide(objectLayer)),
    );
  }),
);

const alchemySchedulerFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) => new EventStorageFailed({ operation, cause })),
      Effect.provide(RuntimeContext.phantom),
    );

const scheduleAgentRunReconcile = (
  state: Cloudflare.DurableObjectState["Service"],
  delayMs = 0,
  reason: AgentRunReconcileReason,
): Effect.Effect<void, EventStorageFailed> =>
  Cloudflare.scheduleEvent(AGENT_RUN_RECONCILE_EVENT_ID, new Date(Date.now() + delayMs), {
    type: "agent-run/reconcile",
    reason,
  } satisfies AgentRunReconcileEventPayload).pipe(
    Effect.provideService(Cloudflare.DurableObjectState, state),
    alchemySchedulerFailure("schedule agent run reconcile event"),
  );

const cancelAgentRunReconcile = (
  state: Cloudflare.DurableObjectState["Service"],
): Effect.Effect<void, EventStorageFailed> =>
  Cloudflare.cancelEvent(AGENT_RUN_RECONCILE_EVENT_ID).pipe(
    Effect.provideService(Cloudflare.DurableObjectState, state),
    alchemySchedulerFailure("cancel agent run reconcile event"),
  );

const reconcileAgentConversation = Effect.fn("AgentConversationObject.reconcileAgentConversation")(
  function* (
    state: Cloudflare.DurableObjectState["Service"],
    coordinator: AgentConversationCoordinatorInterface,
    pi: PiRuntime.Interface,
  ): Effect.fn.Return<void, EventStreamError> {
    yield* scheduleAgentRunReconcile(
      state,
      AGENT_RUN_RECONCILE_RETRY_DELAY_MS,
      "alarm_reconcile_retry",
    );
    const result = yield* coordinator.reconcile({
      pi,
      scheduleWake: (delayMs) => scheduleAgentRunReconcile(state, delayMs, "coordinator_wake"),
    });
    if (result.needsWake) {
      yield* scheduleAgentRunReconcile(state, result.wakeDelayMs, "coordinator_result");
      return;
    }
    yield* cancelAgentRunReconcile(state);
  },
);

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

type AgentRunReconcileReason =
  | "abort_conversation"
  | "alarm_reconcile_retry"
  | "conversation_lifecycle"
  | "coordinator_result"
  | "coordinator_wake"
  | "submission_admitted"
  | "wait_for_result";

interface AgentRunReconcileEventPayload {
  readonly type: "agent-run/reconcile";
  readonly reason: AgentRunReconcileReason;
}

export * as AgentConversationObjectModule from "./AgentConversationObject.ts";
