import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { PiRuntime } from "../agent-loop/PiRuntime.ts";
import {
  AgentRunPersistence,
  type Error as AgentRunPersistenceError,
} from "./AgentRunPersistence.ts";
import { type EventStreamError, makeInMemoryEventStreamStore } from "./EventStreamStore.ts";
import { AgentRunLifecycle, type CreateRunInput, type CreateRunResult } from "./Lifecycle.ts";
import {
  eventStreamErrorResponse,
  forbiddenResponse,
  handleStreamHead,
  handleStreamRead,
  internalErrorResponse,
} from "./StreamProtocol.ts";

export class CreateAgentRunFailed extends Schema.TaggedErrorClass<CreateAgentRunFailed>()(
  "CreateAgentRunFailed",
  {
    reason: Schema.Literals([
      "invalid_stream_offset",
      "stream_not_found",
      "stream_closed",
      "event_serialization_failed",
      "event_storage_failed",
      "persistence_failed",
      "run_not_authorized",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

export interface CreateAgentRunInput {
  readonly runId?: string | undefined;
  readonly input?: unknown;
  readonly userId?: string | undefined;
  readonly conversationId?: string | undefined;
  readonly triggerMessageId?: string | undefined;
}

export interface AgentRunObjectStub {
  readonly create: (input: CreateRunInput) => Effect.Effect<CreateRunResult, EventStreamError>;
  readonly fetch: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, HttpServerError>;
}

export interface AgentRunObjectNamespace {
  readonly getByName: (name: string) => AgentRunObjectStub;
}

export interface Interface {
  readonly create: (
    input: CreateAgentRunInput,
  ) => Effect.Effect<CreateRunResult, CreateAgentRunFailed>;
  readonly streamRequest: (
    runId: string,
    userId: string,
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/AgentRuns") {}

export const layer = (
  objects: AgentRunObjectNamespace,
): Layer.Layer<Service, never, AgentRunPersistence.Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const persistence = yield* AgentRunPersistence.Service;

      return Service.of({
        create: (input) => {
          const runId = input.runId ?? crypto.randomUUID();
          if (input.userId === undefined) {
            return Effect.fail(
              new CreateAgentRunFailed({
                reason: "run_not_authorized",
                message: "Authenticated user is required to create an Agent Run.",
              }),
            );
          }

          return persistence
            .registerRun({
              runId,
              input: input.input,
              userId: input.userId,
              conversationId: input.conversationId,
              triggerMessageId: input.triggerMessageId,
            })
            .pipe(
              Effect.flatMap((registered) =>
                objects
                  .getByName(runId)
                  .create({ runId, input: registered.input, userId: input.userId })
                  .pipe(
                    Effect.map((created) => ({
                      ...created,
                      created: registered.created && created.created,
                    })),
                  ),
              ),
              Effect.mapError(createAgentRunFailed),
              Effect.catchCause(createAgentRunFailedFromCause),
            );
        },
        streamRequest: (runId, userId, request) =>
          persistence.authorizeRun({ runId, userId }).pipe(
            Effect.matchEffect({
              onFailure: (error) => {
                if (error._tag === "RunNotAuthorized") {
                  return Effect.succeed(HttpServerResponse.fromWeb(forbiddenResponse()));
                }
                return Effect.gen(function* () {
                  const traceId = crypto.randomUUID();
                  yield* Effect.logError("agent run stream authorization failed", {
                    runId,
                    traceId,
                    error,
                  });
                  return HttpServerResponse.fromWeb(internalErrorResponse(traceId));
                });
              },
              onSuccess: () => objects.getByName(runId).fetch(request),
            }),
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                const traceId = crypto.randomUUID();
                yield* Effect.logError("agent run stream forwarding failed", {
                  runId,
                  traceId,
                  cause,
                });
                return HttpServerResponse.fromWeb(internalErrorResponse(traceId));
              }),
            ),
          ),
      });
    }),
  );

export const inMemoryLayer: Layer.Layer<Service, never, PiRuntime.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const pi = yield* PiRuntime.Service;
    const store = makeInMemoryEventStreamStore();

    return Service.of({
      create: (input) => {
        const runId = input.runId ?? crypto.randomUUID();
        return AgentRunLifecycle.startRun(store, {
          runId,
          input: input.input,
          userId: input.userId,
          scheduleExecution: () =>
            AgentRunLifecycle.executeRun(store, {
              runId,
              input: input.input,
              userId: input.userId,
              pi,
            }).pipe(
              Effect.catch((error) =>
                Effect.logError("in-memory agent run execution failed", { error }),
              ),
              Effect.forkDetach({ startImmediately: true }),
              Effect.asVoid,
            ),
        }).pipe(Effect.mapError(createAgentRunFailed));
      },
      streamRequest: (runId, _userId, request) =>
        Effect.gen(function* () {
          const webRequest = yield* HttpServerRequest.toWeb(request);
          const path = `runs/${runId}`;
          const response = yield* (
            webRequest.method === "HEAD"
              ? handleStreamHead(store, path)
              : handleStreamRead({
                  store,
                  path,
                  request: webRequest,
                })
          ).pipe(Effect.catch((error) => Effect.succeed(eventStreamErrorResponse(error, path))));
          return HttpServerResponse.fromWeb(response);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              const traceId = crypto.randomUUID();
              yield* Effect.logError("agent run in-memory stream request failed", {
                runId,
                traceId,
                cause,
              });
              return HttpServerResponse.fromWeb(internalErrorResponse(traceId));
            }),
          ),
        ),
    });
  }),
);

const createAgentRunFailed = (
  error: EventStreamError | AgentRunPersistenceError,
): CreateAgentRunFailed => {
  if (error._tag === "PersistenceFailed") {
    return new CreateAgentRunFailed({
      reason: "persistence_failed",
      message: "Agent Run persistence failed.",
    });
  }
  if (error._tag === "RunNotAuthorized") {
    return new CreateAgentRunFailed({
      reason: "run_not_authorized",
      message: "Agent Run is not available for the authenticated user.",
    });
  }

  switch (error._tag) {
    case "InvalidStreamOffset":
      return new CreateAgentRunFailed({
        reason: "invalid_stream_offset",
        message: "Invalid stream offset.",
      });
    case "StreamNotFound":
      return new CreateAgentRunFailed({
        reason: "stream_not_found",
        message: "Agent run stream was not found.",
      });
    case "StreamClosed":
      return new CreateAgentRunFailed({
        reason: "stream_closed",
        message: "Agent run stream is closed.",
      });
    case "EventSerializationFailed":
      return new CreateAgentRunFailed({
        reason: "event_serialization_failed",
        message: "Agent run event could not be serialized.",
      });
    case "EventStorageFailed":
      return new CreateAgentRunFailed({
        reason: "event_storage_failed",
        message: "Agent run event stream storage failed.",
      });
  }
};

const createAgentRunFailedFromCause = (
  cause: Cause.Cause<CreateAgentRunFailed>,
): Effect.Effect<never, CreateAgentRunFailed> => {
  const failure = cause.reasons.find(Cause.isFailReason)?.error;
  if (failure !== undefined) return Effect.fail(failure);

  return Effect.gen(function* () {
    const traceId = crypto.randomUUID();
    yield* Effect.logError("agent run create failed", { traceId, cause });
    return yield* new CreateAgentRunFailed({
      reason: "event_storage_failed",
      message: "Agent run could not be started.",
    });
  });
};

export * as AgentRuns from "./AgentRuns.ts";
