import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { type EventStreamError, makeInMemoryEventStreamStore } from "./EventStreamStore.ts";
import { AgentRunLifecycle, type CreateRunInput, type CreateRunResult } from "./Lifecycle.ts";
import {
  eventStreamErrorResponse,
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
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

export interface CreateAgentRunInput {
  readonly runId?: string | undefined;
  readonly input?: unknown;
  readonly userId?: string | undefined;
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
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
}

export class Service extends Context.Service<Service, Interface>()("@denora/server/AgentRuns") {}

export const layer = (objects: AgentRunObjectNamespace): Layer.Layer<Service> =>
  Layer.succeed(
    Service,
    Service.of({
      create: (input) => {
        const runId = input.runId ?? crypto.randomUUID();
        return objects
          .getByName(runId)
          .create({ runId, input: input.input, userId: input.userId })
          .pipe(Effect.mapError(createAgentRunFailed));
      },
      streamRequest: (runId, request) =>
        objects
          .getByName(runId)
          .fetch(request)
          .pipe(
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
    }),
  );

export const inMemoryLayer = Layer.sync(Service, () => {
  const store = makeInMemoryEventStreamStore();

  return Service.of({
    create: (input) => {
      const runId = input.runId ?? crypto.randomUUID();
      return AgentRunLifecycle.createRun(store, {
        runId,
        input: input.input,
        userId: input.userId,
      }).pipe(Effect.mapError(createAgentRunFailed));
    },
    streamRequest: (runId, request) =>
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
});

const createAgentRunFailed = (error: EventStreamError): CreateAgentRunFailed => {
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

export * as AgentRuns from "./AgentRuns.ts";
