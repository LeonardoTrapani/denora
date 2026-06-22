import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { HttpServerError } from "effect/unstable/http/HttpServerError";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { type EventStreamError, makeInMemoryEventStreamStore } from "./EventStreamStore.ts";
import { AgentRunLifecycle, type CreateRunInput, type CreateRunResult } from "./Lifecycle.ts";
import { handleStreamRead } from "./StreamProtocol.ts";

export class CreateAgentRunFailed extends Schema.TaggedErrorClass<CreateAgentRunFailed>()(
  "CreateAgentRunFailed",
  { message: Schema.String },
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
          .pipe(Effect.mapError((error) => new CreateAgentRunFailed({ message: error._tag })));
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
                return HttpServerResponse.fromWeb(
                  Response.json({ error: "InternalError", traceId }, { status: 500 }),
                );
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
      }).pipe(Effect.mapError((error) => new CreateAgentRunFailed({ message: error._tag })));
    },
    streamRequest: (runId, request) =>
      Effect.gen(function* () {
        const webRequest = yield* HttpServerRequest.toWeb(request);
        const response = yield* handleStreamRead({
          store,
          path: `runs/${runId}`,
          request: webRequest,
        }).pipe(
          Effect.catch((error) =>
            Effect.succeed(Response.json({ error: error._tag }, { status: 500 })),
          ),
        );
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
            return HttpServerResponse.fromWeb(
              Response.json({ error: "InternalError", traceId }, { status: 500 }),
            );
          }),
        ),
      ),
  });
});

export * as AgentRuns from "./AgentRuns.ts";
