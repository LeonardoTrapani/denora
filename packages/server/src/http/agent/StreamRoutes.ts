import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Sink from "effect/Sink";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as HttpRouter from "effect/unstable/http/HttpRouter";
import * as HttpServerRequest from "effect/unstable/http/HttpServerRequest";
import * as HttpServerResponse from "effect/unstable/http/HttpServerResponse";
import { SendMessageRequest } from "../../agent/Schema.ts";
import { AgentThreads } from "../../agent/Threads.ts";
import { Auth } from "../../auth/Auth.ts";

const StreamParams = Schema.Struct({
  agentId: Schema.String,
  threadId: Schema.String,
});

const streamHeaders = {
  "cache-control": "no-cache",
  "content-type": "text/plain; charset=utf-8",
  "x-accel-buffering": "no",
};

export const routes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const auth = yield* Auth.Service;
    const threads = yield* AgentThreads.Service;

    yield* router.add(
      "POST",
      "/agents/:agentId/threads/:threadId/messages/stream",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        const requestId = crypto.randomUUID();

        yield* Effect.logInfo("agent stream route received", { requestId });

        const requestResult = yield* HttpServerRequest.toWeb(request).pipe(
          Effect.map((webRequest) => ({ _tag: "ok" as const, webRequest })),
          Effect.catch(() => Effect.succeed({ _tag: "bad" as const })),
        );
        if (requestResult._tag === "bad") {
          yield* Effect.logWarning("agent stream route invalid request", { requestId });
          return HttpServerResponse.text("Invalid request", { status: 400 });
        }

        const webRequest = requestResult.webRequest;
        const authResult = yield* auth.requireSession(webRequest).pipe(
          Effect.map((user) => ({ _tag: "ok" as const, user })),
          Effect.catch(() => Effect.succeed("unauthorized" as const)),
        );
        if (authResult === "unauthorized") {
          yield* Effect.logWarning("agent stream route unauthorized", { requestId });
          return HttpServerResponse.text("Authentication required", { status: 401 });
        }

        const paramsResult = yield* HttpRouter.schemaPathParams(StreamParams).pipe(
          Effect.map((params) => ({ _tag: "ok" as const, params })),
          Effect.catch(() => Effect.succeed({ _tag: "bad" as const })),
        );
        if (paramsResult._tag === "bad") {
          yield* Effect.logWarning("agent stream route invalid path", { requestId });
          return HttpServerResponse.text("Invalid path", { status: 400 });
        }

        if (paramsResult.params.agentId !== authResult.user.id) {
          yield* Effect.logWarning("agent stream route forbidden", {
            agentId: paramsResult.params.agentId,
            requestId,
            userId: authResult.user.id,
          });
          return HttpServerResponse.text("Agent does not belong to current user", { status: 403 });
        }

        const payloadResult = yield* request.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(SendMessageRequest)),
          Effect.map((payload) => ({ _tag: "ok" as const, payload })),
          Effect.catch(() => Effect.succeed({ _tag: "bad" as const })),
        );
        if (payloadResult._tag === "bad") {
          yield* Effect.logWarning("agent stream route invalid payload", {
            agentId: paramsResult.params.agentId,
            requestId,
            threadId: paramsResult.params.threadId,
          });
          return HttpServerResponse.text("Invalid message payload", { status: 400 });
        }

        yield* Effect.logInfo("agent stream route accepted", {
          agentId: paramsResult.params.agentId,
          messageLength: payloadResult.payload.message.length,
          requestId,
          threadId: paramsResult.params.threadId,
        });

        const body = threads
          .stream({
            agentId: paramsResult.params.agentId,
            threadId: paramsResult.params.threadId,
            message: payloadResult.payload.message,
          })
          .pipe(
            Stream.tap((chunk) =>
              Effect.logInfo("agent stream route chunk", {
                chunkLength: chunk.length,
                requestId,
              }),
            ),
            Stream.tapError((error) =>
              Effect.logError("agent thread HTTP stream failed", {
                agentId: paramsResult.params.agentId,
                threadId: paramsResult.params.threadId,
                error,
              }),
            ),
          );

        const streamScope = yield* Scope.make();
        const closeStreamScope = Scope.close(streamScope, Exit.void);
        const streamResult = yield* body.pipe(
          Stream.peel(Sink.head()),
          Effect.provideService(Scope.Scope, streamScope),
          Effect.map(([firstChunk, rest]) => ({ _tag: "ok" as const, firstChunk, rest })),
          Effect.catch((error) =>
            closeStreamScope.pipe(Effect.as({ _tag: "failed" as const, error })),
          ),
        );

        if (streamResult._tag === "failed") {
          yield* Effect.logError("agent stream route failed before response", {
            agentId: paramsResult.params.agentId,
            threadId: paramsResult.params.threadId,
            error: streamResult.error,
            requestId,
          });
          return HttpServerResponse.text(streamErrorMessage(streamResult.error), {
            status: 500,
          });
        }

        if (Option.isNone(streamResult.firstChunk)) {
          yield* Effect.logError("agent stream route completed without chunks", {
            agentId: paramsResult.params.agentId,
            threadId: paramsResult.params.threadId,
            requestId,
          });
          yield* closeStreamScope;
          return HttpServerResponse.text("Assistant stream completed without text", {
            status: 500,
          });
        }

        const streamBody = streamResult.rest.pipe(
          Stream.prepend([streamResult.firstChunk.value]),
          Stream.ensuring(
            Effect.all(
              [Effect.logInfo("agent stream route finalized", { requestId }), closeStreamScope],
              { discard: true },
            ),
          ),
          Stream.encodeText,
        );

        return HttpServerResponse.stream(streamBody, { headers: streamHeaders });
      }),
    );
  }),
);

const streamErrorMessage = (error: unknown): string => {
  const agentThreadError = findAgentThreadError(error);
  if (agentThreadError !== undefined) {
    return [
      agentThreadError.message,
      agentThreadError.model === undefined ? undefined : `model=${agentThreadError.model}`,
      agentThreadError.detail,
    ]
      .filter((part) => part !== undefined && part.length > 0)
      .join("; ");
  }

  if (error instanceof Error && error.message.length > 0) return error.message;
  if (typeof error === "object" && error !== null && "message" in error) {
    const message = (error as { readonly message?: unknown }).message;
    if (typeof message === "string" && message.length > 0) return message;
  }
  return "Unable to stream assistant response";
};

const findAgentThreadError = (
  error: unknown,
): { readonly message: string; readonly model?: string; readonly detail?: string } | undefined => {
  if (typeof error !== "object" || error === null) return undefined;

  const record = error as {
    readonly _tag?: unknown;
    readonly message?: unknown;
    readonly model?: unknown;
    readonly detail?: unknown;
    readonly error?: unknown;
  };

  if (record._tag === "AgentThreadError" && typeof record.message === "string") {
    return {
      message: record.message,
      ...(typeof record.model === "string" ? { model: record.model } : {}),
      ...(typeof record.detail === "string" ? { detail: record.detail } : {}),
    };
  }

  return findAgentThreadError(record.error);
};

export * as AgentStreamRoutes from "./StreamRoutes.ts";
