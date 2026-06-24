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
  type EventStreamError,
  agentStreamPath,
  makeInMemoryEventStreamStore,
} from "../agent-run/EventStreamStore.ts";
import {
  AgentRunLifecycle,
  type CreateConversationSubmissionInput,
} from "../agent-run/Lifecycle.ts";
import {
  eventStreamErrorResponse,
  forbiddenResponse,
  handleStreamHead,
  handleStreamRead,
  internalErrorResponse,
} from "../agent-run/StreamProtocol.ts";
import {
  ConversationPersistence,
  type ConversationMessageRecord,
  type ConversationRecord,
} from "./ConversationPersistence.ts";

export class ConversationRequestFailed extends Schema.TaggedErrorClass<ConversationRequestFailed>()(
  "ConversationRequestFailed",
  {
    reason: Schema.Literals([
      "invalid_stream_offset",
      "stream_not_found",
      "stream_closed",
      "event_serialization_failed",
      "event_storage_failed",
      "persistence_failed",
      "conversation_not_authorized",
    ]),
    message: Schema.String,
  },
  { httpApiStatus: 500 },
) {}

export interface SubmitMessageInput {
  readonly conversationId: string;
  readonly userId: string;
  readonly agentName?: string | undefined;
  readonly message?: string | undefined;
  readonly images?: ReadonlyArray<ConversationPersistence.AgentPromptImage> | undefined;
  readonly content?: unknown;
  readonly waitForResult?: boolean | undefined;
}

export interface SubmitMessageResult {
  readonly conversationId: string;
  readonly messageId: string;
  readonly submissionId: string;
  readonly runId: string;
  readonly streamPath: string;
  readonly offset: string;
  readonly result?: unknown;
}

export interface AbortConversationResult {
  readonly abortedSubmissions: number;
  readonly needsWake: boolean;
  readonly wakeDelayMs: number;
}

export interface AgentConversationObjectStub {
  readonly submitMessage: (
    input: CreateConversationSubmissionInput,
  ) => Effect.Effect<SubmitMessageResult, EventStreamError>;
  readonly abortConversation: (input?: {
    readonly reason?: string | undefined;
  }) => Effect.Effect<AbortConversationResult, EventStreamError>;
  readonly fetch: (
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse, HttpServerError>;
}

export interface AgentConversationObjectNamespace {
  readonly getByName: (name: string) => AgentConversationObjectStub;
}

export interface Interface {
  readonly createConversation: (input: {
    readonly userId: string;
    readonly conversationId?: string | undefined;
    readonly agentId?: string | null | undefined;
    readonly title?: string | null | undefined;
    readonly metadata?: unknown;
  }) => Effect.Effect<ConversationRecord, ConversationRequestFailed>;
  readonly listConversations: (
    userId: string,
  ) => Effect.Effect<ReadonlyArray<ConversationRecord>, ConversationRequestFailed>;
  readonly listMessages: (input: {
    readonly conversationId: string;
    readonly userId: string;
  }) => Effect.Effect<ReadonlyArray<ConversationMessageRecord>, ConversationRequestFailed>;
  readonly submitMessage: (
    input: SubmitMessageInput,
  ) => Effect.Effect<SubmitMessageResult, ConversationRequestFailed>;
  readonly abortConversation: (input: {
    readonly conversationId: string;
    readonly userId: string;
    readonly reason?: string | undefined;
  }) => Effect.Effect<AbortConversationResult, ConversationRequestFailed>;
  readonly streamRequest: (
    agentName: string,
    conversationId: string,
    userId: string,
    request: HttpServerRequest.HttpServerRequest,
  ) => Effect.Effect<HttpServerResponse.HttpServerResponse>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/Conversations",
) {}

export const layer = (
  objects: AgentConversationObjectNamespace,
): Layer.Layer<Service, never, ConversationPersistence.Service> =>
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const persistence = yield* ConversationPersistence.Service;

      return Service.of({
        createConversation: (input) =>
          persistence
            .createConversation(input)
            .pipe(
              Effect.mapError(conversationRequestFailed),
              Effect.catchCause(conversationRequestFailedFromCause),
            ),
        listConversations: (userId) =>
          persistence
            .listConversations(userId)
            .pipe(
              Effect.mapError(conversationRequestFailed),
              Effect.catchCause(conversationRequestFailedFromCause),
            ),
        listMessages: (input) =>
          persistence
            .listMessages(input)
            .pipe(
              Effect.mapError(conversationRequestFailed),
              Effect.catchCause(conversationRequestFailedFromCause),
            ),
        submitMessage: (input) =>
          persistence.submitMessage(input).pipe(
            Effect.flatMap((submitted) =>
              objects
                .getByName(submitted.conversationId)
                .submitMessage({
                  agentName: input.agentName ?? "default",
                  runId: submitted.runId,
                  conversationId: submitted.conversationId,
                  submissionId: submitted.submissionId,
                  triggerMessageId: submitted.messageId,
                  input: submitted.input,
                  userId: input.userId,
                  waitForResult: input.waitForResult,
                })
                .pipe(
                  Effect.map((admitted) => ({
                    ...admitted,
                    messageId: submitted.messageId,
                    streamPath: submitted.streamPath,
                  })),
                ),
            ),
            Effect.mapError(conversationRequestFailed),
            Effect.catchCause(conversationRequestFailedFromCause),
          ),
        abortConversation: (input) =>
          persistence.authorizeConversation(input).pipe(
            Effect.flatMap(() =>
              objects.getByName(input.conversationId).abortConversation({ reason: input.reason }),
            ),
            Effect.mapError(conversationRequestFailed),
            Effect.catchCause(conversationRequestFailedFromCause),
          ),
        streamRequest: (_agentName, conversationId, userId, request) =>
          persistence.authorizeConversation({ conversationId, userId }).pipe(
            Effect.matchEffect({
              onFailure: (error) => {
                if (error._tag === "ConversationNotAuthorized") {
                  return Effect.succeed(HttpServerResponse.fromWeb(forbiddenResponse()));
                }
                return Effect.gen(function* () {
                  const traceId = crypto.randomUUID();
                  yield* Effect.logError("conversation stream authorization failed", {
                    conversationId,
                    traceId,
                    error,
                  });
                  return HttpServerResponse.fromWeb(internalErrorResponse(traceId));
                });
              },
              onSuccess: () => objects.getByName(conversationId).fetch(request),
            }),
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                const traceId = crypto.randomUUID();
                yield* Effect.logError("conversation stream forwarding failed", {
                  conversationId,
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
    const conversations = new Map<string, ConversationRecord>();
    const messages = new Map<string, ConversationMessageRecord[]>();

    const createInMemoryConversation = (input: {
      readonly userId: string;
      readonly conversationId?: string | undefined;
      readonly agentId?: string | null | undefined;
      readonly title?: string | null | undefined;
      readonly metadata?: unknown;
    }): ConversationRecord => {
      const conversationId = input.conversationId ?? `conversation_${crypto.randomUUID()}`;
      const existing = conversations.get(conversationId);
      if (existing !== undefined) return existing;
      const timestamp = new Date().toISOString();
      const conversation: ConversationRecord = {
        id: conversationId,
        ownerUserId: input.userId,
        agentId: input.agentId ?? null,
        status: "active",
        title: input.title ?? null,
        metadata: input.metadata ?? null,
        createdAt: timestamp,
        updatedAt: timestamp,
        archivedAt: null,
      };
      conversations.set(conversationId, conversation);
      messages.set(conversationId, []);
      return conversation;
    };

    return Service.of({
      createConversation: (input) => Effect.sync(() => createInMemoryConversation(input)),
      listConversations: (userId) =>
        Effect.sync(() =>
          Array.from(conversations.values()).filter(
            (conversation) => conversation.ownerUserId === userId,
          ),
        ),
      listMessages: ({ conversationId, userId }) =>
        Effect.sync(() => {
          const conversation = conversations.get(conversationId);
          if (conversation === undefined || conversation.ownerUserId !== userId) return [];
          return messages.get(conversationId) ?? [];
        }),
      submitMessage: (input) =>
        Effect.gen(function* () {
          let conversation = conversations.get(input.conversationId);
          if (conversation === undefined) {
            conversation = createInMemoryConversation({
              userId: input.userId,
              conversationId: input.conversationId,
            });
          }
          const existingMessages = messages.get(input.conversationId) ?? [];
          const timestamp = new Date().toISOString();
          const messageId = `message_${crypto.randomUUID()}`;
          const submissionId = `submission_${crypto.randomUUID()}`;
          const runId = `run_${crypto.randomUUID()}`;
          const content = input.content ?? {
            text: input.message ?? "",
            ...(input.images ? { images: input.images } : {}),
          };
          const prompt =
            typeof content === "object" &&
            content !== null &&
            typeof (content as Record<string, unknown>).text === "string"
              ? ((content as Record<string, unknown>).text as string)
              : (JSON.stringify(content) ?? "");
          const richMessage = richUserMessage(content);
          const inputPayload = {
            prompt: richMessage === undefined ? prompt : "",
            submittedMessage: content,
            messages: richMessage === undefined ? [] : [richMessage],
          };
          const userMessage: ConversationMessageRecord = {
            id: messageId,
            conversationId: input.conversationId,
            runId,
            role: "user",
            content,
            metadata: { submissionId },
            createdAt: timestamp,
          };
          messages.set(input.conversationId, [...existingMessages, userMessage]);
          const agentName = input.agentName ?? "default";
          const created = yield* AgentRunLifecycle.createConversationSubmission(store, {
            agentName,
            conversationId: input.conversationId,
            submissionId,
            runId,
            triggerMessageId: messageId,
            input: inputPayload,
            userId: input.userId,
          }).pipe(Effect.mapError(conversationRequestFailed));
          yield* AgentRunLifecycle.executeConversationSubmissionAttempt(store, {
            agentName,
            conversationId: input.conversationId,
            submissionId,
            runId,
            triggerMessageId: messageId,
            input: inputPayload,
            userId: input.userId,
            pi,
          }).pipe(
            Effect.flatMap((result) =>
              store
                .appendEventOnce(
                  agentStreamPath(agentName, input.conversationId),
                  `direct-submission:${submissionId}:settled`,
                  result.terminalEvent,
                )
                .pipe(
                  Effect.flatMap(() =>
                    store.appendEventOnce(
                      agentStreamPath(agentName, input.conversationId),
                      `direct-submission:${submissionId}:idle`,
                      {
                        v: 3,
                        type: "idle",
                        instanceId: input.conversationId,
                        agentName,
                        submissionId,
                        eventIndex: nextEventIndex(result.terminalEvent),
                        timestamp: new Date().toISOString(),
                      },
                    ),
                  ),
                ),
            ),
            Effect.catch((error) =>
              Effect.logError("in-memory conversation submission failed", { error }),
            ),
            Effect.forkDetach({ startImmediately: true }),
          );
          return {
            conversationId: input.conversationId,
            messageId,
            submissionId,
            runId,
            streamPath: created.streamPath,
            offset: created.offset,
          };
        }),
      abortConversation: () =>
        Effect.succeed({ abortedSubmissions: 0, needsWake: false, wakeDelayMs: 0 }),
      streamRequest: (_agentName, _conversationId, _userId, request) =>
        Effect.gen(function* () {
          const webRequest = yield* HttpServerRequest.toWeb(request);
          const path = agentStreamPath(_agentName, _conversationId);
          const response = yield* (
            webRequest.method === "HEAD"
              ? handleStreamHead(store, path)
              : handleStreamRead({ store, path, request: webRequest })
          ).pipe(Effect.catch((error) => Effect.succeed(eventStreamErrorResponse(error, path))));
          return HttpServerResponse.fromWeb(response);
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.gen(function* () {
              const traceId = crypto.randomUUID();
              yield* Effect.logError("conversation in-memory stream request failed", {
                conversationId: _conversationId,
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

const conversationRequestFailed = (
  error: EventStreamError | ConversationPersistence.Error,
): ConversationRequestFailed => {
  if (error._tag === "PersistenceFailed") {
    return new ConversationRequestFailed({
      reason: "persistence_failed",
      message: "Conversation persistence failed.",
    });
  }
  if (error._tag === "ConversationNotAuthorized") {
    return new ConversationRequestFailed({
      reason: "conversation_not_authorized",
      message: "Conversation is not available for the authenticated user.",
    });
  }
  switch (error._tag) {
    case "InvalidStreamOffset":
      return new ConversationRequestFailed({
        reason: "invalid_stream_offset",
        message: "Invalid stream offset.",
      });
    case "StreamNotFound":
      return new ConversationRequestFailed({
        reason: "stream_not_found",
        message: "Conversation stream was not found.",
      });
    case "StreamClosed":
      return new ConversationRequestFailed({
        reason: "stream_closed",
        message: "Conversation stream is closed.",
      });
    case "EventSerializationFailed":
      return new ConversationRequestFailed({
        reason: "event_serialization_failed",
        message: "Conversation event could not be serialized.",
      });
    case "EventStorageFailed":
      return new ConversationRequestFailed({
        reason: "event_storage_failed",
        message: "Conversation event stream storage failed.",
      });
  }
};

const conversationRequestFailedFromCause = (
  cause: Cause.Cause<ConversationRequestFailed>,
): Effect.Effect<never, ConversationRequestFailed> => {
  const failure = cause.reasons.find(Cause.isFailReason)?.error;
  if (failure !== undefined) return Effect.fail(failure);

  return Effect.gen(function* () {
    const traceId = crypto.randomUUID();
    yield* Effect.logError("conversation request failed", { traceId, cause });
    return yield* new ConversationRequestFailed({
      reason: "event_storage_failed",
      message: "Conversation request could not be completed.",
    });
  });
};

const richUserMessage = (content: unknown): unknown | undefined => {
  if (typeof content !== "object" || content === null) return undefined;
  const record = content as Record<string, unknown>;
  const images = imageMessages(record.images ?? record.image);
  if (images.length === 0) return undefined;
  const text = typeof record.text === "string" ? record.text : undefined;
  return {
    role: "user",
    content: [...(text === undefined ? [] : [{ type: "text", text }]), ...images],
    timestamp: Date.now(),
  };
};

const imageMessages = (value: unknown): ReadonlyArray<unknown> => {
  if (Array.isArray(value)) return value.flatMap((item) => imageMessage(item) ?? []);
  const image = imageMessage(value);
  return image === undefined ? [] : [image];
};

const imageMessage = (value: unknown): unknown | undefined => {
  if (typeof value !== "object" || value === null) return undefined;
  const record = value as Record<string, unknown>;
  return record.type === "image" &&
    typeof record.data === "string" &&
    typeof record.mimeType === "string"
    ? { type: "image", data: record.data, mimeType: record.mimeType }
    : undefined;
};

const nextEventIndex = (event: unknown): number =>
  typeof event === "object" &&
  event !== null &&
  typeof (event as { readonly eventIndex?: unknown }).eventIndex === "number"
    ? (event as { readonly eventIndex: number }).eventIndex + 1
    : 0;

export * as Conversations from "./Conversations.ts";
