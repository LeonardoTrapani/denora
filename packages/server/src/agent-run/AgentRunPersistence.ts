import { and, eq, sql } from "drizzle-orm";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { Db } from "../persistence/Db.ts";
import {
  agentRuns,
  conversationMessages,
  conversations,
  denoraEventStreamEntries,
  denoraEventStreams,
  denoraSessions,
  denoraRuns,
} from "../persistence/schema.ts";
import {
  EventStorageFailed,
  type EventStreamError,
  type EventStreamStore,
  parseOffset,
  runStreamPath,
} from "./EventStreamStore.ts";

export class PersistenceFailed extends Schema.TaggedErrorClass<PersistenceFailed>()(
  "PersistenceFailed",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class RunNotAuthorized extends Schema.TaggedErrorClass<RunNotAuthorized>()(
  "RunNotAuthorized",
  { runId: Schema.String },
) {}

export type Error = PersistenceFailed | RunNotAuthorized;

export interface RegisterRunInput {
  readonly runId: string;
  readonly userId: string;
  readonly input?: unknown;
  readonly conversationId?: string | undefined;
  readonly triggerMessageId?: string | undefined;
}

export interface RegisteredRun {
  readonly runId: string;
  readonly conversationId: string;
  readonly triggerMessageId: string | null;
  readonly streamPath: string;
  readonly input: unknown;
  readonly created: boolean;
}

export interface FinishRunInput {
  readonly runId: string;
  readonly isError: boolean;
  readonly durationMs: number;
  readonly result?: unknown;
  readonly error?: unknown;
}

export interface AppendStreamEventInput {
  readonly path: string;
  readonly event: unknown;
  readonly offset: string;
}

export interface Interface {
  readonly registerRun: (input: RegisterRunInput) => Effect.Effect<RegisteredRun, Error>;
  readonly getRunInput: (runId: string) => Effect.Effect<unknown, Error>;
  readonly authorizeRun: (input: {
    readonly runId: string;
    readonly userId: string;
  }) => Effect.Effect<void, Error>;
  readonly markRunStarted: (runId: string) => Effect.Effect<void, Error>;
  readonly finishRun: (input: FinishRunInput) => Effect.Effect<void, Error>;
  readonly createStream: (path: string) => Effect.Effect<void, Error>;
  readonly appendStreamEvent: (input: AppendStreamEventInput) => Effect.Effect<void, Error>;
  readonly closeStream: (path: string) => Effect.Effect<void, Error>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/AgentRunPersistence",
) {}

export const layer: Layer.Layer<Service, never, Db.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* Db.Service;

    const persist = <A>(operation: string, effect: Effect.Effect<A, unknown>) =>
      effect.pipe(Effect.mapError((cause) => new PersistenceFailed({ operation, cause })));

    const nowIso = () => DateTime.now.pipe(Effect.map(DateTime.formatIso));

    const registerRun = Effect.fn("AgentRunPersistence.registerRun")(function* (
      input: RegisterRunInput,
    ): Effect.fn.Return<RegisteredRun, Error> {
      const existing = yield* findRun(input.runId);
      if (existing !== undefined) {
        yield* authorizeExistingRun(input.runId, input.userId);
        return {
          runId: existing.id,
          conversationId: existing.conversationId,
          triggerMessageId: existing.triggerMessageId,
          streamPath: existing.streamPath,
          input: existing.input,
          created: false,
        };
      }

      const timestamp = yield* nowIso();
      const conversationId = input.conversationId ?? `conversation_${crypto.randomUUID()}`;
      const triggerMessageId = input.triggerMessageId ?? `message_${crypto.randomUUID()}`;
      const streamPath = runStreamPath(input.runId);

      if (input.conversationId === undefined) {
        yield* persist(
          "create conversation",
          db.client
            .insert(conversations)
            .values({
              id: conversationId,
              ownerUserId: input.userId,
              status: "active",
              createdAt: timestamp,
              updatedAt: timestamp,
            })
            .onConflictDoNothing(),
        );
      } else {
        yield* authorizeConversation(conversationId, input.userId);
      }

      if (input.triggerMessageId === undefined) {
        yield* persist(
          "create trigger message",
          db.client
            .insert(conversationMessages)
            .values({
              id: triggerMessageId,
              conversationId,
              role: "user",
              content: normalizeUserMessageContent(input.input),
              metadata: { source: "agent_run_input" },
              createdAt: timestamp,
            })
            .onConflictDoNothing(),
        );
      }

      yield* persist(
        "create agent run",
        db.client.insert(agentRuns).values({
          id: input.runId,
          conversationId,
          triggerMessageId,
          status: "queued",
          streamPath,
          input: input.input,
          createdAt: timestamp,
          updatedAt: timestamp,
        }),
      );

      yield* persist(
        "create denora run",
        db.client
          .insert(denoraRuns)
          .values({
            runId: input.runId,
            workflowName: "denora.agent-run",
            status: "active",
            startedAt: timestamp,
            payload: serializeJson(input.input),
          })
          .onConflictDoNothing(),
      );

      return {
        runId: input.runId,
        conversationId,
        triggerMessageId,
        streamPath,
        input: input.input,
        created: true,
      };
    });

    const findRun = Effect.fn("AgentRunPersistence.findRun")(function* (runId: string) {
      const rows = yield* persist(
        "find agent run",
        db.client.select().from(agentRuns).where(eq(agentRuns.id, runId)).limit(1),
      );
      return rows[0];
    });

    const authorizeConversation = Effect.fn("AgentRunPersistence.authorizeConversation")(function* (
      conversationId: string,
      userId: string,
    ): Effect.fn.Return<void, Error> {
      const rows = yield* persist(
        "authorize conversation",
        db.client
          .select({ id: conversations.id })
          .from(conversations)
          .where(and(eq(conversations.id, conversationId), eq(conversations.ownerUserId, userId)))
          .limit(1),
      );
      if (rows[0] === undefined) return yield* new RunNotAuthorized({ runId: conversationId });
    });

    const authorizeExistingRun = Effect.fn("AgentRunPersistence.authorizeExistingRun")(function* (
      runId: string,
      userId: string,
    ): Effect.fn.Return<void, Error> {
      const rows = yield* persist(
        "authorize agent run",
        db.client
          .select({ id: agentRuns.id })
          .from(agentRuns)
          .innerJoin(conversations, eq(agentRuns.conversationId, conversations.id))
          .where(and(eq(agentRuns.id, runId), eq(conversations.ownerUserId, userId)))
          .limit(1),
      );
      if (rows[0] === undefined) return yield* new RunNotAuthorized({ runId });
    });

    const getRunInput = Effect.fn("AgentRunPersistence.getRunInput")(function* (
      runId: string,
    ): Effect.fn.Return<unknown, Error> {
      const row = yield* findRun(runId);
      if (row === undefined) return yield* new RunNotAuthorized({ runId });
      return row.input;
    });

    const markRunStarted = Effect.fn("AgentRunPersistence.markRunStarted")(function* (
      runId: string,
    ): Effect.fn.Return<void, Error> {
      const timestamp = yield* nowIso();
      yield* persist(
        "mark agent run started",
        db.client
          .update(agentRuns)
          .set({ status: "running", startedAt: timestamp, updatedAt: timestamp })
          .where(eq(agentRuns.id, runId)),
      );
    });

    const finishRun = Effect.fn("AgentRunPersistence.finishRun")(function* (
      input: FinishRunInput,
    ): Effect.fn.Return<void, Error> {
      const timestamp = yield* nowIso();
      const row = yield* findRun(input.runId);
      if (row === undefined) return yield* new RunNotAuthorized({ runId: input.runId });

      yield* persist(
        "finish agent run",
        db.client
          .update(agentRuns)
          .set({
            status: input.isError ? "failed" : "completed",
            result: input.result,
            error: input.error,
            endedAt: timestamp,
            updatedAt: timestamp,
          })
          .where(eq(agentRuns.id, input.runId)),
      );

      yield* persist(
        "finish denora run",
        db.client
          .update(denoraRuns)
          .set({
            status: input.isError ? "errored" : "completed",
            endedAt: timestamp,
            isError: input.isError ? 1 : 0,
            durationMs: Math.trunc(input.durationMs),
            result: serializeJson(input.result),
            error: serializeJson(input.error),
          })
          .where(eq(denoraRuns.runId, input.runId)),
      );

      if (!input.isError) {
        yield* persist(
          "create assistant conversation message",
          db.client.insert(conversationMessages).values({
            id: `message_${crypto.randomUUID()}`,
            conversationId: row.conversationId,
            runId: input.runId,
            role: "assistant",
            content: input.result ?? null,
            metadata: { source: "agent_run_result" },
            createdAt: timestamp,
          }),
        );
      }
    });

    const createStream = Effect.fn("AgentRunPersistence.createStream")(function* (
      path: string,
    ): Effect.fn.Return<void, Error> {
      yield* persist(
        "create denora event stream",
        db.client
          .insert(denoraEventStreams)
          .values({ path, nextOffset: 0, closed: 0 })
          .onConflictDoNothing(),
      );
    });

    const appendStreamEvent = Effect.fn("AgentRunPersistence.appendStreamEvent")(function* (
      input: AppendStreamEventInput,
    ): Effect.fn.Return<void, Error> {
      const seq = yield* parseOffset(input.offset).pipe(
        Effect.mapError(
          (cause) => new PersistenceFailed({ operation: "parse stream offset", cause }),
        ),
      );
      const data = yield* Effect.try({
        try: () => JSON.stringify(input.event),
        catch: (cause) => new PersistenceFailed({ operation: "serialize stream event", cause }),
      });
      if (data === undefined) {
        return yield* new PersistenceFailed({
          operation: "serialize stream event",
          cause: new TypeError("Event is not JSON serializable"),
        });
      }

      yield* createStream(input.path);
      const inserted = yield* persist(
        "append denora event stream entry",
        db.client
          .insert(denoraEventStreamEntries)
          .values({ path: input.path, seq, data })
          .onConflictDoNothing()
          .returning({ seq: denoraEventStreamEntries.seq }),
      );
      if (inserted.length === 0) return;

      yield* persist(
        "advance denora event stream offset",
        db.client.execute(
          sql`UPDATE denora_event_streams SET next_offset = GREATEST(next_offset, ${seq + 1}) WHERE path = ${input.path}`,
        ),
      );

      yield* persistSideEffectsFromEvent(input.path, input.event);
    });

    const closeStream = Effect.fn("AgentRunPersistence.closeStream")(function* (
      path: string,
    ): Effect.fn.Return<void, Error> {
      yield* createStream(path);
      yield* persist(
        "close denora event stream",
        db.client
          .update(denoraEventStreams)
          .set({ closed: 1 })
          .where(eq(denoraEventStreams.path, path)),
      );
    });

    const persistSideEffectsFromEvent = Effect.fn(
      "AgentRunPersistence.persistSideEffectsFromEvent",
    )(function* (path: string, event: unknown): Effect.fn.Return<void, Error> {
      const runStart = Schema.decodeUnknownOption(PersistedRunStartEvent)(event);
      if (Option.isSome(runStart)) yield* markRunStarted(runStart.value.runId);

      const agentEnd = Schema.decodeUnknownOption(PersistedAgentEndEvent)(event);
      if (Option.isSome(agentEnd)) {
        const updatedAt = yield* nowIso();
        yield* persist(
          "save denora session",
          db.client
            .insert(denoraSessions)
            .values({
              id: agentEnd.value.runId,
              data: serializeJson({ path, messages: agentEnd.value.messages, updatedAt }) ?? "null",
            })
            .onConflictDoUpdate({
              target: denoraSessions.id,
              set: {
                data:
                  serializeJson({ path, messages: agentEnd.value.messages, updatedAt }) ?? "null",
              },
            }),
        );
      }

      const runEnd = Schema.decodeUnknownOption(PersistedRunEndEvent)(event);
      if (Option.isSome(runEnd)) {
        yield* finishRun({
          runId: runEnd.value.runId,
          isError: runEnd.value.isError,
          durationMs: runEnd.value.durationMs,
          result: runEnd.value.result,
          error: runEnd.value.error,
        });
      }
    });

    return Service.of({
      registerRun,
      getRunInput,
      authorizeRun: ({ runId, userId }) => authorizeExistingRun(runId, userId),
      markRunStarted,
      finishRun,
      createStream,
      appendStreamEvent,
      closeStream,
    });
  }),
);

export const mirrorEventStreamStore = (
  store: EventStreamStore,
  persistence: Interface,
): EventStreamStore => ({
  createStream: (path) =>
    store
      .createStream(path)
      .pipe(
        Effect.flatMap(() =>
          persistence.createStream(path).pipe(Effect.mapError(toEventStorageFailed)),
        ),
      ),
  appendEvent: (path, event) =>
    store
      .appendEvent(path, event)
      .pipe(
        Effect.flatMap((offset) =>
          persistence
            .appendStreamEvent({ path, event, offset })
            .pipe(Effect.mapError(toEventStorageFailed), Effect.as(offset)),
        ),
      ),
  readEvents: store.readEvents,
  closeStream: (path) =>
    store
      .closeStream(path)
      .pipe(
        Effect.flatMap(() =>
          persistence.closeStream(path).pipe(Effect.mapError(toEventStorageFailed)),
        ),
      ),
  getStreamMeta: store.getStreamMeta,
  subscribe: store.subscribe,
});

const toEventStorageFailed = (cause: Error): EventStreamError =>
  new EventStorageFailed({ operation: "persist mirrored event stream", cause });

const serializeJson = (value: unknown): string | null => JSON.stringify(value) ?? null;

const PromptInput = Schema.Struct({ prompt: Schema.String });

const PersistedRunStartEvent = Schema.StructWithRest(
  Schema.Struct({
    type: Schema.Literal("run_start"),
    runId: Schema.String,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PersistedAgentEndEvent = Schema.StructWithRest(
  Schema.Struct({
    type: Schema.Literal("agent_end"),
    runId: Schema.String,
    messages: Schema.Unknown,
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PersistedRunEndEvent = Schema.StructWithRest(
  Schema.Struct({
    type: Schema.Literal("run_end"),
    runId: Schema.String,
    isError: Schema.Boolean,
    durationMs: Schema.Number,
    result: Schema.Unknown,
    error: Schema.optionalKey(Schema.Unknown),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const normalizeUserMessageContent = (input: unknown): unknown => {
  const promptInput = Schema.decodeUnknownOption(PromptInput)(input);
  if (Option.isSome(promptInput)) return { text: promptInput.value.prompt };
  return { input };
};

export * as AgentRunPersistence from "./AgentRunPersistence.ts";
