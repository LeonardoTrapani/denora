import type { AgentMessage } from "@earendil-works/pi-agent-core";
import { and, asc, desc, eq } from "drizzle-orm";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import { Db } from "../persistence/Db.ts";
import {
  agentRuns,
  conversationMessages,
  conversations,
  denoraRuns,
} from "../persistence/schema.ts";
import { agentStreamPath } from "../agent-run/EventStreamStore.ts";
import { ConversationDomain } from "./ConversationDomain.ts";

export class PersistenceFailed extends Schema.TaggedErrorClass<PersistenceFailed>()(
  "PersistenceFailed",
  {
    operation: Schema.String,
    cause: Schema.Defect(),
  },
) {}

export class ConversationNotAuthorized extends Schema.TaggedErrorClass<ConversationNotAuthorized>()(
  "ConversationNotAuthorized",
  { conversationId: Schema.String },
) {}

export type ConversationLifecycleState =
  | "active"
  | "archiving"
  | "archived"
  | "deleting"
  | "deleted";

export class ConversationNotActive extends Schema.TaggedErrorClass<ConversationNotActive>()(
  "ConversationNotActive",
  {
    conversationId: Schema.String,
    status: Schema.Literals(["archiving", "archived", "deleting", "deleted"]),
  },
) {}

export type Error = PersistenceFailed | ConversationNotAuthorized | ConversationNotActive;

export interface CreateConversationInput {
  readonly userId: string;
  readonly conversationId?: string | undefined;
  readonly agentId?: string | null | undefined;
  readonly title?: string | null | undefined;
  readonly metadata?: unknown;
}

export interface ConversationRecord {
  readonly id: string;
  readonly ownerUserId: string;
  readonly agentId: string | null;
  readonly status: ConversationLifecycleState;
  readonly title: string | null;
  readonly metadata: unknown;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly archivedAt: string | null;
}

export interface ConversationMessageRecord {
  readonly id: string;
  readonly conversationId: string;
  readonly runId: string | null;
  readonly role: "system" | "user" | "assistant" | "tool" | "event";
  readonly content: unknown;
  readonly metadata: unknown;
  readonly createdAt: string;
}

export interface SubmitMessageInput {
  readonly conversationId: string;
  readonly userId: string;
  readonly agentName?: string | undefined;
  readonly message?: string | undefined;
  readonly images?: ReadonlyArray<AgentPromptImage> | undefined;
  readonly content?: unknown;
}

export type AgentPromptImage = ConversationDomain.ImageContent;

export interface SubmittedMessage {
  readonly conversationId: string;
  readonly messageId: string;
  readonly submissionId: string;
  readonly runId: string;
  readonly streamPath: string;
  readonly input: unknown;
}

export interface RecordSubmissionStartedInput {
  readonly conversationId: string;
  readonly userId: string;
  readonly agentName: string;
  readonly messageId: string;
  readonly submissionId: string;
  readonly runId: string;
  readonly content: unknown;
}

export interface RecordedSubmissionStarted {
  readonly input: unknown;
}

export interface FinishRunInput {
  readonly runId: string;
  readonly isError: boolean;
  readonly durationMs: number;
  readonly result?: unknown;
  readonly error?: unknown;
}

export interface Interface {
  readonly createConversation: (
    input: CreateConversationInput,
  ) => Effect.Effect<ConversationRecord, Error>;
  readonly listConversations: (
    userId: string,
  ) => Effect.Effect<ReadonlyArray<ConversationRecord>, Error>;
  readonly listMessages: (input: {
    readonly conversationId: string;
    readonly userId: string;
  }) => Effect.Effect<ReadonlyArray<ConversationMessageRecord>, Error>;
  readonly submitMessage: (input: SubmitMessageInput) => Effect.Effect<SubmittedMessage, Error>;
  readonly recordSubmissionStarted: (
    input: RecordSubmissionStartedInput,
  ) => Effect.Effect<RecordedSubmissionStarted, Error>;
  readonly authorizeConversation: (input: {
    readonly conversationId: string;
    readonly userId: string;
  }) => Effect.Effect<ConversationRecord, Error>;
  readonly setConversationLifecycle: (input: {
    readonly conversationId: string;
    readonly userId: string;
    readonly status: ConversationLifecycleState;
  }) => Effect.Effect<ConversationRecord, Error>;
  readonly archiveConversation: (input: {
    readonly conversationId: string;
    readonly userId: string;
  }) => Effect.Effect<ConversationRecord, Error>;
  readonly deleteConversation: (input: {
    readonly conversationId: string;
    readonly userId: string;
  }) => Effect.Effect<ConversationRecord, Error>;
  readonly finishRun: (input: FinishRunInput) => Effect.Effect<void, Error>;
  readonly markRunStarted: (runId: string) => Effect.Effect<void, Error>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/ConversationPersistence",
) {}

export const layer: Layer.Layer<Service, never, Db.Service> = Layer.effect(
  Service,
  Effect.gen(function* () {
    const db = yield* Db.Service;

    const persist = <A>(operation: string, effect: Effect.Effect<A, unknown>) =>
      effect.pipe(Effect.mapError((cause) => new PersistenceFailed({ operation, cause })));
    const nowIso = () => DateTime.now.pipe(Effect.map(DateTime.formatIso));

    const findConversation = Effect.fn("ConversationPersistence.findConversation")(function* (
      conversationId: string,
      userId: string,
    ): Effect.fn.Return<ConversationRecord | undefined, Error> {
      const rows = yield* persist(
        "find conversation",
        db.client
          .select()
          .from(conversations)
          .where(and(eq(conversations.id, conversationId), eq(conversations.ownerUserId, userId)))
          .limit(1),
      );
      return rows[0] as ConversationRecord | undefined;
    });

    const authorizeConversation = Effect.fn("ConversationPersistence.authorizeConversation")(
      function* (input: {
        readonly conversationId: string;
        readonly userId: string;
      }): Effect.fn.Return<ConversationRecord, Error> {
        const found = yield* findConversation(input.conversationId, input.userId);
        if (found === undefined)
          return yield* new ConversationNotAuthorized({ conversationId: input.conversationId });
        return found;
      },
    );

    const authorizeActiveConversation = Effect.fn(
      "ConversationPersistence.authorizeActiveConversation",
    )(function* (input: {
      readonly conversationId: string;
      readonly userId: string;
    }): Effect.fn.Return<ConversationRecord, Error> {
      const found = yield* findConversation(input.conversationId, input.userId);
      if (found === undefined)
        return yield* new ConversationNotAuthorized({ conversationId: input.conversationId });
      if (found.status !== "active")
        return yield* new ConversationNotActive({
          conversationId: input.conversationId,
          status: found.status,
        });
      return found;
    });

    const createConversation = Effect.fn("ConversationPersistence.createConversation")(function* (
      input: CreateConversationInput,
    ): Effect.fn.Return<ConversationRecord, Error> {
      const conversationId = input.conversationId ?? ConversationDomain.makeConversationId();
      const existing = yield* findConversation(conversationId, input.userId);
      if (existing !== undefined) return existing;

      const timestamp = yield* nowIso();
      yield* persist(
        "create conversation",
        db.client
          .insert(conversations)
          .values({
            id: conversationId,
            ownerUserId: input.userId,
            agentId: input.agentId ?? null,
            status: "active",
            title: input.title ?? null,
            metadata: input.metadata ?? null,
            createdAt: timestamp,
            updatedAt: timestamp,
          })
          .onConflictDoNothing(),
      );

      const created = yield* findConversation(conversationId, input.userId);
      if (created === undefined)
        return yield* new PersistenceFailed({
          operation: "read created conversation",
          cause: new Error("Conversation insert succeeded without a readable row."),
        });
      return created;
    });

    const listConversations = Effect.fn("ConversationPersistence.listConversations")(function* (
      userId: string,
    ): Effect.fn.Return<ReadonlyArray<ConversationRecord>, Error> {
      const rows = yield* persist(
        "list conversations",
        db.client
          .select()
          .from(conversations)
          .where(eq(conversations.ownerUserId, userId))
          .orderBy(desc(conversations.updatedAt)),
      );
      return rows as ReadonlyArray<ConversationRecord>;
    });

    const listMessages = Effect.fn("ConversationPersistence.listMessages")(function* (input: {
      readonly conversationId: string;
      readonly userId: string;
    }): Effect.fn.Return<ReadonlyArray<ConversationMessageRecord>, Error> {
      yield* authorizeConversation(input);
      const rows = yield* readMessages(input.conversationId);
      return rows;
    });

    const readMessages = Effect.fn("ConversationPersistence.readMessages")(function* (
      conversationId: string,
    ): Effect.fn.Return<ReadonlyArray<ConversationMessageRecord>, Error> {
      const rows = yield* persist(
        "list conversation messages",
        db.client
          .select()
          .from(conversationMessages)
          .where(eq(conversationMessages.conversationId, conversationId))
          .orderBy(asc(conversationMessages.createdAt)),
      );
      return rows as ReadonlyArray<ConversationMessageRecord>;
    });

    const submitMessage = Effect.fn("ConversationPersistence.submitMessage")(function* (
      input: SubmitMessageInput,
    ): Effect.fn.Return<SubmittedMessage, Error> {
      const agentName = input.agentName ?? "default";
      const conversation = yield* createConversation({
        conversationId: input.conversationId,
        userId: input.userId,
        agentId: agentName,
      });
      if (conversation.status !== "active")
        return yield* new ConversationNotActive({
          conversationId: input.conversationId,
          status: conversation.status,
        });
      const messageId = ConversationDomain.makeMessageId();
      const submissionId = ConversationDomain.makeSubmissionId();
      const runId = ConversationDomain.makeRunId();
      const streamPath = agentStreamPath(agentName, input.conversationId);
      const content = normalizeUserContent(input);

      return {
        conversationId: input.conversationId,
        messageId,
        submissionId,
        runId,
        streamPath,
        input: {
          agentName,
          userId: input.userId,
          submittedMessage: content,
        },
      };
    });

    const recordSubmissionStarted = Effect.fn("ConversationPersistence.recordSubmissionStarted")(
      function* (
        input: RecordSubmissionStartedInput,
      ): Effect.fn.Return<RecordedSubmissionStarted, Error> {
        yield* authorizeActiveConversation(input);
        const priorMessages = yield* readMessages(input.conversationId);
        const timestamp = yield* nowIso();
        const content = input.content;
        const prompt = ConversationDomain.promptFromContent(content);
        const runInput = {
          prompt: ConversationDomain.richUserMessage(content) === undefined ? prompt : "",
          submittedMessage: content,
          messages: [...priorMessages.flatMap(toAgentMessage), ...currentUserMessages(content)],
          ...ConversationDomain.runSettingsFromSubmitted(content),
        };

        yield* persist(
          "create user conversation message",
          db.client
            .insert(conversationMessages)
            .values({
              id: input.messageId,
              conversationId: input.conversationId,
              runId: input.runId,
              role: "user",
              content,
              metadata: { submissionId: input.submissionId, agentName: input.agentName },
              createdAt: timestamp,
            })
            .onConflictDoNothing(),
        );

        yield* persist(
          "create conversation agent run",
          db.client
            .insert(agentRuns)
            .values({
              id: input.runId,
              conversationId: input.conversationId,
              triggerMessageId: input.messageId,
              submissionId: input.submissionId,
              status: "queued",
              streamPath: agentStreamPath(input.agentName, input.conversationId),
              input: runInput,
              createdAt: timestamp,
              updatedAt: timestamp,
            })
            .onConflictDoNothing(),
        );

        yield* persist(
          "create conversation denora run",
          db.client
            .insert(denoraRuns)
            .values({
              runId: input.runId,
              workflowName: "denora.agent-conversation",
              status: "active",
              startedAt: timestamp,
              payload: serializeJson(runInput),
            })
            .onConflictDoNothing(),
        );

        yield* persist(
          "touch conversation",
          db.client
            .update(conversations)
            .set({ updatedAt: timestamp })
            .where(eq(conversations.id, input.conversationId)),
        );

        return { input: runInput };
      },
    );

    const markRunStarted = Effect.fn("ConversationPersistence.markRunStarted")(function* (
      runId: string,
    ): Effect.fn.Return<void, Error> {
      const timestamp = yield* nowIso();
      yield* persist(
        "mark conversation run started",
        db.client
          .update(agentRuns)
          .set({ status: "running", startedAt: timestamp, updatedAt: timestamp })
          .where(eq(agentRuns.id, runId)),
      );
    });

    const finishRun = Effect.fn("ConversationPersistence.finishRun")(function* (
      input: FinishRunInput,
    ): Effect.fn.Return<void, Error> {
      const timestamp = yield* nowIso();
      const rows = yield* persist(
        "find conversation run",
        db.client.select().from(agentRuns).where(eq(agentRuns.id, input.runId)).limit(1),
      );
      const row = rows[0];
      if (row === undefined)
        return yield* new PersistenceFailed({
          operation: "find conversation run",
          cause: new Error(`Conversation run ${input.runId} was not found.`),
        });

      const conversationRows = yield* persist(
        "find run conversation",
        db.client
          .select()
          .from(conversations)
          .where(eq(conversations.id, row.conversationId))
          .limit(1),
      );
      const conversation = conversationRows[0] as ConversationRecord | undefined;
      const inactive = conversation !== undefined && conversation.status !== "active";
      const discardedError =
        inactive && conversation !== undefined
          ? {
              message: `Conversation ${row.conversationId} is ${conversation.status}; run output was discarded.`,
            }
          : undefined;

      yield* persist(
        "finish conversation run",
        db.client
          .update(agentRuns)
          .set({
            status: inactive ? "cancelled" : input.isError ? "failed" : "completed",
            result: inactive ? null : input.result,
            error: inactive ? discardedError : input.error,
            endedAt: timestamp,
            updatedAt: timestamp,
          })
          .where(eq(agentRuns.id, input.runId)),
      );

      yield* persist(
        "finish conversation denora run",
        db.client
          .update(denoraRuns)
          .set({
            status: input.isError ? "errored" : "completed",
            endedAt: timestamp,
            isError: input.isError ? 1 : 0,
            durationMs: Math.trunc(input.durationMs),
            result: inactive ? null : serializeJson(input.result),
            error: serializeJson(inactive ? discardedError : input.error),
          })
          .where(eq(denoraRuns.runId, input.runId)),
      );

      if (!input.isError && !inactive) {
        yield* persist(
          "create assistant conversation message",
          db.client.insert(conversationMessages).values({
            id: ConversationDomain.makeMessageId(),
            conversationId: row.conversationId,
            runId: input.runId,
            role: "assistant",
            content: { text: ConversationDomain.assistantTextFromResult(input.result) },
            metadata: { source: "agent_submission_result" },
            createdAt: timestamp,
          }),
        );
      }

      if (!inactive) {
        yield* persist(
          "touch conversation after run",
          db.client
            .update(conversations)
            .set({ updatedAt: timestamp })
            .where(eq(conversations.id, row.conversationId)),
        );
      }
    });

    const setConversationLifecycle = Effect.fn("ConversationPersistence.setConversationLifecycle")(
      function* (input: {
        readonly conversationId: string;
        readonly userId: string;
        readonly status: ConversationLifecycleState;
      }): Effect.fn.Return<ConversationRecord, Error> {
        const existing = yield* findConversation(input.conversationId, input.userId);
        if (existing === undefined)
          return yield* new ConversationNotAuthorized({ conversationId: input.conversationId });
        const timestamp = yield* nowIso();
        yield* persist(
          "set conversation lifecycle",
          db.client
            .update(conversations)
            .set({
              status: input.status,
              updatedAt: timestamp,
              archivedAt:
                input.status === "archiving" || input.status === "archived"
                  ? (existing.archivedAt ?? timestamp)
                  : existing.archivedAt,
            })
            .where(
              and(
                eq(conversations.id, input.conversationId),
                eq(conversations.ownerUserId, input.userId),
              ),
            ),
        );
        const updated = yield* findConversation(input.conversationId, input.userId);
        if (updated === undefined)
          return yield* new PersistenceFailed({
            operation: "read lifecycle-updated conversation",
            cause: new Error("Conversation lifecycle update succeeded without a readable row."),
          });
        return updated;
      },
    );

    const archiveConversation = Effect.fn("ConversationPersistence.archiveConversation")(
      function* (input: {
        readonly conversationId: string;
        readonly userId: string;
      }): Effect.fn.Return<ConversationRecord, Error> {
        const existing = yield* findConversation(input.conversationId, input.userId);
        if (existing === undefined)
          return yield* new ConversationNotAuthorized({ conversationId: input.conversationId });
        return yield* setConversationLifecycle({
          ...input,
          status: archiveTargetStatus(existing.status),
        });
      },
    );

    const deleteConversation = Effect.fn("ConversationPersistence.deleteConversation")(
      function* (input: {
        readonly conversationId: string;
        readonly userId: string;
      }): Effect.fn.Return<ConversationRecord, Error> {
        return yield* setConversationLifecycle({ ...input, status: "deleted" });
      },
    );

    return Service.of({
      createConversation,
      listConversations,
      listMessages,
      submitMessage,
      recordSubmissionStarted,
      authorizeConversation,
      setConversationLifecycle,
      archiveConversation,
      deleteConversation,
      markRunStarted,
      finishRun,
    });
  }),
);

const archiveTargetStatus = (status: ConversationLifecycleState): ConversationLifecycleState => {
  switch (status) {
    case "deleting":
    case "deleted":
      return status;
    case "active":
    case "archiving":
    case "archived":
      return "archived";
  }
};

const serializeJson = (value: unknown): string | null => JSON.stringify(value) ?? null;

const normalizeUserContent = (input: SubmitMessageInput): unknown => {
  if (input.content !== undefined) return input.content;
  return { text: input.message ?? "", ...(input.images ? { images: input.images } : {}) };
};

const toAgentMessage = (message: ConversationMessageRecord): ReadonlyArray<AgentMessage> => {
  if (message.role !== "user" && message.role !== "assistant") return [];
  const text = ConversationDomain.promptFromContent(message.content);
  if (message.role === "user")
    return [{ role: "user", content: text, timestamp: Date.parse(message.createdAt) }];
  return [
    {
      role: "assistant",
      content: [{ type: "text", text }],
      timestamp: Date.parse(message.createdAt),
    } as AgentMessage,
  ];
};

const currentUserMessages = (content: unknown): ReadonlyArray<AgentMessage> => {
  const rich = ConversationDomain.richUserMessage(content);
  if (rich !== undefined) return [rich];
  return [];
};

export * as ConversationPersistence from "./ConversationPersistence.ts";
