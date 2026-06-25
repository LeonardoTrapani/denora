import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  assertAgentConversationContentWithinLimits,
  assertAgentConversationJsonWithinLimits,
} from "./AgentConversationContentLimits.ts";
import { ConversationDomain } from "../conversation/ConversationDomain.ts";
import { EventStorageFailed } from "./EventStreamStore.ts";
import { SqlStorage } from "./SqlStorage.ts";

const CREATE_MESSAGES_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_conversation_session_messages (
  sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id      TEXT NOT NULL UNIQUE,
  conversation_id TEXT NOT NULL,
  parent_message_id TEXT,
  run_id          TEXT,
  submission_id   TEXT,
  role            TEXT NOT NULL,
  parts_json      TEXT NOT NULL,
  plain_text      TEXT NOT NULL,
  status          TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  updated_at      TEXT NOT NULL
)`;

const CREATE_MESSAGES_CONVERSATION_INDEX = `
CREATE INDEX IF NOT EXISTS denora_agent_conversation_session_messages_conversation_sequence_idx
ON denora_agent_conversation_session_messages (conversation_id, sequence ASC)`;

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
  readonly conversationId: string;
  readonly runId: string;
  readonly submissionId: string;
  readonly isError: boolean;
  readonly result?: unknown;
}

export interface RecordAssistantMessageStartedInput {
  readonly conversationId: string;
  readonly runId: string;
  readonly submissionId: string;
  readonly messageIndex: number;
}

export interface RecordAssistantTextPartCompletedInput extends RecordAssistantMessageStartedInput {
  readonly contentIndex: number;
  readonly text: string;
}

export interface RecordAssistantMessageCompletedInput extends RecordAssistantMessageStartedInput {
  readonly parts: ReadonlyArray<unknown>;
  readonly plainText: string;
}

export interface RecordToolCallCheckpointInput {
  readonly conversationId: string;
  readonly runId: string;
  readonly submissionId: string;
  readonly toolCallId: string;
  readonly name: string;
  readonly args: unknown;
}

export interface RecordToolResultCheckpointInput {
  readonly conversationId: string;
  readonly runId: string;
  readonly submissionId: string;
  readonly toolCallId: string;
  readonly result: unknown;
  readonly isError: boolean;
}

export interface CompletedAssistantRun {
  readonly assistantText: string;
}

export interface SubmissionProgress {
  readonly inputApplied: boolean;
  readonly assistantStarted: boolean;
  readonly assistantCompleted: CompletedAssistantRun | null;
  readonly toolResultCompletedWithoutAssistant: boolean;
}

export interface Interface {
  readonly recordSubmissionStarted: (
    input: RecordSubmissionStartedInput,
  ) => Effect.Effect<RecordedSubmissionStarted, EventStorageFailed>;
  readonly recordAssistantMessageStarted: (
    input: RecordAssistantMessageStartedInput,
  ) => Effect.Effect<void, EventStorageFailed>;
  readonly recordAssistantTextPartCompleted: (
    input: RecordAssistantTextPartCompletedInput,
  ) => Effect.Effect<void, EventStorageFailed>;
  readonly recordAssistantMessageCompleted: (
    input: RecordAssistantMessageCompletedInput,
  ) => Effect.Effect<void, EventStorageFailed>;
  readonly finishRun: (input: FinishRunInput) => Effect.Effect<void, EventStorageFailed>;
  readonly reconstructCompletedRun: (input: {
    readonly conversationId: string;
    readonly runId: string;
  }) => Effect.Effect<CompletedAssistantRun | null, EventStorageFailed>;
  readonly inspectSubmissionProgress: (input: {
    readonly conversationId: string;
    readonly runId: string;
    readonly submissionId: string;
  }) => Effect.Effect<SubmissionProgress, EventStorageFailed>;
}

export class Service extends Context.Service<Service, Interface>()(
  "@denora/server/AgentConversationSessionStore",
) {}

export const sqliteLayer: Layer.Layer<Service, EventStorageFailed, SqlStorage.Service> =
  Layer.effect(
    Service,
    Effect.gen(function* () {
      const sql = yield* SqlStorage.Service;
      const store = yield* makeSqlite(sql);
      return Service.of(store);
    }),
  );

export const makeSqlite = Effect.fn("AgentConversationSessionStore.makeSqlite")(function* (
  sql: Cloudflare.SqlStorage,
): Effect.fn.Return<Interface, EventStorageFailed> {
  yield* ensureTables(sql);

  const readMessages = Effect.fn("AgentConversationSessionStore.readMessages")(function* (
    conversationId: string,
  ): Effect.fn.Return<ReadonlyArray<MessageRecord>, EventStorageFailed> {
    const cursor = yield* sql
      .exec<MessageRow>(
        `SELECT message_id, conversation_id, parent_message_id, run_id, submission_id, role,
                parts_json, plain_text, status, created_at, updated_at
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ?
          ORDER BY sequence ASC`,
        conversationId,
      )
      .pipe(storageFailure("list conversation session messages"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect conversation session messages"));
    return yield* Effect.forEach(rows, parseMessageRow);
  });

  const recordSubmissionStarted = Effect.fn(
    "AgentConversationSessionStore.recordSubmissionStarted",
  )(function* (
    input: RecordSubmissionStartedInput,
  ): Effect.fn.Return<RecordedSubmissionStarted, EventStorageFailed> {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const content = input.content;

    yield* insertMessage(sql, {
      messageId: input.messageId,
      conversationId: input.conversationId,
      parentMessageId: yield* readLatestMessageId(sql, input.conversationId),
      runId: input.runId,
      submissionId: input.submissionId,
      role: "user",
      parts: partsFromUserContent(content),
      plainText: plainTextFromContent(content),
      status: "completed",
      createdAt: timestamp,
      updatedAt: timestamp,
    });

    const messages = yield* readMessages(input.conversationId);
    const runInput = {
      prompt: "",
      submittedMessage: content,
      messages: messages.flatMap(toAgentMessage),
    };

    return { input: runInput };
  });

  const recordAssistantMessageStarted = Effect.fn(
    "AgentConversationSessionStore.recordAssistantMessageStarted",
  )(function* (
    input: RecordAssistantMessageStartedInput,
  ): Effect.fn.Return<void, EventStorageFailed> {
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    yield* insertMessage(sql, {
      messageId: assistantMessageId(input.runId, input.messageIndex),
      conversationId: input.conversationId,
      parentMessageId: yield* readLatestMessageId(sql, input.conversationId, {
        exceptMessageId: assistantMessageId(input.runId, input.messageIndex),
      }),
      runId: input.runId,
      submissionId: input.submissionId,
      role: "assistant",
      parts: [],
      plainText: "",
      status: "started",
      createdAt: timestamp,
      updatedAt: timestamp,
    });
  });

  const recordAssistantTextPartCompleted = Effect.fn(
    "AgentConversationSessionStore.recordAssistantTextPartCompleted",
  )(function* (
    input: RecordAssistantTextPartCompletedInput,
  ): Effect.fn.Return<void, EventStorageFailed> {
    const messageId = assistantMessageId(input.runId, input.messageIndex);
    const existing = yield* readMessageById(sql, input.conversationId, messageId);
    if (existing?.status === "completed") return;
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    const parts = withCompletedTextPart(existing?.parts ?? [], input.contentIndex, input.text);
    yield* upsertAssistantMessage(sql, {
      messageId,
      conversationId: input.conversationId,
      parentMessageId:
        existing?.parentMessageId ??
        (yield* readLatestMessageId(sql, input.conversationId, { exceptMessageId: messageId })),
      runId: input.runId,
      submissionId: input.submissionId,
      role: "assistant",
      parts,
      plainText: plainTextFromParts(parts),
      status: "partial",
      createdAt: existing?.createdAt ?? timestamp,
      updatedAt: timestamp,
    });
  });

  const recordAssistantMessageCompleted = Effect.fn(
    "AgentConversationSessionStore.recordAssistantMessageCompleted",
  )(function* (
    input: RecordAssistantMessageCompletedInput,
  ): Effect.fn.Return<void, EventStorageFailed> {
    const messageId = assistantMessageId(input.runId, input.messageIndex);
    const existing = yield* readMessageById(sql, input.conversationId, messageId);
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    yield* upsertAssistantMessage(sql, {
      messageId,
      conversationId: input.conversationId,
      parentMessageId:
        existing?.parentMessageId ??
        (yield* readLatestMessageId(sql, input.conversationId, { exceptMessageId: messageId })),
      runId: input.runId,
      submissionId: input.submissionId,
      role: "assistant",
      parts: input.parts,
      plainText: input.plainText,
      status: "completed",
      createdAt: existing?.createdAt ?? timestamp,
      updatedAt: timestamp,
    });
  });

  const finishRun = Effect.fn("AgentConversationSessionStore.finishRun")(function* (
    input: FinishRunInput,
  ): Effect.fn.Return<void, EventStorageFailed> {
    if (input.isError) return;
    const assistantText = ConversationDomain.assistantTextFromResult(input.result);
    yield* recordAssistantMessageCompleted({
      runId: input.runId,
      messageIndex: 0,
      conversationId: input.conversationId,
      submissionId: input.submissionId,
      parts: textParts(assistantText),
      plainText: assistantText,
    });
  });

  const reconstructCompletedRun = Effect.fn(
    "AgentConversationSessionStore.reconstructCompletedRun",
  )(function* (input: {
    readonly conversationId: string;
    readonly runId: string;
  }): Effect.fn.Return<CompletedAssistantRun | null, EventStorageFailed> {
    const cursor = yield* sql
      .exec<MessageRow>(
        `SELECT message_id, conversation_id, parent_message_id, run_id, submission_id, role,
                parts_json, plain_text, status, created_at, updated_at
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ? AND message_id = ? AND run_id = ? AND role = 'assistant'
            AND status = 'completed'
          LIMIT 1`,
        input.conversationId,
        assistantMessageId(input.runId, 0),
        input.runId,
      )
      .pipe(storageFailure("read completed assistant run"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect completed assistant run"));
    const row = rows[0];
    if (row === undefined) return null;
    const message = yield* parseMessageRow(row);
    return { assistantText: message.plainText };
  });

  const inspectSubmissionProgress = Effect.fn(
    "AgentConversationSessionStore.inspectSubmissionProgress",
  )(function* (input: {
    readonly conversationId: string;
    readonly runId: string;
    readonly submissionId: string;
  }): Effect.fn.Return<SubmissionProgress, EventStorageFailed> {
    const cursor = yield* sql
      .exec<MessageRow>(
        `SELECT message_id, conversation_id, parent_message_id, run_id, submission_id, role,
                parts_json, plain_text, status, created_at, updated_at
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ? AND submission_id = ?
          ORDER BY sequence ASC`,
        input.conversationId,
        input.submissionId,
      )
      .pipe(storageFailure("inspect conversation submission progress"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect conversation submission progress"));
    const messages = yield* Effect.forEach(rows, parseMessageRow);
    const inputApplied = messages.some(
      (message) => message.role === "user" && message.status === "completed",
    );
    const assistantMessages = messages.filter(
      (message) => message.runId === input.runId && message.role === "assistant",
    );
    const completed = assistantMessages.find((message) => message.status === "completed");
    return {
      inputApplied,
      assistantStarted: assistantMessages.length > 0,
      assistantCompleted: completed === undefined ? null : { assistantText: completed.plainText },
      // Denora has no tool runtime yet; keep the recovery branch explicit and inert.
      toolResultCompletedWithoutAssistant: false,
    };
  });

  return {
    recordSubmissionStarted,
    recordAssistantMessageStarted,
    recordAssistantTextPartCompleted,
    recordAssistantMessageCompleted,
    finishRun,
    inspectSubmissionProgress,
    reconstructCompletedRun,
  } satisfies Interface;
});

const ensureTables = (sql: Cloudflare.SqlStorage): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    for (const [operation, statement] of [
      ["create conversation session messages table", CREATE_MESSAGES_TABLE],
      ["create conversation session messages index", CREATE_MESSAGES_CONVERSATION_INDEX],
    ] as const) {
      yield* sql.exec(statement).pipe(storageFailure(operation), Effect.asVoid);
    }
  });

const insertMessage = (
  sql: Cloudflare.SqlStorage,
  input: MessageRecord,
): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    yield* validateMessageForPersistence(input);
    const partsJson = yield* stringify(input.parts);
    yield* validateSerializedContent(partsJson, "Conversation session message parts");
    yield* sql
      .exec(
        `INSERT OR IGNORE INTO denora_agent_conversation_session_messages
             (message_id, conversation_id, parent_message_id, run_id, submission_id, role,
              parts_json, plain_text, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        input.messageId,
        input.conversationId,
        input.parentMessageId,
        input.runId,
        input.submissionId,
        input.role,
        partsJson,
        input.plainText,
        input.status,
        input.createdAt,
        input.updatedAt,
      )
      .pipe(storageFailure("insert conversation session message"), Effect.asVoid);
  });

const upsertAssistantMessage = (
  sql: Cloudflare.SqlStorage,
  input: MessageRecord & { readonly role: "assistant" },
): Effect.Effect<void, EventStorageFailed> =>
  Effect.gen(function* () {
    yield* validateMessageForPersistence(input);
    const partsJson = yield* stringify(input.parts);
    yield* validateSerializedContent(partsJson, "Conversation session message parts");
    yield* sql
      .exec(
        `INSERT INTO denora_agent_conversation_session_messages
             (message_id, conversation_id, parent_message_id, run_id, submission_id, role,
              parts_json, plain_text, status, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(message_id) DO UPDATE SET
             parts_json = excluded.parts_json,
             plain_text = excluded.plain_text,
             status = excluded.status,
             updated_at = excluded.updated_at
           WHERE denora_agent_conversation_session_messages.status != 'completed'
              OR excluded.status = 'completed'`,
        input.messageId,
        input.conversationId,
        input.parentMessageId,
        input.runId,
        input.submissionId,
        input.role,
        partsJson,
        input.plainText,
        input.status,
        input.createdAt,
        input.updatedAt,
      )
      .pipe(storageFailure("upsert assistant conversation session message"), Effect.asVoid);
  });

const readMessageById = (
  sql: Cloudflare.SqlStorage,
  conversationId: string,
  messageId: string,
): Effect.Effect<MessageRecord | null, EventStorageFailed> =>
  Effect.gen(function* () {
    const cursor = yield* sql
      .exec<MessageRow>(
        `SELECT message_id, conversation_id, parent_message_id, run_id, submission_id, role,
                parts_json, plain_text, status, created_at, updated_at
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ? AND message_id = ?
          LIMIT 1`,
        conversationId,
        messageId,
      )
      .pipe(storageFailure("read conversation session message"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect conversation session message"));
    const row = rows[0];
    if (row === undefined) return null;
    return yield* parseMessageRow(row);
  });

const readLatestMessageId = (
  sql: Cloudflare.SqlStorage,
  conversationId: string,
  options: { readonly exceptMessageId?: string | undefined } = {},
): Effect.Effect<string | null, EventStorageFailed> =>
  Effect.gen(function* () {
    const cursor = yield* sql
      .exec<LatestMessageRow>(
        `SELECT message_id
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ?
            AND (? IS NULL OR message_id != ?)
          ORDER BY sequence DESC
          LIMIT 1`,
        conversationId,
        options.exceptMessageId ?? null,
        options.exceptMessageId ?? null,
      )
      .pipe(storageFailure("read latest conversation session message"));
    const rows = yield* cursor
      .toArray()
      .pipe(storageFailure("collect latest conversation session message"));
    return rows[0]?.message_id ?? null;
  });

const parseMessageRow = (row: MessageRow): Effect.Effect<MessageRecord, EventStorageFailed> =>
  Effect.try({
    try: () => ({
      messageId: row.message_id,
      conversationId: row.conversation_id,
      parentMessageId: row.parent_message_id ?? null,
      runId: row.run_id ?? null,
      submissionId: row.submission_id ?? null,
      role: parseRole(row.role),
      parts: JSON.parse(row.parts_json) as ReadonlyArray<unknown>,
      plainText: row.plain_text,
      status: row.status,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    }),
    catch: (cause) =>
      new EventStorageFailed({ operation: "parse conversation session message", cause }),
  });

const parseRole = (role: string): MessageRecord["role"] => {
  if (role === "user" || role === "assistant") return role;
  throw new Error(`Unsupported conversation session role ${role}.`);
};

const storageFailure =
  (operation: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, EventStorageFailed, R> =>
    effect.pipe(Effect.mapError((cause) => new EventStorageFailed({ operation, cause })));

const stringify = (value: unknown): Effect.Effect<string, EventStorageFailed> =>
  Effect.try({
    try: () => JSON.stringify(value),
    catch: (cause) =>
      new EventStorageFailed({ operation: "serialize conversation session data", cause }),
  }).pipe(
    Effect.flatMap((data) =>
      data === undefined
        ? Effect.fail(
            new EventStorageFailed({
              operation: "serialize conversation session data",
              cause: new TypeError("Value is not JSON serializable"),
            }),
          )
        : Effect.succeed(data),
    ),
  );

const validateMessageForPersistence = (
  input: Pick<MessageRecord, "parts" | "plainText">,
): Effect.Effect<void, EventStorageFailed> =>
  Effect.try({
    try: () => assertAgentConversationContentWithinLimits(input),
    catch: (cause) =>
      new EventStorageFailed({ operation: "validate conversation session message", cause }),
  });

const validateSerializedContent = (
  json: string,
  label: string,
): Effect.Effect<void, EventStorageFailed> =>
  Effect.try({
    try: () => assertAgentConversationJsonWithinLimits(json, label),
    catch: (cause) =>
      new EventStorageFailed({ operation: "validate conversation session message", cause }),
  });

const toAgentMessage = (message: MessageRecord): ReadonlyArray<AgentMessage> => {
  if (message.role === "user") {
    return [
      {
        role: "user",
        content: userMessageContent(message),
        timestamp: Date.parse(message.createdAt),
      } as AgentMessage,
    ];
  }
  return [
    {
      role: "assistant",
      content: message.parts,
      timestamp: Date.parse(message.createdAt),
    } as AgentMessage,
  ];
};

const userMessageContent = (message: MessageRecord): string | ReadonlyArray<unknown> => {
  if (message.parts.length === 1) {
    const part = message.parts[0];
    if (
      typeof part === "object" &&
      part !== null &&
      (part as { readonly type?: unknown }).type === "text" &&
      typeof (part as { readonly text?: unknown }).text === "string"
    ) {
      return (part as { readonly text: string }).text;
    }
  }
  return message.parts;
};

const partsFromUserContent = (content: unknown): ReadonlyArray<unknown> => {
  const rich = ConversationDomain.richUserMessage(content)?.content;
  return Array.isArray(rich) ? rich : textParts(plainTextFromContent(content));
};

const plainTextFromContent = (content: unknown): string => {
  if (typeof content === "object" && content !== null) {
    const text = (content as { readonly text?: unknown }).text;
    if (typeof text === "string") return text;
  }
  return ConversationDomain.promptFromContent(content);
};

const textParts = (text: string): ReadonlyArray<unknown> => [{ type: "text", text }];

const withCompletedTextPart = (
  parts: ReadonlyArray<unknown>,
  contentIndex: number,
  text: string,
): ReadonlyArray<unknown> => {
  const next = parts.slice();
  while (next.length < contentIndex) next.push({ type: "text", text: "" });
  next[contentIndex] = { type: "text", text };
  return next;
};

const plainTextFromParts = (parts: ReadonlyArray<unknown>): string =>
  parts
    .flatMap((part) =>
      typeof part === "object" &&
      part !== null &&
      (part as { readonly type?: unknown }).type === "text" &&
      typeof (part as { readonly text?: unknown }).text === "string"
        ? [(part as { readonly text: string }).text]
        : [],
    )
    .join("");

export const assistantMessageId = (runId: string, messageIndex: number): string =>
  `assistant:${runId}:${messageIndex}`;

export const toolCallMessageId = (runId: string, toolCallId: string): string =>
  `tool-call:${runId}:${toolCallId}`;

export const toolResultMessageId = (runId: string, toolCallId: string): string =>
  `tool-result:${runId}:${toolCallId}`;

interface MessageRecord {
  readonly messageId: string;
  readonly conversationId: string;
  readonly parentMessageId: string | null;
  readonly runId: string | null;
  readonly submissionId: string | null;
  readonly role: "user" | "assistant";
  readonly parts: ReadonlyArray<unknown>;
  readonly plainText: string;
  readonly status: string;
  readonly createdAt: string;
  readonly updatedAt: string;
}

interface MessageRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly message_id: string;
  readonly conversation_id: string;
  readonly parent_message_id: string | null;
  readonly run_id: string | null;
  readonly submission_id: string | null;
  readonly role: string;
  readonly parts_json: string;
  readonly plain_text: string;
  readonly status: string;
  readonly created_at: string;
  readonly updated_at: string;
}

interface LatestMessageRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly message_id: string;
}

export * as AgentConversationSessionStore from "./AgentConversationSessionStore.ts";
