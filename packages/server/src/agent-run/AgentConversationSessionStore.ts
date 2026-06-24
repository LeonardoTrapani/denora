import type { AgentMessage } from "@earendil-works/pi-agent-core";
import type * as Cloudflare from "alchemy/Cloudflare";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { ConversationDomain } from "../conversation/ConversationDomain.ts";
import { EventStorageFailed } from "./EventStreamStore.ts";
import { SqlStorage } from "./SqlStorage.ts";

const CREATE_MESSAGES_TABLE = `
CREATE TABLE IF NOT EXISTS denora_agent_conversation_session_messages (
  sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
  message_id      TEXT NOT NULL UNIQUE,
  conversation_id TEXT NOT NULL,
  run_id          TEXT,
  role            TEXT NOT NULL,
  content_json    TEXT NOT NULL,
  metadata_json   TEXT,
  created_at      TEXT NOT NULL
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
  readonly isError: boolean;
  readonly result?: unknown;
}

export interface CompletedAssistantRun {
  readonly assistantText: string;
}

export interface Interface {
  readonly recordSubmissionStarted: (
    input: RecordSubmissionStartedInput,
  ) => Effect.Effect<RecordedSubmissionStarted, EventStorageFailed>;
  readonly finishRun: (input: FinishRunInput) => Effect.Effect<void, EventStorageFailed>;
  readonly reconstructCompletedRun: (input: {
    readonly conversationId: string;
    readonly runId: string;
  }) => Effect.Effect<CompletedAssistantRun | null, EventStorageFailed>;
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
        `SELECT message_id, conversation_id, run_id, role, content_json, metadata_json, created_at
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
      runId: input.runId,
      role: "user",
      content,
      metadata: { submissionId: input.submissionId, agentName: input.agentName },
      createdAt: timestamp,
    });

    const messages = yield* readMessages(input.conversationId);
    const runInput = {
      prompt: "",
      submittedMessage: content,
      messages: messages.flatMap(toAgentMessage),
    };

    return { input: runInput };
  });

  const finishRun = Effect.fn("AgentConversationSessionStore.finishRun")(function* (
    input: FinishRunInput,
  ): Effect.fn.Return<void, EventStorageFailed> {
    if (input.isError) return;
    const timestamp = DateTime.formatIso(yield* DateTime.now);
    yield* insertMessage(sql, {
      messageId: assistantMessageId(input.runId),
      conversationId: input.conversationId,
      runId: input.runId,
      role: "assistant",
      content: { text: ConversationDomain.assistantTextFromResult(input.result) },
      metadata: { source: "agent_submission_result", result: input.result },
      createdAt: timestamp,
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
        `SELECT message_id, conversation_id, run_id, role, content_json, metadata_json, created_at
           FROM denora_agent_conversation_session_messages
          WHERE conversation_id = ? AND message_id = ? AND run_id = ? AND role = 'assistant'
          LIMIT 1`,
        input.conversationId,
        assistantMessageId(input.runId),
        input.runId,
      )
      .pipe(storageFailure("read completed assistant run"));
    const rows = yield* cursor.toArray().pipe(storageFailure("collect completed assistant run"));
    const row = rows[0];
    if (row === undefined) return null;
    const message = yield* parseMessageRow(row);
    return { assistantText: ConversationDomain.promptFromContent(message.content) };
  });

  return { recordSubmissionStarted, finishRun, reconstructCompletedRun } satisfies Interface;
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
  stringify(input.content).pipe(
    Effect.flatMap((contentJson) =>
      stringify(input.metadata).pipe(
        Effect.flatMap((metadataJson) =>
          sql
            .exec(
              `INSERT OR IGNORE INTO denora_agent_conversation_session_messages
                 (message_id, conversation_id, run_id, role, content_json, metadata_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)`,
              input.messageId,
              input.conversationId,
              input.runId ?? null,
              input.role,
              contentJson,
              metadataJson,
              input.createdAt,
            )
            .pipe(storageFailure("insert conversation session message"), Effect.asVoid),
        ),
      ),
    ),
  );

const parseMessageRow = (row: MessageRow): Effect.Effect<MessageRecord, EventStorageFailed> =>
  Effect.try({
    try: () => ({
      messageId: row.message_id,
      conversationId: row.conversation_id,
      runId: row.run_id ?? null,
      role: parseRole(row.role),
      content: JSON.parse(row.content_json) as unknown,
      metadata: row.metadata_json === null ? null : (JSON.parse(row.metadata_json) as unknown),
      createdAt: row.created_at,
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

const toAgentMessage = (message: MessageRecord): ReadonlyArray<AgentMessage> => {
  const text = ConversationDomain.promptFromContent(message.content);
  if (message.role === "user") {
    const rich = ConversationDomain.richUserMessage(message.content, Date.parse(message.createdAt));
    return [rich ?? { role: "user", content: text, timestamp: Date.parse(message.createdAt) }];
  }
  return [
    {
      role: "assistant",
      content: [{ type: "text", text }],
      timestamp: Date.parse(message.createdAt),
    } as AgentMessage,
  ];
};

const assistantMessageId = (runId: string): string => `assistant:${runId}`;

interface MessageRecord {
  readonly messageId: string;
  readonly conversationId: string;
  readonly runId: string | null;
  readonly role: "user" | "assistant";
  readonly content: unknown;
  readonly metadata: unknown;
  readonly createdAt: string;
}

interface MessageRow extends Record<string, Cloudflare.SqlStorageValue> {
  readonly message_id: string;
  readonly conversation_id: string;
  readonly run_id: string | null;
  readonly role: string;
  readonly content_json: string;
  readonly metadata_json: string | null;
  readonly created_at: string;
}

export * as AgentConversationSessionStore from "./AgentConversationSessionStore.ts";
