import { index, jsonb, pgEnum, pgTable, text, timestamp } from "drizzle-orm/pg-core";

export const records = pgTable("records", {
  id: text("id").primaryKey(),
  createdAt: text("created_at").notNull(),
});

export const conversationStatus = pgEnum("conversation_status", ["active", "archived"]);

export const conversationMessageRole = pgEnum("conversation_message_role", [
  "system",
  "user",
  "assistant",
  "tool",
  "event",
]);

export const agentRunStatus = pgEnum("agent_run_status", [
  "queued",
  "running",
  "completed",
  "failed",
  "cancelled",
]);

export const conversations = pgTable(
  "conversations",
  {
    id: text("id").primaryKey(),
    ownerUserId: text("owner_user_id").notNull(),
    agentId: text("agent_id"),
    status: conversationStatus("status").notNull().default("active"),
    title: text("title"),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { mode: "string", withTimezone: true }).notNull(),
    updatedAt: timestamp("updated_at", { mode: "string", withTimezone: true }).notNull(),
    archivedAt: timestamp("archived_at", { mode: "string", withTimezone: true }),
  },
  (table) => [
    index("conversations_owner_user_id_created_at_idx").on(table.ownerUserId, table.createdAt),
    index("conversations_agent_id_created_at_idx").on(table.agentId, table.createdAt),
  ],
);

export const conversationMessages = pgTable(
  "conversation_messages",
  {
    id: text("id").primaryKey(),
    conversationId: text("conversation_id")
      .notNull()
      .references(() => conversations.id, { onDelete: "cascade" }),
    runId: text("run_id"),
    role: conversationMessageRole("role").notNull(),
    content: jsonb("content").notNull(),
    metadata: jsonb("metadata"),
    createdAt: timestamp("created_at", { mode: "string", withTimezone: true }).notNull(),
  },
  (table) => [
    index("conversation_messages_conversation_id_created_at_idx").on(
      table.conversationId,
      table.createdAt,
    ),
    index("conversation_messages_run_id_idx").on(table.runId),
  ],
);

export const agentRuns = pgTable(
  "agent_runs",
  {
    id: text("id").primaryKey(),
    conversationId: text("conversation_id")
      .notNull()
      .references(() => conversations.id, { onDelete: "cascade" }),
    triggerMessageId: text("trigger_message_id").references(() => conversationMessages.id, {
      onDelete: "set null",
    }),
    status: agentRunStatus("status").notNull().default("queued"),
    streamPath: text("stream_path").notNull(),
    input: jsonb("input"),
    result: jsonb("result"),
    error: jsonb("error"),
    createdAt: timestamp("created_at", { mode: "string", withTimezone: true }).notNull(),
    startedAt: timestamp("started_at", { mode: "string", withTimezone: true }),
    endedAt: timestamp("ended_at", { mode: "string", withTimezone: true }),
    updatedAt: timestamp("updated_at", { mode: "string", withTimezone: true }).notNull(),
  },
  (table) => [
    index("agent_runs_conversation_id_created_at_idx").on(table.conversationId, table.createdAt),
    index("agent_runs_status_created_at_idx").on(table.status, table.createdAt),
    index("agent_runs_trigger_message_id_idx").on(table.triggerMessageId),
  ],
);

export * as schema from "./schema.ts";
