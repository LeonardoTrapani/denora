import {
  index,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  primaryKey,
  serial,
  text,
  timestamp,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const records = pgTable("records", {
  id: text("id").primaryKey(),
  createdAt: text("created_at").notNull(),
});

export const conversationStatus = pgEnum("conversation_status", [
  "active",
  "archiving",
  "archived",
  "deleting",
  "deleted",
]);

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
    submissionId: text("submission_id"),
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
    uniqueIndex("agent_runs_submission_id_idx").on(table.submissionId),
  ],
);

export const denoraRuns = pgTable(
  "denora_runs",
  {
    runId: text("run_id").primaryKey(),
    workflowName: text("workflow_name"),
    status: text("status").notNull(),
    startedAt: text("started_at").notNull(),
    payload: text("payload"),
    traceparent: text("traceparent"),
    tracestate: text("tracestate"),
    endedAt: text("ended_at"),
    isError: integer("is_error"),
    durationMs: integer("duration_ms"),
    result: text("result"),
    error: text("error"),
  },
  (table) => [
    index("denora_runs_workflow_started_idx").on(table.workflowName, table.startedAt),
    index("denora_runs_status_started_idx").on(table.status, table.startedAt, table.runId),
  ],
);

export const denoraSessions = pgTable("denora_sessions", {
  id: text("id").primaryKey(),
  data: text("data").notNull(),
});

export const denoraSessionEntries = pgTable(
  "denora_session_entries",
  {
    sessionId: text("session_id").notNull(),
    entryId: text("entry_id").notNull(),
    position: integer("position").notNull(),
    data: text("data").notNull(),
  },
  (table) => [
    primaryKey({ columns: [table.sessionId, table.entryId] }),
    index("denora_session_entries_session_position_idx").on(table.sessionId, table.position),
  ],
);

export const denoraAgentTurnJournals = pgTable("denora_agent_turn_journals", {
  submissionId: text("submission_id").primaryKey(),
  sessionKey: text("session_key").notNull(),
  kind: text("kind").notNull(),
  attemptId: text("attempt_id").notNull(),
  operationId: text("operation_id").notNull(),
  turnId: text("turn_id").notNull(),
  phase: text("phase").notNull(),
  revision: integer("revision").notNull(),
  createdAt: integer("created_at").notNull(),
  updatedAt: integer("updated_at").notNull(),
  checkpointLeafId: text("checkpoint_leaf_id"),
  toolRequestJson: text("tool_request_json"),
  streamKey: text("stream_key"),
  streamConsumedAt: integer("stream_consumed_at"),
  committed: integer("committed").notNull().default(0),
  committedLeafId: text("committed_leaf_id"),
});

export const denoraAgentStreamChunks = pgTable(
  "denora_agent_stream_chunks",
  {
    streamKey: text("stream_key").notNull(),
    segmentIndex: integer("segment_index").notNull(),
    body: text("body").notNull(),
  },
  (table) => [primaryKey({ columns: [table.streamKey, table.segmentIndex] })],
);

export const denoraAgentSubmissions = pgTable(
  "denora_agent_submissions",
  {
    sequence: serial("sequence").primaryKey(),
    submissionId: text("submission_id").notNull(),
    sessionKey: text("session_key").notNull(),
    kind: text("kind").notNull(),
    payload: text("payload").notNull(),
    status: text("status").notNull(),
    acceptedAt: integer("accepted_at").notNull(),
    attemptId: text("attempt_id"),
    inputAppliedAt: integer("input_applied_at"),
    recoveryRequestedAt: integer("recovery_requested_at"),
    startedAt: integer("started_at"),
    settledAt: integer("settled_at"),
    error: text("error"),
    attemptCount: integer("attempt_count").notNull().default(0),
    maxRetry: integer("max_retry").notNull().default(3),
    timeoutAt: integer("timeout_at").notNull().default(0),
    ownerId: text("owner_id"),
    leaseExpiresAt: integer("lease_expires_at").notNull().default(0),
    terminalEventKey: text("terminal_event_key"),
    terminalEventJson: text("terminal_event_json"),
    terminalEventOffset: text("terminal_event_offset"),
  },
  (table) => [
    uniqueIndex("denora_agent_submissions_submission_id_idx").on(table.submissionId),
    index("denora_agent_submissions_status_sequence_idx").on(table.status, table.sequence),
    index("denora_agent_submissions_session_status_sequence_idx").on(
      table.sessionKey,
      table.status,
      table.sequence,
    ),
  ],
);

export const denoraAgentSessionDeletions = pgTable("denora_agent_session_deletions", {
  sessionKey: text("session_key").primaryKey(),
  startedAt: integer("started_at").notNull(),
});

export const denoraAgentDispatchReceipts = pgTable("denora_agent_dispatch_receipts", {
  dispatchId: text("dispatch_id").primaryKey(),
  acceptedAt: integer("accepted_at").notNull(),
});

export const denoraAgentAttemptMarkers = pgTable(
  "denora_agent_attempt_markers",
  {
    submissionId: text("submission_id").notNull(),
    attemptId: text("attempt_id").notNull(),
    createdAt: integer("created_at").notNull(),
  },
  (table) => [primaryKey({ columns: [table.submissionId, table.attemptId] })],
);

export * as schema from "./schema.ts";
