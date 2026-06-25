CREATE TYPE "agent_run_status" AS ENUM('queued', 'running', 'completed', 'failed', 'cancelled');
--> statement-breakpoint
CREATE TYPE "conversation_message_role" AS ENUM('system', 'user', 'assistant', 'tool', 'event');
--> statement-breakpoint
CREATE TYPE "conversation_status" AS ENUM('active', 'archiving', 'archived', 'deleting', 'deleted');
--> statement-breakpoint
CREATE TABLE "agent_runs" (
	"id" text PRIMARY KEY,
	"conversation_id" text NOT NULL,
	"trigger_message_id" text,
	"submission_id" text,
	"status" "agent_run_status" DEFAULT 'queued'::"agent_run_status" NOT NULL,
	"stream_path" text NOT NULL,
	"input" jsonb,
	"result" jsonb,
	"error" jsonb,
	"created_at" timestamp with time zone NOT NULL,
	"started_at" timestamp with time zone,
	"ended_at" timestamp with time zone,
	"updated_at" timestamp with time zone NOT NULL
);

--> statement-breakpoint
CREATE TABLE "conversation_messages" (
	"id" text PRIMARY KEY,
	"conversation_id" text NOT NULL,
	"run_id" text,
	"role" "conversation_message_role" NOT NULL,
	"content" jsonb NOT NULL,
	"metadata" jsonb,
	"created_at" timestamp with time zone NOT NULL
);

--> statement-breakpoint
CREATE TABLE "conversations" (
	"id" text PRIMARY KEY,
	"owner_user_id" text NOT NULL,
	"agent_id" text,
	"status" "conversation_status" DEFAULT 'active'::"conversation_status" NOT NULL,
	"title" text,
	"metadata" jsonb,
	"created_at" timestamp with time zone NOT NULL,
	"updated_at" timestamp with time zone NOT NULL,
	"archived_at" timestamp with time zone
);

--> statement-breakpoint
CREATE TABLE "denora_agent_attempt_markers" (
	"submission_id" text,
	"attempt_id" text,
	"created_at" integer NOT NULL,
	CONSTRAINT "denora_agent_attempt_markers_pkey" PRIMARY KEY("submission_id","attempt_id")
);

--> statement-breakpoint
CREATE TABLE "denora_agent_dispatch_receipts" (
	"dispatch_id" text PRIMARY KEY,
	"accepted_at" integer NOT NULL
);

--> statement-breakpoint
CREATE TABLE "denora_agent_session_deletions" (
	"session_key" text PRIMARY KEY,
	"started_at" integer NOT NULL
);

--> statement-breakpoint
CREATE TABLE "denora_agent_stream_chunks" (
	"stream_key" text,
	"segment_index" integer,
	"body" text NOT NULL,
	CONSTRAINT "denora_agent_stream_chunks_pkey" PRIMARY KEY("stream_key","segment_index")
);

--> statement-breakpoint
CREATE TABLE "denora_agent_submissions" (
	"sequence" serial PRIMARY KEY,
	"submission_id" text NOT NULL,
	"session_key" text NOT NULL,
	"kind" text NOT NULL,
	"payload" text NOT NULL,
	"status" text NOT NULL,
	"accepted_at" integer NOT NULL,
	"attempt_id" text,
	"input_applied_at" integer,
	"recovery_requested_at" integer,
	"started_at" integer,
	"settled_at" integer,
	"error" text,
	"attempt_count" integer DEFAULT 0 NOT NULL,
	"max_retry" integer DEFAULT 3 NOT NULL,
	"timeout_at" integer DEFAULT 0 NOT NULL,
	"owner_id" text,
	"lease_expires_at" integer DEFAULT 0 NOT NULL,
	"terminal_event_key" text,
	"terminal_event_json" text,
	"terminal_event_offset" text
);

--> statement-breakpoint
CREATE TABLE "denora_agent_turn_journals" (
	"submission_id" text PRIMARY KEY,
	"session_key" text NOT NULL,
	"kind" text NOT NULL,
	"attempt_id" text NOT NULL,
	"operation_id" text NOT NULL,
	"turn_id" text NOT NULL,
	"phase" text NOT NULL,
	"revision" integer NOT NULL,
	"created_at" integer NOT NULL,
	"updated_at" integer NOT NULL,
	"checkpoint_leaf_id" text,
	"tool_request_json" text,
	"stream_key" text,
	"stream_consumed_at" integer,
	"committed" integer DEFAULT 0 NOT NULL,
	"committed_leaf_id" text
);

--> statement-breakpoint
CREATE TABLE "denora_runs" (
	"run_id" text PRIMARY KEY,
	"workflow_name" text,
	"status" text NOT NULL,
	"started_at" text NOT NULL,
	"payload" text,
	"traceparent" text,
	"tracestate" text,
	"ended_at" text,
	"is_error" integer,
	"duration_ms" integer,
	"result" text,
	"error" text
);

--> statement-breakpoint
CREATE TABLE "denora_session_entries" (
	"session_id" text,
	"entry_id" text,
	"position" integer NOT NULL,
	"data" text NOT NULL,
	CONSTRAINT "denora_session_entries_pkey" PRIMARY KEY("session_id","entry_id")
);

--> statement-breakpoint
CREATE TABLE "denora_sessions" (
	"id" text PRIMARY KEY,
	"data" text NOT NULL
);

--> statement-breakpoint
CREATE TABLE "records" (
	"id" text PRIMARY KEY,
	"created_at" text NOT NULL
);

--> statement-breakpoint
CREATE INDEX "agent_runs_conversation_id_created_at_idx" ON "agent_runs" ("conversation_id","created_at");
--> statement-breakpoint
CREATE INDEX "agent_runs_status_created_at_idx" ON "agent_runs" ("status","created_at");
--> statement-breakpoint
CREATE INDEX "agent_runs_trigger_message_id_idx" ON "agent_runs" ("trigger_message_id");
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_runs_submission_id_idx" ON "agent_runs" ("submission_id");
--> statement-breakpoint
CREATE INDEX "conversation_messages_conversation_id_created_at_idx" ON "conversation_messages" ("conversation_id","created_at");
--> statement-breakpoint
CREATE INDEX "conversation_messages_run_id_idx" ON "conversation_messages" ("run_id");
--> statement-breakpoint
CREATE INDEX "conversations_owner_user_id_created_at_idx" ON "conversations" ("owner_user_id","created_at");
--> statement-breakpoint
CREATE INDEX "conversations_agent_id_created_at_idx" ON "conversations" ("agent_id","created_at");
--> statement-breakpoint
CREATE UNIQUE INDEX "denora_agent_submissions_submission_id_idx" ON "denora_agent_submissions" ("submission_id");
--> statement-breakpoint
CREATE INDEX "denora_agent_submissions_status_sequence_idx" ON "denora_agent_submissions" ("status","sequence");
--> statement-breakpoint
CREATE INDEX "denora_agent_submissions_session_status_sequence_idx" ON "denora_agent_submissions" ("session_key","status","sequence");
--> statement-breakpoint
CREATE INDEX "denora_runs_workflow_started_idx" ON "denora_runs" ("workflow_name","started_at");
--> statement-breakpoint
CREATE INDEX "denora_runs_status_started_idx" ON "denora_runs" ("status","started_at","run_id");
--> statement-breakpoint
CREATE INDEX "denora_session_entries_session_position_idx" ON "denora_session_entries" ("session_id","position");
--> statement-breakpoint
ALTER TABLE "agent_runs" ADD CONSTRAINT "agent_runs_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE;
--> statement-breakpoint
ALTER TABLE "agent_runs" ADD CONSTRAINT "agent_runs_trigger_message_id_conversation_messages_id_fkey" FOREIGN KEY ("trigger_message_id") REFERENCES "conversation_messages"("id") ON DELETE SET NULL;
--> statement-breakpoint
ALTER TABLE "conversation_messages" ADD CONSTRAINT "conversation_messages_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE;
