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
CREATE INDEX "agent_runs_conversation_id_created_at_idx" ON "agent_runs" ("conversation_id","created_at");
--> statement-breakpoint
CREATE INDEX "agent_runs_status_created_at_idx" ON "agent_runs" ("status","created_at");
--> statement-breakpoint
CREATE INDEX "agent_runs_trigger_message_id_idx" ON "agent_runs" ("trigger_message_id");
--> statement-breakpoint
CREATE INDEX "conversation_messages_conversation_id_created_at_idx" ON "conversation_messages" ("conversation_id","created_at");
--> statement-breakpoint
CREATE INDEX "conversation_messages_run_id_idx" ON "conversation_messages" ("run_id");
--> statement-breakpoint
CREATE INDEX "conversations_owner_user_id_created_at_idx" ON "conversations" ("owner_user_id","created_at");
--> statement-breakpoint
CREATE INDEX "conversations_agent_id_created_at_idx" ON "conversations" ("agent_id","created_at");
--> statement-breakpoint
ALTER TABLE "agent_runs" ADD CONSTRAINT "agent_runs_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE;
--> statement-breakpoint
ALTER TABLE "agent_runs" ADD CONSTRAINT "agent_runs_trigger_message_id_conversation_messages_id_fkey" FOREIGN KEY ("trigger_message_id") REFERENCES "conversation_messages"("id") ON DELETE SET NULL;
--> statement-breakpoint
ALTER TABLE "conversation_messages" ADD CONSTRAINT "conversation_messages_conversation_id_conversations_id_fkey" FOREIGN KEY ("conversation_id") REFERENCES "conversations"("id") ON DELETE CASCADE;
