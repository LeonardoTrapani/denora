CREATE TABLE "workos_event_cursors" (
	"name" text PRIMARY KEY,
	"last_event_id" text,
	"updated_at" text NOT NULL
);

--> statement-breakpoint
CREATE TABLE "workos_event_sync_locks" (
	"name" text PRIMARY KEY,
	"owner" text NOT NULL,
	"leased_until" text NOT NULL,
	"updated_at" text NOT NULL
);

--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "deleted_at" text;
--> statement-breakpoint
ALTER TABLE "users" ADD COLUMN "workos_deleted_at" text;
