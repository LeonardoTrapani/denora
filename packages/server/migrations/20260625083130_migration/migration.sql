ALTER TYPE "conversation_status" ADD VALUE IF NOT EXISTS 'archiving' BEFORE 'archived';
--> statement-breakpoint
ALTER TYPE "conversation_status" ADD VALUE IF NOT EXISTS 'deleting';
--> statement-breakpoint
ALTER TYPE "conversation_status" ADD VALUE IF NOT EXISTS 'deleted';
