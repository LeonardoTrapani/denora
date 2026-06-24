ALTER TABLE "agent_runs" ADD COLUMN "submission_id" text;
--> statement-breakpoint
CREATE UNIQUE INDEX "agent_runs_submission_id_idx" ON "agent_runs" ("submission_id");
