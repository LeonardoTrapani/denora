CREATE TABLE "agents" (
	"id" text PRIMARY KEY,
	"user_id" text NOT NULL,
	"name" text NOT NULL,
	"handle" text NOT NULL,
	"created_at" text NOT NULL
);

--> statement-breakpoint
CREATE TABLE "records" (
	"id" text PRIMARY KEY,
	"created_at" text NOT NULL
);

--> statement-breakpoint
CREATE UNIQUE INDEX "agents_user_handle_unique" ON "agents" ("user_id","handle");
