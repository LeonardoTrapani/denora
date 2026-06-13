CREATE TABLE "users" (
	"id" text PRIMARY KEY,
	"workos_user_id" text NOT NULL,
	"email" text NOT NULL,
	"email_verified" boolean NOT NULL,
	"name" text,
	"first_name" text,
	"last_name" text,
	"profile_picture_url" text,
	"locale" text,
	"last_sign_in_at" text,
	"workos_created_at" text NOT NULL,
	"workos_updated_at" text NOT NULL,
	"created_at" text NOT NULL,
	"updated_at" text NOT NULL
);
--> statement-breakpoint
CREATE UNIQUE INDEX "users_workos_user_id_unique" ON "users" ("workos_user_id");