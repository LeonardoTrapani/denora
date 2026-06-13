import { boolean, pgTable, text, uniqueIndex } from "drizzle-orm/pg-core";

export const records = pgTable("records", {
  id: text("id").primaryKey(),
  createdAt: text("created_at").notNull(),
});

export const users = pgTable(
  "users",
  {
    id: text("id").primaryKey(),
    workosUserId: text("workos_user_id").notNull(),
    email: text("email").notNull(),
    emailVerified: boolean("email_verified").notNull(),
    name: text("name"),
    firstName: text("first_name"),
    lastName: text("last_name"),
    profilePictureUrl: text("profile_picture_url"),
    locale: text("locale"),
    lastSignInAt: text("last_sign_in_at"),
    workosCreatedAt: text("workos_created_at").notNull(),
    workosUpdatedAt: text("workos_updated_at").notNull(),
    createdAt: text("created_at").notNull(),
    updatedAt: text("updated_at").notNull(),
  },
  (table) => [uniqueIndex("users_workos_user_id_unique").on(table.workosUserId)],
);

export * as schema from "./schema.ts";
