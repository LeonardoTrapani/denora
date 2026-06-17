import { pgTable, text } from "drizzle-orm/pg-core";

export const records = pgTable("records", {
  id: text("id").primaryKey(),
  createdAt: text("created_at").notNull(),
});

export * as schema from "./schema.ts";
