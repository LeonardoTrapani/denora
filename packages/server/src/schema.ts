import { pgTable, text, uniqueIndex } from "drizzle-orm/pg-core";

export const records = pgTable("records", {
  id: text("id").primaryKey(),
  createdAt: text("created_at").notNull(),
});

export const agents = pgTable(
  "agents",
  {
    id: text("id").primaryKey(),
    userId: text("user_id").notNull(),
    name: text("name").notNull(),
    handle: text("handle").notNull(),
    createdAt: text("created_at").notNull(),
  },
  (table) => [uniqueIndex("agents_user_handle_unique").on(table.userId, table.handle)],
);
