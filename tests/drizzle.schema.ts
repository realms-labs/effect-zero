import { pgTable, text } from "drizzle-orm/pg-core";

export const messages = pgTable('messages', {
  id: text().primaryKey(),
  body: text().notNull(),
});
