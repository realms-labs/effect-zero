import type { Schema as ZeroSchema } from "@rocicorp/zero";
import { schema as schemaGen } from "./schema.gen";

export const schema = {
  ...schemaGen,
} as const satisfies ZeroSchema;

export type Schema = typeof schema;
