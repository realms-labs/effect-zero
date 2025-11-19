import { ANYONE_CAN_DO_ANYTHING, definePermissions, type Schema as ZeroSchema } from "@rocicorp/zero";
import { schema as schemaGen } from "./schema.gen";

export const schema = {
  ...schemaGen,
  enableLegacyQueries: true,
  enableLegacyMutators: true,
} as const satisfies ZeroSchema;

export type Schema = typeof schema;

export const permissions = definePermissions<unknown, Schema>(schema, () => ({
  messages: ANYONE_CAN_DO_ANYTHING,
}));
