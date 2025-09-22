import { ANYONE_CAN_DO_ANYTHING, definePermissions } from '@rocicorp/zero';
import { schema, type Schema } from './schema.gen';

export { schema, type Schema };

export const permissions = definePermissions<unknown, Schema>(schema, () => ({
  messages: ANYONE_CAN_DO_ANYTHING,
}));
