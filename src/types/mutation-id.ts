import * as Schema from "effect/Schema";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/c22b300d15435712a4c5e61313645881942f1d3a/packages/zero-protocol/src/mutation-id.ts

export const MutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});
export type MutationId = typeof MutationId.Type;
