import * as Schema from "effect/Schema";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/mutation-id.ts

export const MutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});
export type MutationId = typeof MutationId.Type;
