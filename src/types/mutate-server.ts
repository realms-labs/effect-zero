import * as Schema from "effect/Schema";
import { PushFailedBody } from "./error.js";
import { MutationResponse } from "./mutation.js";
import { PushError } from "./push.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/mutate-server.ts

const LegacyPushSuccess = Schema.Struct({
  mutations: Schema.Array(MutationResponse),
});

const LegacyPushResponse = Schema.Union([LegacyPushSuccess, PushError, PushFailedBody]);

export const MutateSuccess = Schema.Struct({
  kind: Schema.Literal("MutateResponse"),
  userID: Schema.optional(Schema.NullOr(Schema.String)),
  mutations: Schema.Array(MutationResponse),
});
export type MutateSuccess = typeof MutateSuccess.Type;

export const MutateResponse = Schema.Union([
  MutateSuccess,
  PushFailedBody,
  // for backwards compatibility
  LegacyPushResponse,
]);
export type MutateResponse = typeof MutateResponse.Type;

/**
 * The schema for the querystring parameters of the custom mutate endpoint.
 */
export const MutateParams = Schema.Struct({
  schema: Schema.String,
  appID: Schema.String,
});
export type MutateParams = typeof MutateParams.Type;
