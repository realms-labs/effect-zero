import * as Schema from "effect/Schema";
import { PushFailedBody } from "./error.js";
import { Mutation, MutationResponse } from "./mutation.js";
import { MutationId } from "./mutation-id.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/push.ts

export const PushBody = Schema.Struct({
  clientGroupID: Schema.String,
  mutations: Schema.Array(Mutation),
  pushVersion: Schema.Number,
  // For legacy (CRUD) mutations, the schema is tied to the client group /
  // sync connection. For custom mutations, schema versioning is delegated
  // to the custom protocol / api-server.
  schemaVersion: Schema.optional(Schema.Number),
  timestamp: Schema.Number,
  requestID: Schema.String,
  /**
   * @deprecated auth is managed at client-group scope via connect/updateAuth
   * and should not be included in push messages.
   */
  auth: Schema.optional(Schema.String),
  /** W3C traceparent header for distributed tracing. */
  traceparent: Schema.optional(Schema.String),
});
export type PushBody = typeof PushBody.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushMessage = Schema.Tuple([Schema.Literal("push"), PushBody]);

const PushOk = Schema.Struct({
  mutations: Schema.Array(MutationResponse),
});

/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const UnsupportedPushVersion = Schema.Struct({
  /** @deprecated */
  error: Schema.Literal("unsupportedPushVersion"),
  /** @deprecated */
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
const UnsupportedSchemaVersion = Schema.Struct({
  /** @deprecated */
  error: Schema.Literal("unsupportedSchemaVersion"),
  /** @deprecated */
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

/**
 * @deprecated push http errors are now represented as ['error', { ... }] messages
 */
const HttpError = Schema.Struct({
  /** @deprecated */
  error: Schema.Literal("http"),
  /** @deprecated */
  status: Schema.Number,
  /** @deprecated */
  details: Schema.String,
  /** @deprecated */
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

/**
 * @deprecated push zero errors are now represented as ['error', { ... }] messages
 */
const ZeroPusherError = Schema.Struct({
  /** @deprecated */
  error: Schema.Literal("zeroPusher"),
  /** @deprecated */
  details: Schema.String,
  /** @deprecated */
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

/**
 * @deprecated push errors are now represented as ['error', { ... }] messages
 */
export const PushError = Schema.Union([UnsupportedPushVersion, UnsupportedSchemaVersion, HttpError, ZeroPusherError]);

export const PushResponseBody = Schema.Union([PushOk, PushError]);

export const PushResponse = Schema.Union([PushResponseBody, PushFailedBody]);
export type PushResponse = typeof PushResponse.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushResponseMessage = Schema.Tuple([Schema.Literal("pushResponse"), PushResponseBody]);

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const AckMutationResponsesMessage = Schema.Tuple([Schema.Literal("ackMutationResponses"), MutationId]);
