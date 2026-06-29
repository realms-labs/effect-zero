import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";
import { PushFailedBody } from "./error.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/zero-protocol/src/push.ts

const PrimaryKey = Schema.Array(Schema.String);

const PrimaryKeyValue = Schema.Union([Schema.String, Schema.Number, Schema.Boolean]);

const PrimaryKeyValueRecord = Schema.Record(Schema.String, PrimaryKeyValue);

const Value = Schema.Union([JsonSchema, Schema.Undefined]);
const Row = Schema.Record(Schema.String, Value);

const CRUD_MUTATION_NAME = "_zero_crud";

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const CLEANUP_RESULTS_MUTATION_NAME = "_zero_cleanupResults";

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const CleanupResultsArg = Schema.Union([
  // Legacy format (no type field) - treat as single
  Schema.Struct({
    clientGroupID: Schema.String,
    clientID: Schema.String,
    upToMutationID: Schema.Number,
  }),
  // Explicit single: delete up to a specific mutation ID for one client
  Schema.Struct({
    type: Schema.Literal("single"),
    clientGroupID: Schema.String,
    clientID: Schema.String,
    upToMutationID: Schema.Number,
  }),
  // Bulk: delete all mutations for multiple clients
  Schema.Struct({
    type: Schema.Literal("bulk"),
    clientGroupID: Schema.String,
    clientIDs: Schema.NonEmptyArray(Schema.String),
  }),
]);

/**
 * Inserts if entity with id does not already exist.
 */
const InsertOp = Schema.Struct({
  op: Schema.Literal("insert"),
  tableName: Schema.String,
  primaryKey: PrimaryKey,
  value: Row,
});

/**
 * Upsert semantics. Inserts if entity with id does not already exist,
 * otherwise updates existing entity with id.
 */
const UpsertOp = Schema.Struct({
  op: Schema.Literal("upsert"),
  tableName: Schema.String,
  primaryKey: PrimaryKey,
  value: Row,
});

/**
 * Updates if entity with id exists, otherwise does nothing.
 */
const UpdateOp = Schema.Struct({
  op: Schema.Literal("update"),
  tableName: Schema.String,
  primaryKey: PrimaryKey,
  // Partial value with at least the primary key fields
  value: Row,
});

/**
 * Deletes entity with id if it exists, otherwise does nothing.
 */
const DeleteOp = Schema.Struct({
  op: Schema.Literal("delete"),
  tableName: Schema.String,
  primaryKey: PrimaryKey,
  // Partial value representing the primary key
  value: PrimaryKeyValueRecord,
});

const CrudOp = Schema.Union([InsertOp, UpsertOp, UpdateOp, DeleteOp]);

const CrudArg = Schema.Struct({
  ops: Schema.Array(CrudOp),
});

const CrudArgs = Schema.Tuple([CrudArg]);

const CrudMutation = Schema.Struct({
  type: Schema.Literal("crud"),
  id: Schema.Number,
  clientID: Schema.String,
  name: Schema.Literal(CRUD_MUTATION_NAME),
  args: CrudArgs,
  timestamp: Schema.Number,
});

const CustomMutation = Schema.Struct({
  type: Schema.Literal("custom"),
  id: Schema.Number,
  clientID: Schema.String,
  name: Schema.String,
  args: Schema.Array(JsonSchema),
  timestamp: Schema.Number,
});

export const Mutation = Schema.Union([CrudMutation, CustomMutation]);
export type Mutation = typeof Mutation.Type;

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
});
export type PushBody = typeof PushBody.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushMessage = Schema.Tuple([Schema.Literal("push"), PushBody]);

const MutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});

export const AppError = Schema.Struct({
  error: Schema.Literal("app"),
  // The user can return any additional data here
  message: Schema.optional(Schema.String),
  details: Schema.optional(JsonSchema),
});

export type AppError = typeof AppError.Type;

export const ZeroError = Schema.Struct({
  error: Schema.Literals([
    /** @deprecated push oooMutation errors are now represented as ['error', { ... }] messages */
    "oooMutation",
    "alreadyProcessed",
  ]),
  details: Schema.optional(JsonSchema),
});

export type ZeroError = typeof ZeroError.Type;

const MutationOk = Schema.Struct({
  // The user can return any additional data here
  data: Schema.optional(JsonSchema),
});

const MutationError = Schema.Union([AppError, ZeroError]);

export const MutationResult = Schema.Union([
  // We flip the original order here as otherwise values of type MutationError would get parsed as MutationOk with empty `data`
  MutationError,
  MutationOk,
]);
export type MutationResult = typeof MutationResult.Type;

export const MutationResponse = Schema.Struct({
  id: MutationId,
  result: MutationResult,
});
export type MutationResponse = typeof MutationResponse.Type;

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
const PushError = Schema.Union([UnsupportedPushVersion, UnsupportedSchemaVersion, HttpError, ZeroPusherError]);

const PushResponseBody = Schema.Union([PushOk, PushError]);

/**
 * The success response shape introduced in Zero 1.5 (the push endpoint became `/mutate`).
 * `handleMutateRequest` now always emits `{ kind: "MutateResponse", ... }`; the legacy `PushOk`
 * (`{ mutations }`) shape is retained in {@link MutateResponse} below for backwards compatibility.
 */
export const MutateSuccess = Schema.Struct({
  kind: Schema.Literal("MutateResponse"),
  // The userID passed to `handleMutateRequest`, echoed back so zero-cache can enforce that only
  // tabs belonging to the same user share a client group. `null` for logged-out clients.
  userID: Schema.optional(Schema.NullOr(Schema.String)),
  mutations: Schema.Array(MutationResponse),
});
export type MutateSuccess = typeof MutateSuccess.Type;

/**
 * The response returned by `handleMutateRequest`. Since Zero 1.5 this is a superset: the current
 * `MutateSuccess` (`{ kind: "MutateResponse", ... }`) plus the legacy `PushOk`/`PushError` shapes
 * and `PushFailedBody`.
 */
export const MutateResponse = Schema.Union([MutateSuccess, PushResponseBody, PushFailedBody]);
export type MutateResponse = typeof MutateResponse.Type;

/** @deprecated Renamed to {@link MutateResponse} when the push endpoint became `/mutate` in Zero 1.5. */
export const PushResponse = MutateResponse;
/** @deprecated Renamed to {@link MutateResponse} when the push endpoint became `/mutate` in Zero 1.5. */
export type PushResponse = typeof MutateResponse.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushResponseMessage = Schema.Tuple([Schema.Literal("pushResponse"), PushResponseBody]);

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const AckMutationResponsesMessage = Schema.Tuple([Schema.Literal("ackMutationResponses"), MutationId]);

/**
 * The schema for the querystring parameters of the custom push endpoint.
 */
export const PushParams = Schema.Struct({
  schema: Schema.String,
  appID: Schema.String,
});
export type PushParams = typeof PushParams.Type;
