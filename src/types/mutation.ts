import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";
import { MutationId } from "./mutation-id.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/mutation.ts

const PrimaryKey = Schema.NonEmptyArray(Schema.String);

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
