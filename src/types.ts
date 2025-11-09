import * as Schema from "effect/Schema";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/30f209f2946b4cf2cd2dee459849351498f11308/packages/zero-protocol/src/push.ts

/** TODO: Defined as unknown for now */
const JsonSchema = Schema.Unknown;

const PrimaryKey = Schema.Array(Schema.String);

const PrimaryKeyValue = Schema.Union(Schema.String, Schema.Number, Schema.Boolean);

const PrimaryKeyValueRecord = Schema.Record({ key: Schema.String, value: PrimaryKeyValue });

const Value = Schema.Union(JsonSchema, Schema.Undefined);
const Row = Schema.Record({ key: Schema.String, value: Value });

const CRUD_MUTATION_NAME = "_zero_crud";

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

const CrudOp = Schema.Union(InsertOp, UpsertOp, UpdateOp, DeleteOp);

const CrudArg = Schema.Struct({
  ops: Schema.Array(CrudOp),
});

const CrudArgs = Schema.Tuple(CrudArg);

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

export const Mutation = Schema.Union(CrudMutation, CustomMutation);
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
});
export type PushBody = typeof PushBody.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushMessage = Schema.Tuple(Schema.Literal("push"), PushBody);

const MutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});

export const AppError = Schema.Struct({
  error: Schema.Literal("app"),
  // The user can return any additional data here
  details: Schema.optional(JsonSchema),
});

export type AppError = typeof AppError.Type;

export const ZeroError = Schema.Struct({
  error: Schema.Union(Schema.Literal("oooMutation"), Schema.Literal("alreadyProcessed")),
  details: Schema.optional(JsonSchema),
});

export type ZeroError = typeof ZeroError.Type;

const MutationOk = Schema.Struct({
  // The user can return any additional data here
  data: Schema.optional(JsonSchema),
});

const MutationError = Schema.Union(AppError, ZeroError);

export const MutationResult = Schema.Union(
  // We flip the original order here as otherwise values of type MutationError would get parsed as MutationOk with empty `data`
  MutationError,
  MutationOk,
);
export type MutationResult = typeof MutationResult.Type;

export const MutationResponse = Schema.Struct({
  id: MutationId,
  result: MutationResult,
});
export type MutationResponse = typeof MutationResponse.Type;

const PushOk = Schema.Struct({
  mutations: Schema.Array(MutationResponse),
});

const UnsupportedPushVersion = Schema.Struct({
  error: Schema.Literal("unsupportedPushVersion"),
  // optional for backwards compatibility
  // This field is included so the client knows which mutations
  // were not processed by the server.
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

const UnsupportedSchemaVersion = Schema.Struct({
  error: Schema.Literal("unsupportedSchemaVersion"),
  // optional for backwards compatibility
  // This field is included so the client knows which mutations
  // were not processed by the server.
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

const HttpError = Schema.Struct({
  error: Schema.Literal("http"),
  status: Schema.Number,
  details: Schema.String,
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

const ZeroPusherError = Schema.Struct({
  error: Schema.Literal("zeroPusher"),
  details: Schema.String,
  mutationIDs: Schema.optional(Schema.Array(MutationId)),
});

const PushError = Schema.Union(UnsupportedPushVersion, UnsupportedSchemaVersion, HttpError, ZeroPusherError);

export const PushResponse = Schema.Union(PushOk, PushError);
export type PushResponse = typeof PushResponse.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const PushResponseMessage = Schema.Tuple(Schema.Literal("pushResponse"), PushResponse);

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const AckMutationResponsesMessage = Schema.Tuple(Schema.Literal("ackMutationResponses"), MutationId);

/**
 * The schema for the querystring parameters of the custom push endpoint.
 */
export const PushParams = Schema.Struct({
  schema: Schema.String,
  appID: Schema.String,
});
export type PushParams = typeof PushParams.Type;
