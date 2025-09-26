import * as Schema from "effect/Schema";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/30f209f2946b4cf2cd2dee459849351498f11308/packages/zero-protocol/src/push.ts

/** TODO: Defined as unknown for now */
const JsonSchema = Schema.Unknown;

const ZeroPrimaryKey = Schema.Array(Schema.String);

const ZeroPrimaryKeyValue = Schema.Union(Schema.String, Schema.Number, Schema.Boolean);

const ZeroPrimaryKeyValueRecord = Schema.Record({ key: Schema.String, value: ZeroPrimaryKeyValue });

const ZeroValue = Schema.Union(JsonSchema, Schema.Undefined);
const ZeroRow = Schema.Record({ key: Schema.String, value: ZeroValue });

const CRUD_MUTATION_NAME = "_zero_crud";

/**
 * Inserts if entity with id does not already exist.
 */
const ZeroInsertOp = Schema.Struct({
  op: Schema.Literal("insert"),
  tableName: Schema.String,
  primaryKey: ZeroPrimaryKey,
  value: ZeroRow,
});

/**
 * Upsert semantics. Inserts if entity with id does not already exist,
 * otherwise updates existing entity with id.
 */
const ZeroUpsertOp = Schema.Struct({
  op: Schema.Literal("upsert"),
  tableName: Schema.String,
  primaryKey: ZeroPrimaryKey,
  value: ZeroRow,
});

/**
 * Updates if entity with id exists, otherwise does nothing.
 */
const ZeroUpdateOp = Schema.Struct({
  op: Schema.Literal("update"),
  tableName: Schema.String,
  primaryKey: ZeroPrimaryKey,
  // Partial value with at least the primary key fields
  value: ZeroRow,
});

/**
 * Deletes entity with id if it exists, otherwise does nothing.
 */
const ZeroDeleteOp = Schema.Struct({
  op: Schema.Literal("delete"),
  tableName: Schema.String,
  primaryKey: ZeroPrimaryKey,
  // Partial value representing the primary key
  value: ZeroPrimaryKeyValueRecord,
});

const ZeroCrudOp = Schema.Union(ZeroInsertOp, ZeroUpsertOp, ZeroUpdateOp, ZeroDeleteOp);

const ZeroCrudArg = Schema.Struct({
  ops: Schema.Array(ZeroCrudOp),
});

const ZeroCrudArgs = Schema.Tuple(ZeroCrudArg);

const ZeroCrudMutation = Schema.Struct({
  type: Schema.Literal("crud"),
  id: Schema.Number,
  clientID: Schema.String,
  name: Schema.Literal(CRUD_MUTATION_NAME),
  args: ZeroCrudArgs,
  timestamp: Schema.Number,
});

const ZeroCustomMutation = Schema.Struct({
  type: Schema.Literal("custom"),
  id: Schema.Number,
  clientID: Schema.String,
  name: Schema.String,
  args: Schema.Array(JsonSchema),
  timestamp: Schema.Number,
});

export const ZeroMutation = Schema.Union(ZeroCrudMutation, ZeroCustomMutation);
export type ZeroMutation = typeof ZeroMutation.Type;

export const ZeroPushBody = Schema.Struct({
  clientGroupID: Schema.String,
  mutations: Schema.Array(ZeroMutation),
  pushVersion: Schema.Number,
  // For legacy (CRUD) mutations, the schema is tied to the client group /
  // sync connection. For custom mutations, schema versioning is delegated
  // to the custom protocol / api-server.
  schemaVersion: Schema.optional(Schema.Number),
  timestamp: Schema.Number,
  requestID: Schema.String,
});
export type ZeroPushBody = typeof ZeroPushBody.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const ZeroPushMessage = Schema.Tuple(Schema.Literal("push"), ZeroPushBody);

const ZeroMutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});

const ZeroAppError = Schema.Struct({
  error: Schema.Literal("app"),
  // The user can return any additional data here
  details: Schema.optional(JsonSchema),
});

const ZeroError = Schema.Struct({
  error: Schema.Union(Schema.Literal("oooMutation"), Schema.Literal("alreadyProcessed")),
  details: Schema.optional(JsonSchema),
});

const ZeroMutationOk = Schema.Struct({
  // The user can return any additional data here
  data: Schema.optional(JsonSchema),
});

const ZeroMutationError = Schema.Union(ZeroAppError, ZeroError);

export const ZeroMutationResult = Schema.Union(ZeroMutationOk, ZeroMutationError);
export type ZeroMutationResult = typeof ZeroMutationResult.Type;

export const ZeroMutationResponse = Schema.Struct({
  id: ZeroMutationId,
  result: ZeroMutationResult,
});
export type ZeroMutationResponse = typeof ZeroMutationResponse.Type;

const ZeroPushOk = Schema.Struct({
  mutations: Schema.Array(ZeroMutationResponse),
});

const ZeroUnsupportedPushVersion = Schema.Struct({
  error: Schema.Literal("unsupportedPushVersion"),
  // optional for backwards compatibility
  // This field is included so the client knows which mutations
  // were not processed by the server.
  mutationIDs: Schema.optional(Schema.Array(ZeroMutationId)),
});

const ZeroUnsupportedSchemaVersion = Schema.Struct({
  error: Schema.Literal("unsupportedSchemaVersion"),
  // optional for backwards compatibility
  // This field is included so the client knows which mutations
  // were not processed by the server.
  mutationIDs: Schema.optional(Schema.Array(ZeroMutationId)),
});

const ZeroHttpError = Schema.Struct({
  error: Schema.Literal("http"),
  status: Schema.Number,
  details: Schema.String,
  mutationIDs: Schema.optional(Schema.Array(ZeroMutationId)),
});

const ZeroPusherError = Schema.Struct({
  error: Schema.Literal("zeroPusher"),
  details: Schema.String,
  mutationIDs: Schema.optional(Schema.Array(ZeroMutationId)),
});

const ZeroPushError = Schema.Union(
  ZeroUnsupportedPushVersion,
  ZeroUnsupportedSchemaVersion,
  ZeroHttpError,
  ZeroPusherError,
);

export const ZeroPushResponse = Schema.Union(ZeroPushOk, ZeroPushError);
export type ZeroPushResponse = typeof ZeroPushResponse.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const ZeroPushResponseMessage = Schema.Tuple(Schema.Literal("pushResponse"), ZeroPushResponse);

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const ZeroAckMutationResponsesMessage = Schema.Tuple(Schema.Literal("ackMutationResponses"), ZeroMutationId);

/**
 * The schema for the querystring parameters of the custom push endpoint.
 */
export const ZeroPushParams = Schema.Struct({
  schema: Schema.String,
  appID: Schema.String,
});
export type ZeroPushParams = typeof ZeroPushParams.Type;
