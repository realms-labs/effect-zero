import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/zero-protocol/src/error.ts

const MutationId = Schema.Struct({
  id: Schema.Number,
  clientID: Schema.String,
});

const PushFailedBase = Schema.Struct({
  kind: Schema.Literal("PushFailed"),
  details: Schema.optional(JsonSchema),
  /**
   * The mutationIDs of the mutations that failed to process.
   * This can be a subset of the mutationIDs in the request.
   */
  mutationIDs: Schema.Array(MutationId),
  message: Schema.String,
});

export const PushFailedBody = Schema.Union([
  Schema.Struct({
    ...PushFailedBase.fields,
    origin: Schema.Literal("server"),
    reason: Schema.Literals(["database", "parse", "oooMutation", "unsupportedPushVersion", "internal"]),
  }),
  Schema.Struct({
    ...PushFailedBase.fields,
    origin: Schema.Literal("zeroCache"),
    reason: Schema.Literal("http"),
    status: Schema.Number,
    bodyPreview: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    ...PushFailedBase.fields,
    origin: Schema.Literal("zeroCache"),
    reason: Schema.Literals(["timeout", "parse", "internal"]),
  }),
]);

const TransformFailedBase = Schema.Struct({
  kind: Schema.Literal("TransformFailed"),
  details: Schema.optional(JsonSchema),
  /**
   * The queryIDs of the queries that failed to transform.
   */
  queryIDs: Schema.Array(Schema.String),
  message: Schema.String,
});

export const TransformFailedBody = Schema.Union([
  Schema.Struct({
    ...TransformFailedBase.fields,
    origin: Schema.Literal("server"),
    reason: Schema.Literals(["database", "parse", "internal"]),
  }),
  Schema.Struct({
    ...TransformFailedBase.fields,
    origin: Schema.Literal("zeroCache"),
    reason: Schema.Literal("http"),
    status: Schema.Number,
    bodyPreview: Schema.optional(Schema.String),
  }),
  Schema.Struct({
    ...TransformFailedBase.fields,
    origin: Schema.Literal("zeroCache"),
    reason: Schema.Literals(["timeout", "parse", "internal"]),
  }),
]);
