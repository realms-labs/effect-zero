import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/9fe2b3cb770bad269ecaf12296c64a0739c23e4e/packages/zero-protocol/src/custom-queries.ts#L14

/** TODO: Defined as unknown for now */
const ZeroAst = Schema.Unknown;

const ZeroTransformRequestBody = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    name: Schema.String,
    args: Schema.Array(JsonSchema),
  }),
);

const ZeroTransformedQuery = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  ast: ZeroAst,
});

const ZeroAppQueryError = Schema.Struct({
  error: Schema.Literal("app"),
  id: Schema.String,
  name: Schema.String,
  details: JsonSchema,
});

const ZeroErrorSchema = Schema.Struct({
  error: Schema.Literal("zero"),
  id: Schema.String,
  name: Schema.String,
  details: JsonSchema,
});

const ZeroHttpQueryError = Schema.Struct({
  error: Schema.Literal("http"),
  id: Schema.String,
  name: Schema.String,
  status: Schema.Number,
  details: JsonSchema,
});

const ZeroErroredQuery = Schema.Union(ZeroAppQueryError, ZeroHttpQueryError, ZeroErrorSchema);

const ZeroTransformResponseBody = Schema.Array(Schema.Union(ZeroTransformedQuery, ZeroErroredQuery));

export const ZeroTransformRequestMessage = Schema.Tuple(Schema.Literal("transform"), ZeroTransformRequestBody);
export type ZeroTransformRequestMessage = typeof ZeroTransformRequestMessage.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const ZeroTransformErrorMessage = Schema.Tuple(Schema.Literal("transformError"), Schema.Array(ZeroErroredQuery));

export const ZeroTransformResponseMessage = Schema.Tuple(Schema.Literal("transformed"), ZeroTransformResponseBody);
