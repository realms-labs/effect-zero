import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/9fe2b3cb770bad269ecaf12296c64a0739c23e4e/packages/zero-protocol/src/custom-queries.ts#L14

/** TODO: Defined as unknown for now */
const Ast = Schema.Unknown;

const TransformRequestBody = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    name: Schema.String,
    args: Schema.Array(JsonSchema),
  }),
);

const TransformedQuery = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  ast: Ast,
});

const AppQueryError = Schema.Struct({
  error: Schema.Literal("app"),
  id: Schema.String,
  name: Schema.String,
  details: JsonSchema,
});

const ErrorSchema = Schema.Struct({
  error: Schema.Literal("zero"),
  id: Schema.String,
  name: Schema.String,
  details: JsonSchema,
});

const HttpQueryError = Schema.Struct({
  error: Schema.Literal("http"),
  id: Schema.String,
  name: Schema.String,
  status: Schema.Number,
  details: JsonSchema,
});

const ErroredQuery = Schema.Union(AppQueryError, HttpQueryError, ErrorSchema);

const TransformResponseBody = Schema.Array(Schema.Union(TransformedQuery, ErroredQuery));

export const TransformRequestMessage = Schema.Tuple(Schema.Literal("transform"), TransformRequestBody);
export type TransformRequestMessage = typeof TransformRequestMessage.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const TransformErrorMessage = Schema.Tuple(Schema.Literal("transformError"), Schema.Array(ErroredQuery));

export const TransformResponseMessage = Schema.Tuple(Schema.Literal("transformed"), TransformResponseBody);
