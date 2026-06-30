import * as Schema from "effect/Schema";
import { Ast } from "./ast.js";
import { JsonSchema } from "./common.js";
import { TransformFailedBody } from "./error.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/custom-queries.ts

const TransformRequestBody = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    name: Schema.String,
    args: Schema.Array(JsonSchema),
  }),
);

export const TransformedQuery = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  ast: Ast,
});

const AppErroredQuery = Schema.Struct({
  error: Schema.Literal("app"),
  id: Schema.String,
  name: Schema.String,
  // optional for backwards compatibility
  message: Schema.optional(Schema.String),
  details: Schema.optional(JsonSchema),
});

const ParseErroredQuery = Schema.Struct({
  error: Schema.Literal("parse"),
  id: Schema.String,
  name: Schema.String,
  message: Schema.String,
  details: Schema.optional(JsonSchema),
});

export const ErroredQuery = Schema.Union([AppErroredQuery, ParseErroredQuery]);

const TransformResponseBody = Schema.Array(Schema.Union([TransformedQuery, ErroredQuery]));

export const TransformRequestMessage = Schema.Tuple([Schema.Literal("transform"), TransformRequestBody]);
export type TransformRequestMessage = typeof TransformRequestMessage.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const TransformErrorMessage = Schema.Tuple([Schema.Literal("transformError"), Schema.Array(ErroredQuery)]);

const TransformFailedMessage = Schema.Tuple([Schema.Literal("transformFailed"), TransformFailedBody]);

const TransformOkMessage = Schema.Tuple([Schema.Literal("transformed"), TransformResponseBody]);

export const TransformResponseMessage = Schema.Union([TransformOkMessage, TransformFailedMessage]);
