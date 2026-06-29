import * as Schema from "effect/Schema";
import { JsonSchema } from "./common.js";
import { TransformFailedBody } from "./error.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/zero-protocol/src/custom-queries.ts

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

const ErroredQuery = Schema.Union([AppErroredQuery, ParseErroredQuery]);

const TransformResponseBody = Schema.Array(Schema.Union([TransformedQuery, ErroredQuery]));

export const TransformRequestMessage = Schema.Tuple([Schema.Literal("transform"), TransformRequestBody]);
export type TransformRequestMessage = typeof TransformRequestMessage.Type;

// biome-ignore lint/correctness/noUnusedVariables: borrowed code
const TransformErrorMessage = Schema.Tuple([Schema.Literal("transformError"), Schema.Array(ErroredQuery)]);

const TransformFailedMessage = Schema.Tuple([Schema.Literal("transformFailed"), TransformFailedBody]);

const TransformOkMessage = Schema.Tuple([Schema.Literal("transformed"), TransformResponseBody]);

/**
 * The success response shape introduced in Zero 1.5. `handleQueryRequest` now emits
 * `{ kind: "QueryResponse", ... }`; the legacy `["transformed", ...]` / `["transformFailed", ...]`
 * tuple messages are retained in {@link QueryResponse} below for backwards compatibility.
 */
export const QuerySuccess = Schema.Struct({
  kind: Schema.Literal("QueryResponse"),
  // The userID passed to `handleQueryRequest`, echoed back. `null` for logged-out clients.
  userID: Schema.optional(Schema.NullOr(Schema.String)),
  queries: TransformResponseBody,
});
export type QuerySuccess = typeof QuerySuccess.Type;

/**
 * The response returned by `handleQueryRequest`. Since Zero 1.5 this is a superset: the current
 * `QuerySuccess` (`{ kind: "QueryResponse", ... }`) and a bare `TransformFailedBody`, plus the
 * legacy `["transformed", ...]` / `["transformFailed", ...]` tuple messages.
 */
export const QueryResponse = Schema.Union([
  QuerySuccess,
  TransformFailedBody,
  TransformOkMessage,
  TransformFailedMessage,
]);
export type QueryResponse = typeof QueryResponse.Type;

/** @deprecated Renamed to {@link QueryResponse} in Zero 1.5. */
export const TransformResponseMessage = QueryResponse;
/** @deprecated Renamed to {@link QueryResponse} in Zero 1.5. */
export type TransformResponseMessage = typeof QueryResponse.Type;
