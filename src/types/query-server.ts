import * as Schema from "effect/Schema";
import { ErroredQuery, TransformedQuery, TransformResponseMessage } from "./custom-queries.js";
import { TransformFailedBody } from "./error.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/74273167af5d15ed07045e04165cb04d3983d44f/packages/zero-protocol/src/query-server.ts

export const QueryResult = Schema.Union([TransformedQuery, ErroredQuery]);
export type QueryResult = typeof QueryResult.Type;

export const QueryResponseBody = Schema.Array(QueryResult);
export type QueryResponseBody = typeof QueryResponseBody.Type;

export const QuerySuccess = Schema.Struct({
  kind: Schema.Literal("QueryResponse"),
  userID: Schema.optional(Schema.NullOr(Schema.String)),
  queries: QueryResponseBody,
});
export type QuerySuccess = typeof QuerySuccess.Type;

export const QueryResponse = Schema.Union([
  QuerySuccess,
  TransformFailedBody,
  // for backwards compatibility
  TransformResponseMessage,
]);
export type QueryResponse = typeof QueryResponse.Type;
