import * as Schema from "effect/Schema";
import { ErroredQuery, TransformedQuery, TransformResponseMessage } from "./custom-queries.js";
import { TransformFailedBody } from "./error.js";

// The code below was converted to Effect Schema from:
// https://github.com/rocicorp/mono/blob/c22b300d15435712a4c5e61313645881942f1d3a/packages/zero-protocol/src/query-server.ts

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
