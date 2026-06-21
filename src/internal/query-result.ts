import type {
  ErroredQuery,
  HumanReadable,
  ResultType,
  QueryResultDetails as ZeroQueryResultDetails,
} from "@rocicorp/zero";

// Our internal representation of Zero's QueryResult type.

export type QueryResult<A> = QueryResult.Partial<A> | QueryResult.Complete<A> | QueryResult.Error;
export namespace QueryResult {
  export interface Complete<A> {
    readonly _tag: "Complete";
    readonly value: HumanReadable<A>;
  }
  export interface Partial<A> {
    readonly _tag: "Partial";
    readonly value: HumanReadable<A>;
  }
  export interface Error {
    readonly _tag: "Error";
    readonly details: Omit<Extract<ZeroQueryResultDetails, { readonly type: "error" }>, "type">;
  }
}

// Build the error `details` from a Zero error payload (mirrors zero-react's `makeError`, minus `type`).
const errorDetails = (retry: () => void, error: ErroredQuery): QueryResult.Error["details"] => ({
  retry,
  refetch: retry,
  error: {
    type: error.error,
    message: error.message ?? "An unknown error occurred",
    ...(error.details ? { details: error.details } : {}),
  },
});

// Build the internal `QueryResult` directly from a Zero view event, without the intermediate zero-react
// snapshot tuple. Mirrors the composed behavior of the previous `getSnapshot` + tuple conversion.
export const fromView = <A>(
  singular: boolean,
  value: HumanReadable<A>,
  resultType: ResultType,
  retry: () => void,
  error?: ErroredQuery,
): QueryResult<A> => {
  switch (resultType) {
    case "complete":
      return { _tag: "Complete", value };
    case "unknown":
      return { _tag: "Partial", value };
    default: {
      if (error) return { _tag: "Error", details: errorDetails(retry, error) };
      // No error payload: an empty result keeps empty details (as the predefined empty error snapshot
      // did); a non-empty one falls back to a generic "app" error (as the non-empty error snapshot did).
      const isEmpty = singular ? value === undefined : (value as ReadonlyArray<unknown>).length === 0;
      if (isEmpty) return { _tag: "Error", details: {} as QueryResult.Error["details"] };
      return {
        _tag: "Error",
        details: errorDetails(retry, {
          error: "app",
          id: "unknown",
          name: "unknown",
          message: "An unknown error occurred",
        }),
      };
    }
  }
};

// The initial (pending) result before any view event has arrived.
export const initial = <A>(singular: boolean): QueryResult<A> => ({
  _tag: "Partial",
  value: (singular ? undefined : []) as unknown as HumanReadable<A>,
});
