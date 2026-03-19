import type { HumanReadable, QueryResultDetails as ZeroQueryResultDetails } from "@rocicorp/zero";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Match from "effect/Match";

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

export const make = <A>([value, details]: ZeroQueryResult<A>): QueryResult<A> => {
  return Match.value(details).pipe(
    Match.when({ type: "complete" }, () => ({ _tag: "Complete", value }) as const),
    Match.when({ type: "unknown" }, () => ({ _tag: "Partial", value }) as const),
    Match.when({ type: "error" }, ({ type: _type, ...details }) => ({ _tag: "Error", details }) as const),
    Match.exhaustive,
  );
};
