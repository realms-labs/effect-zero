import type { HumanReadable, QueryResultDetails as ZeroQueryResultDetails } from "@rocicorp/zero";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";

// Our internal representation of Zero's QueryResult type.

export type QueryResult<A> =
  | QueryResult.Partial<A>
  | QueryResult.Cached<A>
  | QueryResult.Complete<A>
  | QueryResult.Error;
export namespace QueryResult {
  /** Confirmed by the server on this connection. */
  export interface Complete<A> {
    readonly _tag: "Complete";
    readonly value: HumanReadable<A>;
  }
  /**
   * From the local store, which the server confirmed in an earlier session (Zero's `cached`). It may be out of date,
   * but it isn't missing rows that just haven't synced yet: an empty result is really empty. It turns back into
   * `Partial` if Zero evicts the confirmation before the server confirms the query again.
   */
  export interface Cached<A> {
    readonly _tag: "Cached";
    readonly value: HumanReadable<A>;
  }
  /** From the local store, never confirmed by the server (Zero's `unknown`), so it may be missing rows. */
  export interface Partial<A> {
    readonly _tag: "Partial";
    readonly value: HumanReadable<A>;
    /**
     * Whether `value` only stands in for a top-level aggregate (`count()`, `sum()`, `avg()`, `min()`, `max()`), which
     * Zero doesn't compute from the local store: until the server answers, `count()` is `0` and the others are `null`.
     */
    readonly placeholder: boolean;
  }
  export interface Error {
    readonly _tag: "Error";
    readonly details: Omit<Extract<ZeroQueryResultDetails, { readonly type: "error" }>, "type">;
  }
}

/** `aggregate` is whether the query is a top-level aggregate, whose `Partial` results are placeholders. */
export const make = <A>(
  [value, details]: ZeroQueryResult<A>,
  { aggregate }: { readonly aggregate: boolean },
): QueryResult<A> => {
  return Match.value(details).pipe(
    Match.when({ type: "complete" }, () => ({ _tag: "Complete", value }) as const),
    Match.when({ type: "cached" }, () => ({ _tag: "Cached", value }) as const),
    Match.when({ type: "unknown" }, () => ({ _tag: "Partial", value, placeholder: aggregate }) as const),
    Match.when({ type: "error" }, ({ type: _type, ...details }) => ({ _tag: "Error", details }) as const),
    Match.exhaustive,
  );
};

/** How many rows a result holds: a list's length, or 0 or 1 for a singular query's optional row. */
export const rowCount = (value: unknown) =>
  Array.isArray(value) ? value.length : Predicate.isUndefined(value) ? 0 : 1;
