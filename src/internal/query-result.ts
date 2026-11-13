import type { HumanReadable, QueryResultDetails as ZeroQueryResultDetails } from "@rocicorp/zero";
import type { asQueryInternals } from "@rocicorp/zero/bindings";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Match from "effect/Match";
import type * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";

// Our internal representation of Zero's QueryResult type.

export type QueryResult<A> =
  | QueryResult.Partial<A>
  | QueryResult.Cached<A>
  | QueryResult.Complete<A>
  | QueryResult.Error;
export namespace QueryResult {
  /**
   * What a top-level aggregate query (`count()`, `sum()`, `avg()`, `min()`, `max()`) computes, from Zero's query
   * format. Zero doesn't compute an aggregate from the local store: until the server answers, its `Partial` value is
   * the aggregate over no rows, `0` for `count()` and `null` for the others.
   */
  export type Aggregate = NonNullable<ReturnType<typeof asQueryInternals>["format"]["aggregate"]>;
  /** Confirmed by the server on this connection. */
  export interface Complete<A> {
    readonly _tag: "Complete";
    readonly value: HumanReadable<A>;
    /** Set for a top-level aggregate query. */
    readonly aggregate: Option.Option<Aggregate>;
  }
  /**
   * From the local store, which the server confirmed in an earlier session (Zero's `cached`). It may be out of date,
   * but it isn't missing rows that just haven't synced yet: an empty result is really empty. It turns back into
   * `Partial` if Zero evicts the confirmation before the server confirms the query again.
   */
  export interface Cached<A> {
    readonly _tag: "Cached";
    readonly value: HumanReadable<A>;
    /** Set for a top-level aggregate query. */
    readonly aggregate: Option.Option<Aggregate>;
  }
  /** From the local store, never confirmed by the server (Zero's `unknown`), so it may be missing rows. */
  export interface Partial<A> {
    readonly _tag: "Partial";
    readonly value: HumanReadable<A>;
    /** Set for a top-level aggregate query. */
    readonly aggregate: Option.Option<Aggregate>;
  }
  export interface Error {
    readonly _tag: "Error";
    readonly details: Omit<Extract<ZeroQueryResultDetails, { readonly type: "error" }>, "type">;
  }
}

/** `aggregate` is from the query's format, set for a top-level aggregate query. */
export const make = <A>(
  [value, details]: ZeroQueryResult<A>,
  { aggregate }: { readonly aggregate: Option.Option<QueryResult.Aggregate> },
): QueryResult<A> => {
  return Match.value(details).pipe(
    Match.when({ type: "complete" }, () => ({ _tag: "Complete", value, aggregate }) as const),
    Match.when({ type: "cached" }, () => ({ _tag: "Cached", value, aggregate }) as const),
    Match.when({ type: "unknown" }, () => ({ _tag: "Partial", value, aggregate }) as const),
    Match.when({ type: "error" }, ({ type: _type, ...details }) => ({ _tag: "Error", details }) as const),
    Match.exhaustive,
  );
};

/** How many rows a result holds: a list's length, or 0 or 1 for a singular query's optional row. */
export const rowCount = (value: unknown) =>
  Array.isArray(value) ? value.length : Predicate.isUndefined(value) ? 0 : 1;
