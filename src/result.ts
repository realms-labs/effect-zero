import type { HumanReadable } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Fn from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as Struct from "effect/Struct";
import { type QueryResult, rowCount } from "./internal/query-result.js";

/*
### Introduction

When using `effect/reactivity` (atoms) with effect-zero, you will often have atoms which contain a type in the form of

`AsyncResult<QueryResult<A>, E>`

However, such a type is not easy to work with.
The purpose of `effect-zero`.Result is to provide a more convenient type for this use case.

### Background

To start, let's analyze the possible states of the original type:
- The outer `AsyncResult` may be `Initial<_, _>`, `Failure<_, E>`, or `Success<QueryResult, _>`.
- The inner `QueryResult` may be `unknown<A>`, `cached<A>`, `complete<A>`, or `error`.

This means that there are 6 total possible states:
- Initial, representing the state before the query has been executed on either the client or the server.
- Success(unknown(A)), representing the state when the query has only been executed on the client.
- Success(cached(A)), representing the state when the query has only been executed on the client, against local data
  that the server confirmed in an earlier session.
- Success(complete(A)), representing the state when the query has been executed on the server.
- Success(error), representing the state when an error occurs within the query.
- Failure(E), representing the state when an error occurs outside the query (e.g. within your Atom code).

### Solution

On the other hand, `effect-zero`.Result has the following states:
- Initial -> Representing the same state as `Initial` in the original type.
- Success(Partial(A)) -> Representing the same state as `Success(unknown)` in the original type.
- Success(Cached(A)) -> Representing the same state as `Success(cached)` in the original type.
- Success(Complete(A)) -> Representing the same state as `Success(complete)` in the original type.
- Failure(QueryError) -> Representing the same state as `Success(error)` in the original type.
- Failure(E) -> Representing the same state as `Failure(E)` in the original type.

This has a few key benefits:
- It allows you to deal with errors in a more streamlined way, as now all error states are represented as a `Failure(E | QueryError)`,
  thus you can use the builtin facilities of `AsyncResult` to work with the possible error states.
- We provide a method `acceptPartialMode` which allows for simplifying the type further by specifying the situations in which you want to accept
  results the server hasn't confirmed on this connection, resulting a further simplified type of `Initial | Success(A) | Failure(E | QueryError)`.
*/

/** A result the query produced: anything but an error. */
type QueryValue<A> = QueryResult.Partial<A> | QueryResult.Cached<A> | QueryResult.Complete<A>;

export type Result<A, E = never> = AsyncResult.AsyncResult<QueryValue<A>, E | QueryError>;
export namespace Result {
  export type Initial<A, E = never> = AsyncResult.Initial<QueryValue<A>, E | QueryError>;
  export type Failure<A, E = never> = AsyncResult.Failure<QueryValue<A>, E | QueryError>;
  export type Success<A, E = never> = AsyncResult.Success<QueryValue<A>, E | QueryError>;

  // More granular Partial, Cached and Complete types
  export type Partial<A, E = never> = AsyncResult.Success<QueryResult.Partial<A>, E | QueryError>;
  export type Cached<A, E = never> = AsyncResult.Success<QueryResult.Cached<A>, E | QueryError>;
  export type Complete<A, E = never> = AsyncResult.Success<QueryResult.Complete<A>, E | QueryError>;
}

export class QueryError extends Data.TaggedError("QueryError")<{
  readonly details: QueryResult.Error["details"];
}> {}

export const make = <A, E>(value: AsyncResult.AsyncResult<QueryResult<A>, E>): Result<A, E> =>
  Match.valueTags(value, {
    Initial: () => AsyncResult.initial(),
    Success: ({ value }) =>
      Match.valueTags(value, {
        Partial: (value) => AsyncResult.success(value),
        Cached: (value) => AsyncResult.success(value),
        Complete: (value) => AsyncResult.success(value),
        Error: (e): Result<A, E> => AsyncResult.failure(Cause.fail(new QueryError(e))),
      }),
    Failure: (e): Result<A, E> => AsyncResult.failure(e.cause),
  });

// based on: https://github.com/tim-smart/effect-atom/blob/04c15cacda42dd230782f52c0e978400793a502c/packages/atom/src/Result.ts#L424
export const match: {
  <A, E, V, W, X, Y, Z>(options: {
    readonly onInitial: (_: Result.Initial<A, E>) => W;
    readonly onFailure: (_: Result.Failure<A, E>) => X;
    readonly onPartial: (_: QueryResult.Partial<A>) => Y;
    readonly onCached: (_: QueryResult.Cached<A>) => V;
    readonly onComplete: (_: QueryResult.Complete<A>) => Z;
  }): (self: Result<A, E>) => V | W | X | Y | Z;
  <A, E, V, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Result.Initial<A, E>) => W;
      readonly onFailure: (_: Result.Failure<A, E>) => X;
      readonly onPartial: (_: QueryResult.Partial<A>) => Y;
      readonly onCached: (_: QueryResult.Cached<A>) => V;
      readonly onComplete: (_: QueryResult.Complete<A>) => Z;
    },
  ): V | W | X | Y | Z;
} = Fn.dual(
  2,
  <A, E, V, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Result.Initial<A, E>) => W;
      readonly onFailure: (_: Result.Failure<A, E>) => X;
      readonly onPartial: (_: QueryResult.Partial<A>) => Y;
      readonly onCached: (_: QueryResult.Cached<A>) => V;
      readonly onComplete: (_: QueryResult.Complete<A>) => Z;
    },
  ) =>
    Match.valueTags(self, {
      Initial: (initial) => options.onInitial(initial),
      Failure: (failure) => options.onFailure(failure),
      Success: (success) =>
        Match.valueTags(success.value, {
          Partial: (partial) => options.onPartial(partial),
          Cached: (cached) => options.onCached(cached),
          Complete: (complete) => options.onComplete(complete),
        }),
    }),
);

export const mapSuccess: {
  <A, B>(f: (value: A) => B): <E = never>(self: AsyncResult.Success<A, E>) => AsyncResult.Success<B, E>;
  <A, B, E = never>(self: AsyncResult.Success<A, E>, f: (value: A) => B): AsyncResult.Success<B, E>;
} = Fn.dual(2, <A, B, E = never>(self: AsyncResult.Success<A, E>, f: (value: A) => B) => AsyncResult.map(self, f));

/** A result the server hasn't confirmed on this connection, which `acceptPartialWith` asks about. */
export type Unconfirmed<A> = QueryResult.Partial<A> | QueryResult.Cached<A>;

type Accepted<A, E> =
  | AsyncResult.Initial<HumanReadable<A>, E>
  | AsyncResult.Success<HumanReadable<A>, QueryError | E>
  | AsyncResult.Failure<HumanReadable<A>, QueryError | E>;

/**
 * Unwraps a result to its value, accepting a `Partial` or `Cached` one only if `predicate` says so. A complete result
 * is always accepted; until one arrives, a result `predicate` rejects shows as `Initial`. The same goes for the
 * previous success of a failure.
 */
export const acceptPartialWith: {
  <A, E = never>(self: Result<A, E>, predicate: (result: Unconfirmed<A>) => boolean): Accepted<A, E>;
  <A>(predicate: (result: Unconfirmed<A>) => boolean): <E = never>(self: Result<A, E>) => Accepted<A, E>;
} = Fn.dual(2, <A, E = never>(self: Result<A, E>, predicate: (result: Unconfirmed<A>) => boolean): Accepted<A, E> => {
  const isAccepted = (value: QueryValue<A>) => Predicate.isTagged(value, "Complete") || predicate(value);
  return AsyncResult.match(self, {
    onSuccess: (success) =>
      isAccepted(success.value) ? mapSuccess(success, Struct.get("value")) : AsyncResult.initial(success.waiting),
    onFailure: (failure) =>
      AsyncResult.failure(failure.cause, {
        waiting: failure.waiting,
        previousSuccess: failure.previousSuccess.pipe(
          Option.filter((success) => isAccepted(success.value)),
          Option.map((success) => mapSuccess(success, Struct.get("value"))),
        ),
      }),
    onInitial: (initial) => AsyncResult.initial(initial.waiting),
  });
});

export type AcceptPartialMode = "acceptLocal" | "acceptConfirmed" | "waitForServer";

export const acceptPartialMode: {
  <A, E>(
    self: Result<A, E>,
    /**
     * - `acceptLocal` -> Will resolve to the local result if it is available
     *   - For singular queries, it will resolve if the local row exists, or the server confirmed there is none.
     *   - For list queries, it will resolve to the local rows regardless of the length of the list, so a list may be
     *     empty just because nothing has synced yet.
     *   - It will update the returned value as additional rows come in, and then optionally settle on a final value.
     *
     * - `acceptConfirmed` -> Will resolve to the local result once it has rows, or once the server has confirmed it
     *   (on this connection or an earlier one)
     *   - Use it where an empty result shows something, such as an empty state.
     *   - If Zero evicts a `Cached` confirmation before the server confirms the query again, an empty result goes
     *     back to `Initial`.
     *
     * - `waitForServer` -> Will wait for the complete result (from the server) before resolving
     *   - When you want to wait for the server before doing something.
     *   - Only used for queries in which you need an fully accurate result before doing something.
     *
     * Zero doesn't compute a top-level aggregate (`count()`, `sum()`, `avg()`, `min()`, `max()`) locally, so
     * `acceptLocal` and `acceptConfirmed` wait for it to be cached or complete.
     **/
    mode: AcceptPartialMode,
  ): Accepted<A, E>;
  (
    /**
     * - `acceptLocal` -> Will resolve to the local result if it is available
     *   - For singular queries, it will resolve if the local row exists, or the server confirmed there is none.
     *   - For list queries, it will resolve to the local rows regardless of the length of the list, so a list may be
     *     empty just because nothing has synced yet.
     *   - It will update the returned value as additional rows come in, and then optionally settle on a final value.
     *
     * - `acceptConfirmed` -> Will resolve to the local result once it has rows, or once the server has confirmed it
     *   (on this connection or an earlier one)
     *   - Use it where an empty result shows something, such as an empty state.
     *   - If Zero evicts a `Cached` confirmation before the server confirms the query again, an empty result goes
     *     back to `Initial`.
     *
     * - `waitForServer` -> Will wait for the complete result (from the server) before resolving
     *   - When you want to wait for the server before doing something.
     *   - Only used for queries in which you need an fully accurate result before doing something.
     *
     * Zero doesn't compute a top-level aggregate (`count()`, `sum()`, `avg()`, `min()`, `max()`) locally, so
     * `acceptLocal` and `acceptConfirmed` wait for it to be cached or complete.
     **/
    mode: AcceptPartialMode,
  ): <A, E>(self: Result<A, E>) => Accepted<A, E>;
} = Fn.dual(2, <A, E>(self: Result<A, E>, mode: AcceptPartialMode) =>
  acceptPartialWith(self, (result) =>
    Match.value(mode).pipe(
      Match.when("acceptLocal", () =>
        Match.valueTags(result, {
          Partial: ({ value, aggregate }) => aggregate === undefined && Predicate.isNotUndefined(value),
          Cached: () => true,
        }),
      ),
      Match.when("acceptConfirmed", () =>
        Match.valueTags(result, {
          Partial: ({ value, aggregate }) => aggregate === undefined && rowCount(value) > 0,
          Cached: () => true,
        }),
      ),
      Match.when("waitForServer", () => false),
      Match.exhaustive,
    ),
  ),
);
