import { Result as AtomResult } from "@effect-atom/atom";
import type { HumanReadable } from "@rocicorp/zero/react";
import * as Data from "effect/Data";
import * as Fn from "effect/Function";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Struct from "effect/Struct";
import type { QueryResult } from "./internal/query-result.js";

/*
### Introduction

When using effect-atom with effect-zero, you will often have atoms which contain a type in the form of

`@effect-atom/atom`.Result<`@rocicorp/zero/react`.QueryResult<A>, E>

However, such a type is not easy to work with.
The purpose of `effect-zero`.Result is to provide a more convenient type for this use case.

### Background

To start, let's analyze the possible states of the original type:
- The outer `@effect-atom/atom`.Result may be `Initial<_, _>`, `Failure<_, E>`, or `Success<QueryResult, _>`.
- The inner `@rocicorp/zero/react`.QueryResult may be `unknown<A>`, `complete<A>`, or `error`.

This means that there are 5 total possible states:
- Initial, representing the state before the query has been executed on either the client or the server.
- Success(unknown(A)), representing the state when the query has only been executed on the client.
- Success(complete(A)), representing the state when the query has been executed on the server.
- Success(error), representing the state when an error occurs within the query.
- Failure(E), representing the state when an error occurs outside the query (e.g. within your Atom code).

### Solution

On the other hand, `effect-zero`.Result has the following states:
- Initial -> Representing the same state as `Initial` in the original type.
- Success(Partial(A)) -> Representing the same state as `Success(unknown)` in the original type.
- Success(Complete(A)) -> Representing the same state as `Success(complete)` in the original type.
- Failure(QueryError) -> Representing the same state as `Success(error)` in the original type.
- Failure(E) -> Representing the same state as `Failure(E)` in the original type.

This has a few key benefits:
- It allows you to deal with errors in a more streamlined way, as now all error states are represented as a `Failure(E | QueryError)`,
  thus you can use the builtin facilities of `effect-atom`.Result to work with the possible error states.
- We provide a method `acceptPartialMode` which allows for simplifying the type further by specifying the situations in which you want to accept
  partial results, resulting a further simplified type of `Initial | Success(A) | Failure(E | QueryError)`.
*/

export type Result<A, E = never> = AtomResult.Result<QueryResult.Partial<A> | QueryResult.Complete<A>, E | QueryError>;
export namespace Result {
  export type Initial<A, E = never> = AtomResult.Initial<
    QueryResult.Partial<A> | QueryResult.Complete<A>,
    E | QueryError
  >;
  export type Failure<A, E = never> = AtomResult.Failure<
    QueryResult.Partial<A> | QueryResult.Complete<A>,
    E | QueryError
  >;
  export type Success<A, E = never> = AtomResult.Success<
    QueryResult.Partial<A> | QueryResult.Complete<A>,
    E | QueryError
  >;

  // More granular Partial and Complete types
  export type Partial<A, E = never> = AtomResult.Success<QueryResult.Partial<A>, E | QueryError>;
  export type Complete<A, E = never> = AtomResult.Success<QueryResult.Complete<A>, E | QueryError>;
}

export class QueryError extends Data.TaggedError("QueryError")<{
  readonly details: QueryResult.Error["details"];
}> {}

/*
TODO: remove this comment 

Usage is something like:

export const queryAtom = Atom.family(
  <T extends keyof Schema["tables"] & string, R>(query: ZfxQuery.Query<Schema, T, R>) => {
    return Atom.subscribable(
      Effect.fn(function* (get) {
        const zero = yield* get.result(zeroAtom);
        return ZfxQuery.subscribable(zero, query);
      }),
    ).pipe(
      Atom.map(ZfxResult.make),
      Atom.map(ZfxResult.acceptPartialMode("waitForServer"))
    );
  },
);
*/

export const make = <A, E>(value: AtomResult.Result<QueryResult<A>, E>): Result<A, E> => {
  // TODO: implement this.
};

// based on: https://github.com/tim-smart/effect-atom/blob/04c15cacda42dd230782f52c0e978400793a502c/packages/atom/src/Result.ts#L424
export const match: {
  <A, E, W, X, Y, Z>(options: {
    readonly onInitial: (_: Result.Initial<A, E>) => W;
    readonly onFailure: (_: Result.Failure<A, E>) => X;
    readonly onPartial: (_: QueryResult.Partial<A>) => Y;
    readonly onComplete: (_: QueryResult.Complete<A>) => Z;
  }): (self: Result<A, E>) => W | X | Y | Z;
  <A, E, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Result.Initial<A, E>) => W;
      readonly onFailure: (_: Result.Failure<A, E>) => X;
      readonly onPartial: (_: QueryResult.Partial<A>) => Y;
      readonly onComplete: (_: QueryResult.Complete<A>) => Z;
    },
  ): W | X | Y | Z;
} = Fn.dual(
  2,
  <A, E, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Result.Initial<A, E>) => W;
      readonly onFailure: (_: Result.Failure<A, E>) => X;
      readonly onPartial: (_: QueryResult.Partial<A>) => Y;
      readonly onComplete: (_: QueryResult.Complete<A>) => Z;
    },
  ) => {
    switch (self._tag) {
      case "Initial":
        return options.onInitial(self);
      case "Failure":
        return options.onFailure(self);
      case "Success": {
        switch (self.value._tag) {
          case "Partial":
            return options.onPartial(self.value);
          case "Complete":
            return options.onComplete(self.value);
        }
      }
    }
  },
);

export const mapSuccess: {
  <A, B>(f: (value: A) => B): <E = never>(self: AtomResult.Success<A, E>) => AtomResult.Success<B, E>;
  <A, B, E = never>(self: AtomResult.Success<A, E>, f: (value: A) => B): AtomResult.Success<B, E>;
} = Fn.dual(2, <A, B, E = never>(self: AtomResult.Success<A, E>, f: (value: A) => B) => AtomResult.map(self, f));

export const acceptPartialWith = <A, E = never>(self: Result<A, E>, predicate: (value: HumanReadable<A>) => boolean) =>
  AtomResult.match(self, {
    onSuccess: (success) =>
      Predicate.isTagged(success.value, "Partial") && !predicate(success.value.value)
        ? AtomResult.initial<HumanReadable<A>, E>(success.waiting)
        : mapSuccess(success, Struct.get("value")),
    onFailure: (failure) =>
      AtomResult.failure<E | QueryError, HumanReadable<A>>(failure.cause, {
        waiting: failure.waiting,
        previousSuccess: failure.previousSuccess.pipe(
          Option.flatMap((success) =>
            Predicate.isTagged(success.value, "Partial") && !predicate(success.value.value)
              ? Option.none()
              : Option.some(mapSuccess(success, Struct.get("value"))),
          ),
        ),
      }),
    onInitial: (initial) => AtomResult.initial<HumanReadable<A>, E>(initial.waiting),
  });

const acceptPartialMode = <A, E>(self: Result<A, E>, mode: any) => {
  // TODO: implement this.
};
