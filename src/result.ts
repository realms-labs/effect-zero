import { Result as AtomResult } from "@effect-atom/atom";
import type { HumanReadable } from "@rocicorp/zero/react";
import * as Fn from "effect/Function";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Struct from "effect/Struct";

export interface PartialQueryResult<A> {
  readonly _tag: "Partial";
  readonly value: HumanReadable<A>;
}
export interface CompleteQueryResult<A> {
  readonly _tag: "Complete";
  readonly value: HumanReadable<A>;
}
export type QueryResult<A> = PartialQueryResult<A> | CompleteQueryResult<A>;

export type Result<A, E = never> = AtomResult.Result<QueryResult<A>, E>;

// Unlike AtomResult, effect-zero's Result has four states: Initial, Failure, Partial, Complete.
export type Initial<A, E = never> = AtomResult.Initial<QueryResult<A>, E>;
export type Failure<A, E = never> = AtomResult.Failure<QueryResult<A>, E>;
export type Partial<A, E = never> = AtomResult.Success<PartialQueryResult<A>, E>;
export type Complete<A, E = never> = AtomResult.Success<CompleteQueryResult<A>, E>;

export type Success<A, E = never> = AtomResult.Success<QueryResult<A>, E>;

// based on: https://github.com/tim-smart/effect-atom/blob/04c15cacda42dd230782f52c0e978400793a502c/packages/atom/src/Result.ts#L424
export const match: {
  <A, E, W, X, Y, Z>(options: {
    readonly onInitial: (_: Initial<A, E>) => W;
    readonly onFailure: (_: Failure<A, E>) => X;
    readonly onPartial: (_: PartialQueryResult<A>) => Y;
    readonly onComplete: (_: CompleteQueryResult<A>) => Z;
  }): (self: Result<A, E>) => W | X | Y | Z;
  <A, E, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Initial<A, E>) => W;
      readonly onFailure: (_: Failure<A, E>) => X;
      readonly onPartial: (_: PartialQueryResult<A>) => Y;
      readonly onComplete: (_: CompleteQueryResult<A>) => Z;
    },
  ): W | X | Y | Z;
} = Fn.dual(
  2,
  <A, E, W, X, Y, Z>(
    self: Result<A, E>,
    options: {
      readonly onInitial: (_: Initial<A, E>) => W;
      readonly onFailure: (_: Failure<A, E>) => X;
      readonly onPartial: (_: PartialQueryResult<A>) => Y;
      readonly onComplete: (_: CompleteQueryResult<A>) => Z;
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
      AtomResult.failure<E, HumanReadable<A>>(failure.cause, {
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
