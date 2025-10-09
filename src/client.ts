import { Atom } from "@effect-atom/atom";
import type { HumanReadable, Query, ReadonlyJSONValue, Schema, Transaction } from "@rocicorp/zero";
import type { QueryResult } from "@rocicorp/zero/react";
import type * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Stream from "effect/Stream";
import * as Subscribable from "effect/Subscribable";
import * as SubscriptionRef from "effect/SubscriptionRef";
import type { ExtractMutatorSchemaRequirements, AnyZeroMutators } from "./mutators";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot";
import { prefixId } from "./utils";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";

// Updated to: https://github.com/rocicorp/mono/blob/2e18f2e1d084c530ebd9bd7fef9bb848e607cc19/packages/zero-pg/src/push-processor.ts

export interface ZeroClientTransaction {
  readonly _tag: unique symbol;
}

export const makeClient = <S extends Schema>() => {
  // TODO: Maybe prefix this / suffix this with a tag passed in to `make`?
  const ZeroClientTransaction = Context.Tag(prefixId("ZeroClientTransaction"))<ZeroClientTransaction, Transaction<S>>();

  const use = <A>(fn: (transaction: Transaction<S>, options: { readonly signal: AbortSignal }) => PromiseLike<A>) =>
    Effect.andThen(ZeroClientTransaction, (transaction) =>
      Effect.promise((signal) => fn(transaction, { signal })),
    ).pipe(
      Effect.tapErrorCause((cause) => Effect.logError(cause)),
      Effect.sandbox,
      Effect.annotateLogs({ module: prefixId("ZeroClient.Transaction"), method: "use" }),
      Effect.mapError((cause) => new ZeroClientTransactionError({ cause })),
    );

  const unwrapMutators = <T extends AnyZeroMutators>(mutators: T) => {
    return Effect.gen(function* () {
      const runtime = yield* Effect.runtime<Exclude<ExtractMutatorSchemaRequirements<T>, ZeroClientTransaction>>();

      function unwrapMutator(
        mutator: (args: unknown) => Effect.Effect<void, unknown, ExtractMutatorSchemaRequirements<T>>,
      ) {
        return (tx: Transaction<S>, args: unknown) => {
          return mutator(args).pipe(Effect.provideService(ZeroClientTransaction, tx), (effect) =>
            Runtime.runPromise(runtime, effect),
          );
        };
      }

      return Rec.map(mutators, (v) =>
        Match.value(v).pipe(Match.when(Predicate.isFunction, unwrapMutator), Match.orElse(Rec.map(unwrapMutator))),
      ) as UnwrappedMutatorSchema<S, T>;
    });
  };

  const querySub = Effect.fn(function* <T extends keyof S["tables"] & string, R>(query: Query<S, T, R>) {
    const view = yield* Effect.acquireRelease(
      Effect.sync(() => query.materialize()),
      (view) => Effect.sync(() => view.destroy()),
    );

    const subscriptionRef = yield* SubscriptionRef.make<QueryResult<R>>(getDefaultSnapshot(query.format.singular));

    yield* Stream.asyncEffect<Parameters<Parameters<(typeof view)["addListener"]>[0]>>((emit) =>
      Effect.sync(() => view.addListener((...args) => emit.single(args))),
    ).pipe(
      Stream.mapEffect(([data, resultType]) =>
        Effect.sync(() => {
          // logic here borrowed from: https://github.com/rocicorp/mono/blob/288b00ec94f5a9ae6e988513423af25c281dbb2a/packages/zero-react/src/use-query.tsx#L295
          const cloned = data === undefined ? data : (deepClone(data as ReadonlyJSONValue) as HumanReadable<R>);
          return getSnapshot<R>(query.format.singular, cloned, resultType);
        }),
      ),
      Stream.runForEach((snapshot) => SubscriptionRef.set(subscriptionRef, snapshot)),
      Effect.forkScoped,
    );

    return subscriptionRef.pipe(
      Subscribable.map(([data, { type: status }]) => ({
        data,
        status,
      })),
    );
  });

  /** Create an Atom for the query */
  const queryAtom = Atom.family(<T extends keyof S["tables"] & string, R>(query: Query<S, T, R>) =>
    Atom.subscribable(querySub(query)).pipe(Atom.mapResult((res) => res.data)),
  );

  return {
    unwrapMutators,
    querySub,
    queryAtom,
    Transaction: Object.assign(ZeroClientTransaction, { use }),
  };
};

export type UnwrappedMutatorSchema<S extends Schema, T extends AnyZeroMutators> = {
  [A in keyof T]: T[A] extends (...args: infer TArgs) => unknown
    ? (transaction: Transaction<S>, ...args: TArgs) => Promise<void>
    : {
        [B in keyof T[A]]: T[A][B] extends (...args: infer TArgs) => unknown
          ? (transaction: Transaction<S>, ...args: TArgs) => Promise<void>
          : never;
      };
} & {};

class ZeroClientTransactionError extends Data.TaggedError("ZeroClientTransactionError")<{
  cause: Cause.Cause<unknown>;
}> {}
