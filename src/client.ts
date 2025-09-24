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
import { prefixId } from "./utils";
import type { MutatorArgs, MutatorSchema } from "./mutators";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot";

export const makeClient = <S extends Schema>() => {
  // TODO: Maybe prefix this / suffix this with a tag passed in to `make`?
  class ZeroClientTransaction extends Context.Tag(prefixId("ZeroClientTransaction"))<
    ZeroClientTransaction,
    Transaction<S>
  >() {
    static use = <A>(fn: (transaction: Transaction<S>, options: { readonly signal: AbortSignal }) => PromiseLike<A>) =>
      Effect.andThen(ZeroClientTransaction, (transaction) =>
        Effect.promise((signal) => fn(transaction, { signal })),
      ).pipe(
        Effect.tapErrorCause((cause) => Effect.logError(cause)),
        Effect.sandbox,
        Effect.annotateLogs({ module: prefixId("ZeroClient.Transaction"), method: "use" }),
        Effect.mapError((cause) => new ZeroClientTransactionError({ cause })),
      );
  }

  const mutators =
    <M extends MutatorArgs>() =>
      <R>(mutators: MutatorSchema<R, M>) =>
        mutators;

  const unwrapMutators = <M extends MutatorArgs, R>(mutators: MutatorSchema<R, M>) => {
    return Effect.gen(function* () {
      const runtime = yield* Effect.runtime<Exclude<R, ZeroClientTransaction>>();
      return Rec.map(
        mutators,
        Rec.map(
          // NOTE:
          // biome-ignore lint/suspicious/noExplicitAny: think there's no way around this, no way to maintain the type of `args`
          (mutator) => (tx: Transaction<S>, args: any) => {
            return mutator(args).pipe(Effect.provideService(ZeroClientTransaction, tx), (effect) =>
              Runtime.runPromise(runtime, effect),
            );
          },
        ),
      ) as UnwrappedMutatorSchema<S, M>;
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
    mutators,
    unwrapMutators,
    querySub,
    queryAtom,
    Transaction: ZeroClientTransaction,
  };
};

export type UnwrappedMutatorSchema<S extends Schema, M extends MutatorArgs> = {
  [A in keyof M]: {
    [B in keyof M[A]]: (transaction: Transaction<S>, args: M[A][B]) => Promise<void>;
  };
};

class ZeroClientTransactionError extends Data.TaggedError("ZeroClientTransactionError")<{
  cause: Cause.Cause<unknown>;
}> { }
