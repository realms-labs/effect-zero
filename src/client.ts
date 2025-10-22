import { Atom } from "@effect-atom/atom";
import type { AtomRuntime } from "@effect-atom/atom/Atom";
import type { AtomRegistry } from "@effect-atom/atom/Registry";
import type { HumanReadable, Query, ReadonlyJSONValue, Transaction, Zero, Schema as ZeroSchema } from "@rocicorp/zero";
import type { QueryResult } from "@rocicorp/zero/react";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import type * as ParseResult from "effect/ParseResult";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as Subscribable from "effect/Subscribable";
import * as SubscriptionRef from "effect/SubscriptionRef";
import {
  type AnyMutator,
  type AnyMutators,
  type ExtractMutatorsRequirements,
  MutatorArgsSchemaSym,
} from "./mutators.js";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot.js";
import { prefixId } from "./utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/2e18f2e1d084c530ebd9bd7fef9bb848e607cc19/packages/zero-pg/src/push-processor.ts

// Necessary workaround for TS declaration generation
export interface ZeroClientTransaction {
  readonly _tag: unique symbol;
}

export interface ZeroClientProvider {
  readonly _tag: unique symbol;
}

export const makeClient = <S extends ZeroSchema>() => {
  // TODO: Maybe prefix this / suffix this with a tag passed in to `make`?
  const ZeroClientTransaction = Context.Tag(prefixId("ZeroClientTransaction"))<ZeroClientTransaction, Transaction<S>>();

  const ZeroClientProvider = Context.Tag(prefixId("ZeroClientProvider"))<
    ZeroClientProvider,
    Effect.Effect<Zero<S>, never, Scope.Scope | AtomRegistry>
  >();

  const use = <A>(fn: (transaction: Transaction<S>, options: { readonly signal: AbortSignal }) => PromiseLike<A>) =>
    Effect.flatMap(ZeroClientTransaction, (transaction) =>
      Effect.promise((signal) => fn(transaction, { signal })),
    ).pipe(
      Effect.tapErrorCause((cause) => Effect.logError(cause)),
      Effect.sandbox,
      Effect.annotateLogs({ module: prefixId("ZeroClient.Transaction"), method: "use" }),
      Effect.mapError((cause) => new ZeroClientTransactionError({ cause })),
    );

  const unwrapMutators = Effect.fn(function* <T extends AnyMutators>(mutators: T) {
    const runtime = yield* Effect.runtime<Exclude<ExtractMutatorsRequirements<T>, ZeroClientTransaction>>();

    function unwrapMutator<E>(mutator: AnyMutator<ExtractMutatorsRequirements<T>, E>) {
      return async (tx: Transaction<S>, args: unknown) => {
        const exit = await Schema.decode(mutator[MutatorArgsSchemaSym])(args).pipe(
          Effect.catchTag("ParseError", (e) => new ZeroClientArgsParseError({ cause: Cause.fail(e) })),
          Effect.flatMap(mutator),
          Effect.provideService(ZeroClientTransaction, tx),
          Runtime.runPromiseExit(runtime),
        );
        return Exit.getOrElse(exit, (c) => {
          // Extract underlying error bypassing FiberFailure
          throw Cause.squash(c);
        });
      };
    }

    return Rec.map(mutators, (v) =>
      Match.value(v).pipe(Match.when(Predicate.isFunction, unwrapMutator), Match.orElse(Rec.map(unwrapMutator))),
    ) as UnwrapMutators<S, T>;
  });

  const querySub = Effect.fn(function* <T extends keyof S["tables"] & string, R>(query: Query<S, T, R>) {
    const view = yield* Effect.acquireRelease(
      Effect.gen(function* () {
        if (Predicate.hasProperty(query, "_delegate") && query._delegate !== undefined) {
          return yield* Effect.sync(() => query.materialize());
        }
        const zero = yield* Effect.serviceOption(ZeroClientProvider).pipe(
          Effect.map(
            Option.getOrThrowWith(
              () =>
                new Error("unable to materialize query, because it has no delegate, and ZeroProvider was not provided"),
            ),
          ),
          Effect.flatten,
        );
        return yield* Effect.sync(() => {
          return zero.materialize(query);
        });
      }),
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
  // const queryAtom = Atom.family(<T extends keyof S["tables"] & string, R>(query: Query<S, T, R>) => {
  //   return Atom.subscribable(querySub<T, R>(query)).pipe(Atom.mapResult((res) => res.data));
  // });
  const queryAtom = Atom.family(
    <T extends keyof S["tables"] & string, R>({
      runtime,
      query,
    }: {
      runtime: AtomRuntime<ZeroClientProvider>;
      query: Query<S, T, R>;
    }) => {
      return runtime
        .subscribable(
          Effect.fn(function* (get) {
            const scope = yield* Scope.make();
            get.addFinalizer(() => {
              Scope.close(scope, Exit.void).pipe(Effect.runPromise);
            });
            return yield* querySub(query).pipe(Scope.extend(scope));
          }),
        )
        .pipe(Atom.mapResult((res) => res.data));
    },
  );

  return {
    unwrapMutators,
    querySub,
    queryAtom,
    Transaction: Object.assign(ZeroClientTransaction, { use }),
    ZeroProvider: ZeroClientProvider,
  };
};

type UnwrapMutator<S extends ZeroSchema, T extends AnyMutator> = Parameters<T> extends []
  ? (transaction: Transaction<S>) => Promise<void>
  : (transaction: Transaction<S>, args: Schema.Schema.Encoded<T[typeof MutatorArgsSchemaSym]>) => Promise<void>;

export type UnwrapMutators<S extends ZeroSchema, T extends AnyMutators> = {
  [A in keyof T]: T[A] extends AnyMutator
    ? UnwrapMutator<S, T[A]>
    : { [B in keyof T[A]]: T[A][B] extends AnyMutator ? UnwrapMutator<S, T[A][B]> : never };
} & {};

export class ZeroClientTransactionError extends Data.TaggedError("ZeroClientTransactionError")<{
  cause: Cause.Cause<unknown>;
}> {}

export class ZeroClientArgsParseError extends Data.TaggedError("ZeroClientArgsParseError")<{
  cause: Cause.Cause<ParseResult.ParseError>;
}> {}
