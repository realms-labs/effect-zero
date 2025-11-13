import type { HumanReadable, ReadonlyJSONValue, Zero, Query as ZeroQuery, Schema as ZeroSchema } from "@rocicorp/zero";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Hash from "effect/Hash";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Subscribable from "effect/Subscribable";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot.js";

export type Query<S extends ZeroSchema, T extends keyof S["tables"] & string, R> = ZeroQuery<S, T, R> & Equal.Equal;

export const make = <
  N extends string,
  A extends ReadonlyJSONValue[],
  B extends ReadonlyJSONValue[],
  S extends ZeroSchema,
  T extends keyof S["tables"] & string,
  R,
  E,
  R1,
  R2,
>(options: {
  name: N;
  payload: Schema.Schema<readonly [...B], readonly [...A], R1>;
  query: (...args: NoInfer<B>) => Effect.Effect<ZeroQuery<S, T, R>, E, R2>;
}) => {
  return Object.assign(
    Effect.fn(function* (...args: A) {
      const parsed = yield* Schema.decode(options.payload)(args);
      return yield* options.query(...parsed).pipe(
        Effect.map((rawQuery) => {
          const query = rawQuery.nameAndArgs(options.name, parsed) as Query<S, T, R>;
          query[Hash.symbol] = function () {
            return Hash.hash(this.hash());
          };
          query[Equal.symbol] = function (that) {
            if (Hash.isHash(that)) {
              return Equal.equals(this[Hash.symbol](), that[Hash.symbol]());
            }
            return false;
          };
          return query;
        }),
      );
    }),
    { queryName: options.name },
  );
};

// biome-ignore lint/suspicious/noExplicitAny: accept any query
export type MakeQueryResult<E = any, R1 = any, R2 = any> = ReturnType<
  // biome-ignore lint/suspicious/noExplicitAny: accept any query
  typeof make<string, any, any, ZeroSchema, string, any, E, R1, R2>
>;

export const subscribe = Effect.fn(function* <S extends ZeroSchema, T extends keyof S["tables"] & string, R>(
  zero: Zero<S>,
  query: ZeroQuery<S, T, R> | Query<S, T, R>,
) {
  const view = yield* Effect.acquireRelease(
    Effect.sync(() => zero.materialize(query)),
    (view) => Effect.sync(() => view.destroy()),
  );

  const subscriptionRef = yield* SubscriptionRef.make<ZeroQueryResult<R>>(getDefaultSnapshot(query.format.singular));

  yield* Stream.asyncEffect<Parameters<Parameters<(typeof view)["addListener"]>[0]>>((emit) =>
    Effect.sync(() => view.addListener((...args) => emit.single(args))),
  ).pipe(
    Stream.mapEffect(([data, resultType]) =>
      Effect.sync(() => {
        // logic here borrowed from: https://github.com/rocicorp/mono/blob/288b00ec94f5a9ae6e988513423af25c281dbb2a/packages/zero-react/src/use-query.tsx#L295
        const cloned = (data === undefined ? data : deepClone(data as ReadonlyJSONValue)) as HumanReadable<R>;
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
