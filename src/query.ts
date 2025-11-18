import type { HumanReadable, ReadonlyJSONValue, Zero, Query as ZeroQuery, Schema as ZeroSchema } from "@rocicorp/zero";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import * as Subscribable from "effect/Subscribable";
import * as SubscriptionRef from "effect/SubscriptionRef";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot.js";

export const subscribe = Effect.fn(function* <S extends ZeroSchema, T extends keyof S["tables"] & string, R>(
  // TODO: this will be used as the delegate in the synced queries update.
  zero: Zero<S>,
  // TODO: switch to our own Query type wrapper which implements Hash, Equal, etc. in synced queries update.
  query: ZeroQuery<S, T, R>,
) {
  const view = yield* Effect.acquireRelease(
    Effect.sync(() => query.materialize()),
    (view) => Effect.sync(() => view.destroy()),
  );

  const subscriptionRef = yield* SubscriptionRef.make<ZeroQueryResult<R>>(getDefaultSnapshot(query.format.singular));

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
