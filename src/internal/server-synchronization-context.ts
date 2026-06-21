import type * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { MultipleTransactionsError, NoTransactionError } from "./server.js";

// The per-mutation synchronization state. It lives on the server-transaction `Mutation` service (rather
// than a dedicated service), so `guard`/`finalize` are parameterized by the tag that carries it.
export type Synchronization = { readonly executed: SynchronizedRef.SynchronizedRef<boolean> };

// Ensures that at most one transaction is executed per mutation, and records that one committed.
export const guard =
  <I, V extends Synchronization>(tag: Context.Key<I, V>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const { executed } = yield* tag;
      return yield* SynchronizedRef.modifyEffect(
        executed,
        Effect.fn(function* (wasExecuted) {
          if (wasExecuted) {
            return yield* new MultipleTransactionsError();
          }
          const result = yield* effect;
          return [result, true];
        }),
      );
    });

// Enforces that a mutation runs at least one transaction, and that an error occurring AFTER a committed
// transaction is swallowed (the DB state already changed, so the push response must report success).
export const finalize =
  <I, V extends Synchronization>(tag: Context.Key<I, V>) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(function* () {
      const { executed } = yield* tag;

      return yield* effect.pipe(
        // Case #2 "One transaction then fail"
        // If the transaction was executed successfully, swallow the error and just log it, otherwise re-throw it
        Effect.catchCause(
          Effect.fn(function* (e) {
            if (yield* SynchronizedRef.get(executed)) {
              return yield* Effect.logError("Error occurred after transaction execution completed", e);
            }
            return yield* Effect.failCause(e);
          }),
        ),
        // Case #4 "Zero transactions then succeed"
        // Check that the transaction was executed during the mutation
        Effect.tap(
          Effect.gen(function* () {
            if (!(yield* SynchronizedRef.get(executed))) {
              return yield* new NoTransactionError();
            }
          }),
        ),
      );
    });
