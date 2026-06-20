import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { MultipleTransactionsError, NoTransactionError } from "./server.js";
import { prefixId } from "./utils.js";

export class ServerSynchronizationContext extends Context.Service<ServerSynchronizationContext>()(
  prefixId("ServerSynchronizationContext"),
  {
    make: Effect.gen(function* () {
      return {
        wasTransactionExecuted: yield* SynchronizedRef.make(false),
      };
    }),
  },
) {}

export const layer: Layer.Layer<ServerSynchronizationContext> = Layer.effect(ServerSynchronizationContext)(
  ServerSynchronizationContext.make,
);

// Ensures that only one transaction is executed at a time and checks that another transaction wasn't already executed.
export const guard = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const ctx = yield* ServerSynchronizationContext;
    return yield* SynchronizedRef.modifyEffect(
      ctx.wasTransactionExecuted,
      Effect.fn(function* (wasTransactionExecuted) {
        if (wasTransactionExecuted) {
          return yield* new MultipleTransactionsError();
        }
        const result = yield* effect;
        return [result, true];
      }),
    );
  });

export const finalize = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  Effect.gen(function* () {
    const { wasTransactionExecuted } = yield* ServerSynchronizationContext;

    return yield* effect.pipe(
      // Case #2 "One transaction then fail"
      // If the transaction was executed successfully, swallow the error and just log it, otherwise re-throw it
      Effect.catchCause(
        Effect.fn(function* (e) {
          if (yield* SynchronizedRef.get(wasTransactionExecuted)) {
            return yield* Effect.logError("Error occurred after transaction execution completed", e);
          }
          return yield* Effect.failCause(e);
        }),
      ),
      // Case #4 "Zero transactions then succeed"
      // Check that the transaction was executed during the mutation
      Effect.tap(
        Effect.gen(function* () {
          if (!(yield* SynchronizedRef.get(wasTransactionExecuted))) {
            return yield* new NoTransactionError();
          }
        }),
      ),
    );
  });
