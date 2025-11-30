import type { TransactionProviderInput } from "@rocicorp/zero/server";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Predicate from "effect/Predicate";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { prefixId } from "./utils.js";

export class ServerTransactionInput extends Context.Tag(prefixId("ServerTransactionInput"))<
  ServerTransactionInput,
  TransactionProviderInput
>() {}

export class ServerSynchronizationContext extends Effect.Service<ServerSynchronizationContext>()(
  prefixId("ServerSynchronizationContext"),
  {
    effect: Effect.gen(function* () {
      return {
        wasTransactionExecuted: yield* SynchronizedRef.make(false),
      };
    }),
  },
) {
  // Ensures that only one transaction is executed at a time and checks that another transaction wasn't already executed.
  static guard = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(this, (ctx) =>
      SynchronizedRef.modifyEffect(
        ctx.wasTransactionExecuted,
        Effect.fn(function* (wasTransactionExecuted) {
          if (wasTransactionExecuted) {
            return yield* new MultipleTransactionsError();
          }
          const result = yield* effect;
          return [result, true];
        }),
      ),
    );

  static finalize = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.gen(this, function* () {
      const wasTransactionExecuted = yield* this.pipe(Effect.map((ctx) => ctx.wasTransactionExecuted));

      return yield* effect.pipe(
        // Case #2 "One transaction then fail"
        // If the transaction was executed successfully, swallow the error and just log it, otherwise re-throw it
        Effect.catchAllCause(
          Effect.fn(function* (e) {
            if (yield* wasTransactionExecuted) {
              return yield* Effect.logError("Error occurred after transaction execution completed", e);
            }
            return yield* Effect.failCause(e);
          }),
        ),
        // Case #4 "Zero transactions then succeed"
        // Check that the transaction was executed during the mutation
        Effect.tap(
          Effect.gen(function* () {
            if (!(yield* wasTransactionExecuted)) {
              return yield* new NoTransactionError();
            }
          }),
        ),
      );
    });
}

export class NoTransactionError extends Data.TaggedError("NoTransactionError") {}
export class MultipleTransactionsError extends Data.TaggedError("MultipleTransactionsError") {}

const OutOfOrderMutationErrorTypeId = Symbol.for(prefixId("OutOfOrderMutationError"));
export class OutOfOrderMutationError extends Data.TaggedError("OutOfOrderMutationError")<{
  readonly clientID: string;
  readonly receivedMutationID: number;
  readonly lastMutationID: number | bigint;
}> {
  readonly [OutOfOrderMutationErrorTypeId] = OutOfOrderMutationErrorTypeId;
  override get message() {
    return `Client ${this.clientID} sent mutation ID ${this.receivedMutationID} but expected ${this.lastMutationID}`;
  }
  static is(e: unknown): e is OutOfOrderMutationError {
    return Predicate.hasProperty(e, OutOfOrderMutationErrorTypeId);
  }
}

const MutationAlreadyProcessedErrorTypeId = Symbol.for(prefixId("MutationAlreadyProcessedError"));
export class MutationAlreadyProcessedError extends Data.TaggedError("MutationAlreadyProcessedError")<{
  readonly clientID: string;
  readonly received: number;
  readonly actual: number | bigint;
}> {
  readonly [MutationAlreadyProcessedErrorTypeId] = MutationAlreadyProcessedErrorTypeId;
  override get message() {
    return `Ignoring mutation from ${this.clientID} with ID ${this.received} as it was already processed. Expected: ${this.actual}`;
  }
  static is(e: unknown): e is MutationAlreadyProcessedError {
    return Predicate.hasProperty(e, MutationAlreadyProcessedErrorTypeId);
  }
}
