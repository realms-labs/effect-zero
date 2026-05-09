import type {
  Schema as ZeroSchema,
  ServerTransaction as ZeroServerTransaction,
  Transaction as ZeroTransaction,
} from "@rocicorp/zero";
import type { TransactionProviderHooks, ZQLDatabase } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import type * as ClientTransaction from "./client-transaction.js";
import {
  MutationAlreadyProcessedError,
  OutOfOrderMutationError,
  ServerSynchronizationContext,
  ServerTransactionInput,
} from "./internal/server.js";
import { prefixId } from "./internal/utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

export interface ServerTransactionContext {
  readonly _tag: unique symbol;
}

export const make = <const Id extends string, TSchema extends ZeroSchema, TTransaction>(
  id: Id,
  database: ZQLDatabase<TSchema, TTransaction>,
  clientTransaction: Context.TagClass<ClientTransaction.ClientTransaction, string, ZeroTransaction<TSchema>>,
) => {
  const ServerTransactionContext = Context.Tag(`${id}/ServerTransactionContext` as const)<
    ServerTransactionContext,
    { transaction: ZeroServerTransaction<TSchema, TTransaction>; transactionHooks: TransactionProviderHooks }
  >();

  const use = <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) =>
    Effect.flatMap(ServerTransactionContext, (ctx) =>
      Effect.tryPromise({
        try: (signal) => fn(ctx.transaction, { signal }),
        catch: (error) => new ServerTransactionError({ cause: Cause.fail(error) }),
      }),
    );

  const execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    const ctx = yield* Effect.context<
      ServerTransactionInput | Exclude<R, ClientTransaction.ClientTransaction | ServerTransactionContext>
    >();
    const result = yield* Deferred.make<A, E | Effect.Error<typeof checkAndIncrementLastMutationId>>();

    const transactionInput = yield* ServerTransactionInput;
    yield* Effect.tryPromise({
      try: (signal) =>
        database.transaction(async (transaction, transactionHooks) => {
          const exit = await Effect.flatMap(checkAndIncrementLastMutationId, () => effect).pipe(
            Effect.provide([
              Layer.succeed(ServerTransactionContext, { transaction, transactionHooks }),
              Layer.succeed(clientTransaction, transaction),
            ]),
            (effect) => Effect.runPromiseExitWith(ctx)(effect, { signal }),
          );
          Deferred.doneUnsafe(result, exit);
          return Exit.getOrElse(exit, () => {
            // This error's purpose is to differentiate between "external" errors
            // that originate from the user-defined mutator code and "internal" errors
            // that originate from our own code and the Zero API.
            // Both types are caught in the "catch" block below, but at this point we only need to handle
            // the "internal" errors wrapping them in a `ZeroDatabaseError`, because "external" errors
            // are already covered by passing the Exit result to the Deferred, which is why
            // we have the ServerTransactionUserError silenced below in the pipe.
            throw new ServerTransactionUserError();
          });
        }, transactionInput),
      catch: (error) => {
        if (ServerTransactionUserError.is(error)) {
          return error;
        }
        // This is for errors that occur when calling `database.transaction` despite the provided `effect` succeeding.
        // This can be caused by e.g. the database connection timing out or other database-related issues.
        return new ZeroDatabaseError({ cause: Cause.fail(error) });
      },
    }).pipe(Effect.catchTag("ServerTransactionUserError", () => Effect.void));

    return yield* Deferred.await(result);
  }, ServerSynchronizationContext.guard);

  const checkAndIncrementLastMutationId = Effect.gen(function* () {
    const { transactionHooks } = yield* ServerTransactionContext;
    const { clientID, mutationID: receivedMutationID } = yield* ServerTransactionInput;

    const { lastMutationID } = yield* Effect.tryPromise({
      try: () => transactionHooks.updateClientMutationID(),
      catch: (error) => new UpdateClientMutationIdError({ cause: Cause.fail(error) }),
    });

    if (receivedMutationID < lastMutationID) {
      return yield* new MutationAlreadyProcessedError({
        clientID,
        received: receivedMutationID,
        actual: lastMutationID,
      });
    }
    if (receivedMutationID > lastMutationID) {
      return yield* new OutOfOrderMutationError({
        clientID,
        receivedMutationID,
        lastMutationID,
      });
    }
  });

  return Object.assign(ServerTransactionContext, { use, execute });
};

class ServerTransactionError extends Data.TaggedError("ServerTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class UpdateClientMutationIdError extends Data.TaggedError("UpdateClientMutationIdError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class ZeroDatabaseError extends Data.TaggedError("ZeroDatabaseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

const ServerTransactionUserErrorTypeId = Symbol.for(prefixId("ServerTransactionUserError"));
class ServerTransactionUserError extends Data.TaggedError("ServerTransactionUserError") {
  readonly [ServerTransactionUserErrorTypeId] = ServerTransactionUserErrorTypeId;
  static is(e: unknown): e is ServerTransactionUserError {
    return Predicate.hasProperty(e, ServerTransactionUserErrorTypeId);
  }
}
