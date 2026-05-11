import type { Schema as ZeroSchema, ServerTransaction as ZeroServerTransaction } from "@rocicorp/zero";
import type { TransactionProviderHooks, ZQLDatabase } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import type * as ClientTransaction from "./client-transaction.js";
import { MutationAlreadyProcessedError, OutOfOrderMutationError, ServerTransactionInput } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { prefixId } from "./internal/utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

type _ = NodeInspectSymbol;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction (its Id literal is irrelevant here)
export type Context<TSchema extends ZeroSchema, TTransaction> = ReturnType<typeof make<any, TSchema, TTransaction>>;

export const make = <const Id extends string, TSchema extends ZeroSchema, TTransaction>(
  id: Id,
  database: ZQLDatabase<TSchema, TTransaction>,
  clientTransaction: ClientTransaction.Context<TSchema>,
) => {
  class Context extends Ctx.Service<
    Context,
    { transaction: ZeroServerTransaction<TSchema, TTransaction>; transactionHooks: TransactionProviderHooks }
  >()(`${id}/ServerTransactionContext` as `${string}/ServerTransactionContext`) {}

  const use = <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const ctx = yield* Context;
      return yield* Effect.tryPromise({
        try: (signal) => fn(ctx.transaction, { signal }),
        catch: (error) => new ServerTransactionError({ cause: Cause.fail(error) }),
      });
    });

  const execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    const ctx = yield* Effect.context<
      ServerTransactionInput | Exclude<R, (typeof clientTransaction.Context)["Identifier"] | Context>
    >();
    const result = yield* Deferred.make<A, E | Effect.Error<typeof checkAndIncrementLastMutationId>>();

    const transactionInput = yield* ServerTransactionInput;
    yield* Effect.tryPromise({
      try: (signal) =>
        database.transaction(async (transaction, transactionHooks) => {
          const exit = await Effect.flatMap(checkAndIncrementLastMutationId, () => effect).pipe(
            Effect.provide([
              Layer.succeed(Context, { transaction, transactionHooks }),
              Layer.succeed(clientTransaction.Context, transaction),
            ]),
            (effect) => Effect.runPromiseExitWith(ctx)(effect, { signal }),
          );
          Deferred.doneUnsafe(result, exit);
          if (Exit.isFailure(exit)) {
            // This error's purpose is to differentiate between "external" errors
            // that originate from the user-defined mutator code and "internal" errors
            // that originate from our own code and the Zero API.
            // Both types are caught in the "catch" block below, but at this point we only need to handle
            // the "internal" errors wrapping them in a `ZeroDatabaseError`, because "external" errors
            // are already covered by passing the Exit result to the Deferred, which is why
            // we have the ServerTransactionUserError silenced below in the pipe.
            throw new ServerTransactionUserError();
          }
          return exit.value;
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
    const { transactionHooks } = yield* Context;
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

  return { Context, use, execute };
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
