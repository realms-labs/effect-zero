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
import type * as Types from "effect/Types";
import type * as ClientTransaction from "./client-transaction.js";
import {
  type MultipleTransactionsError,
  MutationAlreadyProcessedError,
  OutOfOrderMutationError,
  ServerTransactionInput,
} from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { prefixId } from "./internal/utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

export interface ServerTransactionContext {
  readonly _tag: unique symbol;
}

type ServerTransactionContextShape<TSchema extends ZeroSchema, TTransaction> = {
  transaction: ZeroServerTransaction<TSchema, TTransaction>;
  transactionHooks: TransactionProviderHooks;
};

export interface ServerTransactionTag<Id extends string, TSchema extends ZeroSchema, TTransaction>
  extends Context.ServiceClass<
    ServerTransactionContext,
    `${Id}/ServerTransactionContext`,
    ServerTransactionContextShape<TSchema, TTransaction>
  > {
  readonly usePromise: <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) => Effect.Effect<A, ServerTransactionError, ServerTransactionContext>;
  readonly execute: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<
    A,
    | E
    | MultipleTransactionsError
    | OutOfOrderMutationError
    | MutationAlreadyProcessedError
    | UpdateClientMutationIdError
    | ZeroDatabaseError,
    | ServerSynchronizationContext.ServerSynchronizationContext
    | ServerTransactionInput
    | Exclude<R, ClientTransaction.ClientTransaction | ServerTransactionContext>
  >;
}

export const make = <const Id extends string, TSchema extends ZeroSchema, TTransaction>(
  id: Id,
  database: ZQLDatabase<TSchema, TTransaction>,
  clientTransaction: Context.Key<ClientTransaction.ClientTransaction, ZeroTransaction<TSchema>>,
): ServerTransactionTag<Id, TSchema, TTransaction> => {
  class Tag extends Context.Service<ServerTransactionContext, ServerTransactionContextShape<TSchema, TTransaction>>()(
    `${id}/ServerTransactionContext` as const,
  ) {}

  // Mutate-via-Mutable-cast pattern from effect-smol/src/Layer/LayerMap.ts:307-374. Attaches
  // `usePromise` and `execute` directly on the Service-derived class. Promise-based `usePromise`
  // is named distinctly so it doesn't shadow the inherited `Context.Service.use`.
  const Tag_ = Tag as unknown as Types.Mutable<ServerTransactionTag<Id, TSchema, TTransaction>>;

  Tag_.usePromise = <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const ctx = yield* Tag;
      return yield* Effect.tryPromise({
        try: (signal) => fn(ctx.transaction, { signal }),
        catch: (error) => new ServerTransactionError({ cause: Cause.fail(error) }),
      });
    });

  Tag_.execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
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
              Layer.succeed(Tag, { transaction, transactionHooks }),
              Layer.succeed(clientTransaction, transaction),
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
    const { transactionHooks } = yield* Tag;
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

  return Tag_ as ServerTransactionTag<Id, TSchema, TTransaction>;
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
