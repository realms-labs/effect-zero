import type { Schema as ZeroSchema, ServerTransaction as ZeroServerTransaction } from "@rocicorp/zero";
import type { ZQLDatabase } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import type * as Unify from "effect/Unify";
import * as ClientTransaction from "./client-transaction.js";
import type { TransactCallbackNotInvokedError, TransactInternalError } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { prefixId } from "./internal/utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const DatabaseSymbol = Symbol.for(prefixId("Database"));
export const ServerTransactionCallbackSymbol = Symbol.for(prefixId("ServerTransactionCallback"));

// biome-ignore lint/suspicious/noExplicitAny: any client transaction (its Id literal is irrelevant here)
export type Context<TSchema extends ZeroSchema, TTransaction> = ReturnType<typeof make<any, TSchema, TTransaction>>;

export const make = <const Id extends string, TSchema extends ZeroSchema, TTransaction>(
  id: Id,
  database: ZQLDatabase<TSchema, TTransaction>,
  clientTransaction: ClientTransaction.Context<TSchema>,
) => {
  class Context extends Ctx.Service<Context, ZeroServerTransaction<TSchema, TTransaction>>()(
    `${id as string}/ServerTransactionContext` as const,
  ) {}

  class ServerTransactionCallback extends Ctx.Service<
    ServerTransactionCallback,
    {
      readonly transact: <A, E, R>(
        fn: (transaction: ZeroServerTransaction<TSchema, TTransaction>) => Effect.Effect<A, E, R>,
      ) => Effect.Effect<A, E | TransactInternalError | TransactCallbackNotInvokedError, R>;
    }
  >()(`${id as string}/ServerTransactionCallback` as const) {}

  const use = Effect.fn(function* <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) {
    const transaction = yield* Context;
    return yield* Effect.tryPromise({
      try: (signal) => fn(transaction, { signal }),
      catch: (error) => new TransactionUseError({ cause: Cause.fail(error) }),
    });
  });

  const execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    const { transact } = yield* ServerTransactionCallback;
    return yield* transact((transaction) =>
      effect.pipe(
        // Provide the live transaction under both the server- and client-transaction tags.
        Effect.provide([
          Layer.succeed(Context, transaction),
          Layer.succeed(clientTransaction[ClientTransaction.ContextSymbol], transaction),
        ]),
      ),
    );
  }, ServerSynchronizationContext.guard);

  return {
    use,
    execute,
    [DatabaseSymbol]: database,
    [ServerTransactionCallbackSymbol]: ServerTransactionCallback,
  };
};

class TransactionUseError extends Data.TaggedError("TransactionUseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) ? error.message : "Internal error";
  }
}
