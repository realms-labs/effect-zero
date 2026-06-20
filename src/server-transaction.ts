import type { Schema as ZeroSchema, ServerTransaction as ZeroServerTransaction } from "@rocicorp/zero";
import { OutOfOrderMutation, type TransactFn, type ZQLDatabase } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { toApplicationError } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/process-mutations.ts

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction (its Id literal is irrelevant here)
export type Context<TSchema extends ZeroSchema, TTransaction> = ReturnType<typeof make<any, TSchema, TTransaction>>;

export const make = <const Id extends string, TSchema extends ZeroSchema, TTransaction>(
  id: Id,
  database: ZQLDatabase<TSchema, TTransaction>,
  clientTransaction: ClientTransaction.Context<TSchema>,
) => {
  type Database = ZQLDatabase<TSchema, TTransaction>;

  // The live Zero transaction, available only while a `transact` callback is executing.
  class Context extends Ctx.Service<Context, ZeroServerTransaction<TSchema, TTransaction>>()(
    `${id as string}/ServerTransactionContext` as const,
  ) {}

  // The capability `execute` uses to run this mutation's transaction: the upstream `transact`, wrapped
  // by `handleMutate` so the resulting `MutationResponse` is also captured for it to return.
  class Mutation extends Ctx.Service<Mutation, TransactFn<Database>>()(
    `${id as string}/ServerTransactionMutation` as const,
  ) {}

  const use = <A>(
    fn: (
      transaction: ZeroServerTransaction<TSchema, TTransaction>,
      options: { readonly signal: AbortSignal },
    ) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const transaction = yield* Context;
      return yield* Effect.tryPromise({
        try: (signal) => fn(transaction, { signal }),
        catch: (error) => new ServerTransactionError({ cause: Cause.fail(error) }),
      });
    });

  const execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    const ctx = yield* Effect.context<Exclude<R, (typeof clientTransaction.Context)["Identifier"] | Context>>();
    const transact = yield* Mutation;
    // Bridges the inner effect's value/failure back out of the async `transact` callback to the mutator.
    const result = yield* Deferred.make<A, E | MutationShortCircuit>();

    yield* Effect.tryPromise({
      try: (signal) =>
        // The upstream `transact` opens the DB transaction, runs the last-mutation-id check
        // (throwing OutOfOrderMutation / already-processed itself), invokes this callback to perform
        // the writes, and resolves with the resulting MutationResponse (or with an app-error response
        // if this callback throws an ApplicationError).
        transact(async (transaction) => {
          const exit = await effect.pipe(
            Effect.provide([
              Layer.succeed(Context, transaction),
              Layer.succeed(clientTransaction.Context, transaction),
            ]),
            (eff) => Effect.runPromiseExitWith(ctx)(eff, { signal }),
          );
          Deferred.doneUnsafe(result, exit);
          if (Exit.isFailure(exit)) {
            // Throw an upstream ApplicationError so `transact` retries the transaction without the
            // mutator and writes the app-error result, then resolves with that response.
            throw toApplicationError(exit.cause);
          }
        }),
      // OutOfOrderMutation must escape unconverted so it reaches handleMutateRequest's top-level
      // handler (which produces a PushFailed response); everything else is an internal DB error.
      catch: (error) =>
        error instanceof OutOfOrderMutation ? error : new ExecuteTransactError({ cause: Cause.fail(error) }),
    });

    // If `transact` resolved without ever invoking our callback (e.g. an already-processed mutation,
    // whose last-mutation-id check fails before the callback runs), `result` was never completed.
    // Fail it (a no-op when already done) so `handleMutate` surfaces the captured response instead of hanging.
    yield* Deferred.fail(result, new MutationShortCircuit());
    return yield* Deferred.await(result);
  }, ServerSynchronizationContext.guard);

  return { Context, Mutation, database, use, execute };
};

class ServerTransactionError extends Data.TaggedError("ServerTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class ExecuteTransactError extends Data.TaggedError("ExecuteTransactError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class MutationShortCircuit extends Data.TaggedError("MutationShortCircuit") {}
