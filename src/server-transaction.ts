import type { Schema as ZeroSchema, ServerTransaction as ZeroServerTransaction } from "@rocicorp/zero";
import { OutOfOrderMutation, type TransactFn, type ZQLDatabase } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import type * as SynchronizedRef from "effect/SynchronizedRef";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { toApplicationError } from "./internal/server.js";
import { finalize, guard } from "./internal/server-synchronization-context.js";

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

  // The per-mutation channel `execute` and `finalize` use: the upstream `transact` (wrapped by
  // `handleMutate` to capture its response) plus the synchronization flag tracking whether a
  // transaction has committed (formerly a separate ServerSynchronizationContext service).
  class Mutation extends Ctx.Service<
    Mutation,
    { readonly transact: TransactFn<Database>; readonly executed: SynchronizedRef.SynchronizedRef<boolean> }
  >()(`${id as string}/ServerTransactionMutation` as const) {}

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
    const { transact } = yield* Mutation;
    // Captures the inner effect's Exit out of the async `transact` callback. Upstream awaits the callback
    // before `transact` resolves, so a plain closure variable suffices — no Deferred rendezvous needed.
    let exit: Exit.Exit<A, E> | undefined;

    yield* Effect.tryPromise({
      try: (signal) =>
        // The upstream `transact` opens the DB transaction, runs the last-mutation-id check
        // (throwing OutOfOrderMutation / already-processed itself), invokes this callback to perform
        // the writes, and resolves with the resulting MutationResponse (or with an app-error response
        // if this callback throws an ApplicationError).
        transact(async (transaction) => {
          exit = await effect.pipe(
            Effect.provide([
              Layer.succeed(Context, transaction),
              Layer.succeed(clientTransaction.Context, transaction),
            ]),
            (eff) => Effect.runPromiseExitWith(ctx)(eff, { signal }),
          );
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
    // whose last-mutation-id check fails before the callback runs), `exit` is unset — short-circuit so
    // `handleMutate` surfaces the captured response instead of hanging.
    if (exit === undefined) return yield* new MutationShortCircuit();
    return yield* exit;
  }, guard(Mutation));

  return { Context, Mutation, database, use, execute, finalize: finalize(Mutation) };
};

class ServerTransactionError extends Data.TaggedError("ServerTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class ExecuteTransactError extends Data.TaggedError("ExecuteTransactError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class MutationShortCircuit extends Data.TaggedError("MutationShortCircuit") {}
