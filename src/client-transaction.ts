import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";

// Necessary workaround for TS declaration generation
export interface ClientTransaction {
  readonly _tag: unique symbol;
}

export const make = <const Id extends string, TSchema extends ZeroSchema>(id: Id, schema: TSchema) => {
  const ClientTransaction = Context.Service<ClientTransaction, ZeroTransaction<TSchema>>()(id);

  const use = <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const transaction = yield* ClientTransaction;
      return yield* Effect.tryPromise({
        try: (signal) => fn(transaction, { signal }),
        catch: (error) => new ClientTransactionError({ cause: Cause.fail(error) }),
      });
    });

  // Cast away the inherited `Service.use` from `Context.Service` so our custom
  // promise-based `use` is the sole overload visible to callers.
  return Object.assign(ClientTransaction, { use, schema }) as unknown as Omit<typeof ClientTransaction, "use"> & {
    readonly use: typeof use;
    readonly schema: TSchema;
  };
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}
