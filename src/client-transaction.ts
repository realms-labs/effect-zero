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
  const ClientTransaction = Context.Tag(id)<ClientTransaction, ZeroTransaction<TSchema>>();

  const use = <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) =>
    Effect.flatMap(ClientTransaction, (transaction) =>
      Effect.tryPromise({
        try: (signal) => fn(transaction, { signal }),
        catch: (error) => new ClientTransactionError({ cause: Cause.fail(error) }),
      }),
    );

  return Object.assign(ClientTransaction, { use, schema });
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}
