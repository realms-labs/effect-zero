import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";

type ClientTransactionIdentifier<Id extends string, TSchema extends ZeroSchema> = Context.ServiceClass.Shape<
  Id,
  ZeroTransaction<TSchema>
>;

type ClientTransactionService<Id extends string, TSchema extends ZeroSchema> = Context.ServiceClass<
  ClientTransactionIdentifier<Id, TSchema>,
  Id,
  ZeroTransaction<TSchema>
> & {
  readonly schema: TSchema;
  readonly usePromise: <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) => Effect.Effect<A, ClientTransactionError, ClientTransactionIdentifier<Id, TSchema>>;
};

export const make = <const Id extends string, TSchema extends ZeroSchema>(
  id: Id,
  schema: TSchema,
): ClientTransactionService<Id, TSchema> => {
  class ClientTransaction extends Context.Service<ClientTransaction, ZeroTransaction<TSchema>>()(id) {
    static readonly schema = schema;

    static usePromise<A>(
      fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
    ) {
      return Effect.gen(function* () {
        const transaction = yield* ClientTransaction;
        return yield* Effect.tryPromise({
          try: (signal) => fn(transaction, { signal }),
          catch: (error) => new ClientTransactionError({ cause: Cause.fail(error) }),
        });
      });
    }
  }

  return ClientTransaction;
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}
