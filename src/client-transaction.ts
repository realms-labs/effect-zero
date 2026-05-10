import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type * as Types from "effect/Types";

// Necessary workaround for TS declaration generation
export interface ClientTransaction {
  readonly _tag: unique symbol;
}

export interface ClientTransactionTag<Id extends string, TSchema extends ZeroSchema>
  extends Context.ServiceClass<ClientTransaction, Id, ZeroTransaction<TSchema>> {
  readonly usePromise: <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) => Effect.Effect<A, ClientTransactionError, ClientTransaction>;
  readonly schema: TSchema;
}

export const make = <const Id extends string, TSchema extends ZeroSchema>(
  id: Id,
  schema: TSchema,
): ClientTransactionTag<Id, TSchema> => {
  class Tag extends Context.Service<ClientTransaction, ZeroTransaction<TSchema>>()(id) {}
  // Mutate-via-Mutable-cast pattern from effect-smol/src/Layer/LayerMap.ts:307-374.
  // Attaches the promise-based `usePromise` helper plus the `schema` field directly on the
  // Service-derived class. We use `usePromise` rather than `use` so we don't shadow the
  // inherited `Context.Service.use` (which has a different, Effect-callback shape).
  const Tag_ = Tag as unknown as Types.Mutable<ClientTransactionTag<Id, TSchema>>;
  Tag_.usePromise = <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const transaction = yield* Tag;
      return yield* Effect.tryPromise({
        try: (signal) => fn(transaction, { signal }),
        catch: (error) => new ClientTransactionError({ cause: Cause.fail(error) }),
      });
    });
  Tag_.schema = schema;
  return Tag_ as ClientTransactionTag<Id, TSchema>;
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}
