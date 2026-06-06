import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import type * as Unify from "effect/Unify";

type _ = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction (its Id literal is irrelevant here)
export type Context<TSchema extends ZeroSchema> = ReturnType<typeof make<any, TSchema>>;

export const make = <const Id extends string, TSchema extends ZeroSchema>(id: Id, schema: TSchema) => {
  class Context extends Ctx.Service<Context, ZeroTransaction<TSchema>>()(id) {}

  const use = <A>(
    fn: (transaction: ZeroTransaction<TSchema>, options: { readonly signal: AbortSignal }) => PromiseLike<A>,
  ) =>
    Effect.gen(function* () {
      const transaction = yield* Context;
      return yield* Effect.tryPromise({
        try: (signal) => fn(transaction, { signal }),
        catch: (error) => new ClientTransactionError({ cause: Cause.fail(error) }),
      });
    });

  return { Context, use, schema };
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}
