import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Ctx from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Predicate from "effect/Predicate";
import type * as Unify from "effect/Unify";
import { prefixId } from "./internal/utils.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const ContextSymbol = Symbol.for(prefixId("Context"));
export const SchemaSymbol = Symbol.for(prefixId("Schema"));

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

  return { use, [ContextSymbol]: Context, [SchemaSymbol]: schema };
};

class ClientTransactionError extends Data.TaggedError("ClientTransactionError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const err = Cause.squash(this.cause);
    return Predicate.isError(err) ? err.message : "exception was not of type `Error`";
  }
}
