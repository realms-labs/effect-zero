import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import { normalizeArgs } from "./internal/utils.js";
import * as Mutators from "./mutators.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const make = Effect.fn(function* <TSchema extends ZeroSchema, TMutators extends Mutators.AnyMutators>(
  transaction: ClientTransaction.Context<TSchema>,
  mutators: TMutators,
  options: Omit<ZeroOptions<TSchema, UnwrapMutators<TSchema, TMutators>>, "schema" | "mutators">,
) {
  const ctx =
    yield* Effect.context<
      Exclude<Mutators.ExtractMutatorsRequirements<TMutators>, (typeof transaction)[typeof ContextSymbol]["Identifier"]>
    >();

  function unwrapMutator<E>(mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>, E>) {
    return (tx: ZeroTransaction<TSchema>, args: unknown, _ctx: unknown) =>
      Schema.decodeUnknownEffect(mutator[Mutators.MutatorSchemaSymbol])(normalizeArgs(args)).pipe(
        Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ message: e.message, cause: e }))),
        Effect.flatMap(mutator),
        Effect.provideService(transaction[ContextSymbol], tx),
        Effect.runPromiseWith(ctx),
      );
  }

  const unwrappedMutators = Rec.map(mutators, (v) =>
    Match.value(v).pipe(Match.when(Predicate.isFunction, unwrapMutator), Match.orElse(Rec.map(unwrapMutator))),
  ) as ZeroOptions<TSchema, UnwrapMutators<TSchema, TMutators>>["mutators"];

  return yield* Effect.acquireRelease(
    Effect.sync(() => {
      return new Zero({
        ...options,
        schema: transaction[SchemaSymbol],
        mutators: unwrappedMutators,
      });
    }),
    (zero) => Effect.promise(() => zero.close()),
  );
});

type UnwrapMutator<TSchema extends ZeroSchema, TMutators extends Mutators.AnyMutator> = Parameters<TMutators> extends []
  ? (transaction: ZeroTransaction<TSchema>) => Promise<void>
  : (
      transaction: ZeroTransaction<TSchema>,
      args: Schema.Codec.Encoded<TMutators[typeof Mutators.MutatorSchemaSymbol]>,
    ) => Promise<void>;

type UnwrapMutators<TSchema extends ZeroSchema, TMutators extends Mutators.AnyMutators> = {
  [A in keyof TMutators]: TMutators[A] extends Mutators.AnyMutator
    ? UnwrapMutator<TSchema, TMutators[A]>
    : {
        [B in keyof TMutators[A]]: TMutators[A][B] extends Mutators.AnyMutator
          ? UnwrapMutator<TSchema, TMutators[A][B]>
          : never;
      };
} & {};

// Idiomatic v4: pass the underlying error's text as the native `message` and the raw error as the native
// `cause`. `Data.TaggedError` forwards both to the JS `Error`, so `.message` is clean (no serialized
// `Cause` blob) and `.cause` chains — no hand-rolled getter, no `Cause` wrapping.
export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly message: string;
  readonly cause: Schema.SchemaError;
}> {}
