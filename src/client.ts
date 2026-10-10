import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import type * as Schema from "effect/Schema";
import type * as SchemaIssue from "effect/SchemaIssue";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import { decodeArgs, inputValidationError } from "./internal/args.js";
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

  function unwrapMutator<E>(
    name: string,
    mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>, E>,
  ) {
    return (tx: ZeroTransaction<TSchema>, args: unknown, _ctx: unknown) =>
      decodeArgs(mutator[Mutators.MutatorSchemaSymbol], args).pipe(
        Effect.mapError((e) => new ClientArgsParseError({ mutator: name, issue: e.issue })),
        Effect.flatMap(mutator),
        Effect.provideService(transaction[ContextSymbol], tx),
        Effect.runPromiseWith(ctx),
      );
  }

  // Mutators are named like upstream's `defineMutators` registry: "name" or "namespace.name".
  const unwrappedMutators = Rec.map(mutators, (v, namespace) =>
    Match.value(v).pipe(
      Match.when(Predicate.isFunction, (mutator) => unwrapMutator(namespace, mutator)),
      Match.orElse(
        Rec.map((mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>>, name) =>
          unwrapMutator(`${namespace}.${name}`, mutator),
        ),
      ),
    ),
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

/**
 * Mutator args failed to decode on the client. Zero surfaces it as the same app error a `defineMutator` validator
 * failure produces: `message` is "Validation failed for mutator <name>: <issues>", and `details` carries the issues.
 */
export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly mutator: string;
  readonly issue: SchemaIssue.Issue;
}> {
  override get message() {
    return inputValidationError(this.mutator, this.issue).message;
  }
  get details() {
    return inputValidationError(this.mutator, this.issue).details;
  }
}
