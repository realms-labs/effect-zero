import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Match from "effect/Match";
import type * as ParseResult from "effect/ParseResult";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import type * as ClientTransaction from "./client-transaction.js";
import * as Mutators from "./mutators.js";

type ClientTransactionContext<TSchema extends ZeroSchema> = Omit<
  ReturnType<typeof ClientTransaction.make<string, TSchema>>,
  "use"
>;

export const make = Effect.fn(function* <TSchema extends ZeroSchema, TMutators extends Mutators.AnyMutators>(
  transaction: ClientTransactionContext<TSchema>,
  mutators: TMutators,
  options: Omit<ZeroOptions<TSchema, UnwrapMutators<TSchema, TMutators>>, "schema" | "mutators">,
) {
  const runtime =
    yield* Effect.runtime<
      Exclude<Mutators.ExtractMutatorsRequirements<TMutators>, ClientTransaction.ClientTransaction>
    >();

  function unwrapMutator<E>(mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>, E>) {
    return async (tx: ZeroTransaction<TSchema>, args: unknown, _ctx: unknown) => {
      const exit = await Schema.decode(mutator[Mutators.MutatorSchemaSymbol])(args).pipe(
        Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ cause: Cause.fail(e) }))),
        Effect.flatMap(mutator),
        Effect.provideService(transaction, tx),
        Runtime.runPromiseExit(runtime),
      );
      return Exit.getOrElse(exit, (c) => {
        // Extract underlying error bypassing FiberFailure
        throw Cause.squash(c);
      });
    };
  }

  const unwrappedMutators = Rec.map(mutators, (v) =>
    Match.value(v).pipe(Match.when(Predicate.isFunction, unwrapMutator), Match.orElse(Rec.map(unwrapMutator))),
  ) as ZeroOptions<TSchema, UnwrapMutators<TSchema, TMutators>>["mutators"];

  return yield* Effect.acquireRelease(
    Effect.sync(() => {
      return new Zero({
        ...options,
        schema: transaction.schema,
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
      args: Schema.Schema.Encoded<TMutators[typeof Mutators.MutatorSchemaSymbol]>,
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

export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly cause: Cause.Cause<ParseResult.ParseError>;
}> {}
