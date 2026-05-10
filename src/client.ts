import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
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
  const ctx =
    yield* Effect.context<
      Exclude<Mutators.ExtractMutatorsRequirements<TMutators>, ClientTransaction.ClientTransaction>
    >();

  function unwrapMutator<E>(mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>, E>) {
    return async (tx: ZeroTransaction<TSchema>, args: unknown, _ctx: unknown) => {
      // Normalize JSON's `null` to `undefined` for `Schema.Void` decode.
      //
      // Zero's mutator-call dispatch unconditionally coerces the args slot via `args ?? null`
      // when freezing pending mutations -- so an arg-less call (`mutator()`, where `args`
      // is `undefined`) is stored, persisted, and replayed as `null`:
      // https://github.com/rocicorp/mono/blob/05ab7f78047b5bb1cdfc0797bea8cf2537685c93/packages/replicache/src/replicache-impl.ts#L1521
      //
      // Effect v3's `Schema.Void` parser accepted any input unconditionally (its
      // `VoidKeyword` case returned `Either.right(input)` like `Unknown`/`Any`), so
      // `null` flowed through. v4's `Schema.Void` is strict: its AST uses
      // `fromConst(this, undefined)` and rejects anything that isn't `=== undefined`
      // with `Expected void, got null`.
      //
      // To preserve the v3 client behavior (arg-less mutators succeed) without
      // weakening every mutator's payload schema, normalize the wire `null` back to
      // `undefined` here before decoding.
      const input = args === null ? undefined : args;
      const exit = await Schema.decodeUnknownEffect(mutator[Mutators.MutatorSchemaSymbol])(input).pipe(
        Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ cause: Cause.fail(e) }))),
        Effect.flatMap(mutator),
        Effect.provideService(transaction, tx),
        Effect.runPromiseExitWith(ctx),
      );
      if (Exit.isFailure(exit)) {
        // Extract underlying error bypassing FiberFailure
        throw Cause.squash(exit.cause);
      }
      return exit.value;
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

export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly cause: Cause.Cause<Schema.SchemaError>;
}> {}
