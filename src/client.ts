import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import type * as ClientTransaction from "./client-transaction.js";
import * as Mutators from "./mutators.js";

type _ = NodeInspectSymbol;

export const make = Effect.fn(function* <TSchema extends ZeroSchema, TMutators extends Mutators.AnyMutators>(
  transaction: ClientTransaction.Context<TSchema>,
  mutators: TMutators,
  options: Omit<ZeroOptions<TSchema, UnwrapMutators<TSchema, TMutators>>, "schema" | "mutators">,
) {
  const ctx =
    yield* Effect.context<
      Exclude<Mutators.ExtractMutatorsRequirements<TMutators>, (typeof transaction.Context)["Identifier"]>
    >();

  function unwrapMutator<E>(mutator: Mutators.AnyMutator<Mutators.ExtractMutatorsRequirements<TMutators>, E>) {
    return async (tx: ZeroTransaction<TSchema>, args: unknown, _ctx: unknown) => {
      // Zero unconditionally serializes arg-less mutator calls as `null` on the wire
      // (https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-client/src/client/custom.ts).
      // v3 `Schema.Void` happened to accept any input; v4 `Schema.Void` is strict
      // `=== undefined`, so we renormalize before decoding.
      const input = args === null ? undefined : args;
      const exit = await Schema.decodeUnknownEffect(mutator[Mutators.MutatorSchemaSymbol])(input).pipe(
        Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ cause: Cause.fail(e) }))),
        Effect.flatMap(mutator),
        Effect.provideService(transaction.Context, tx),
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
