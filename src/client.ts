import { Zero, type ZeroOptions, type Schema as ZeroSchema, type Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import type * as LogLevel from "effect/LogLevel";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import type * as Unify from "effect/Unify";
import { ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import * as Mutator from "./mutator.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export type Options<TRegistry extends Mutator.AnyRegistry> = Omit<
  ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>, UnwrapMutators<Mutator.ZeroSchemaOf<TRegistry>, TRegistry>>,
  "schema" | "mutators"
>;

/**
 * Makes a Zero client that runs the registry's mutators with the services in scope here.
 * Run mutations with `zero.mutate.todo.create(args)`, passing the encoded args.
 */
export const make = Effect.fn(function* <TRegistry extends Mutator.AnyRegistry>(
  mutators: TRegistry,
  options: Options<TRegistry>,
) {
  const transaction = mutators[Mutator.RegistryTypeId].transaction;
  const context =
    yield* Effect.context<
      Exclude<Mutator.Requirements<TRegistry>, Mutator.Transaction<TRegistry>[typeof ContextSymbol]["Identifier"]>
    >();

  const unwrapMutator =
    (mutator: Mutator.AnyDefinition) => (tx: ZeroTransaction<Mutator.ZeroSchemaOf<TRegistry>>, args: unknown) =>
      Schema.decodeUnknownEffect(mutator.payload)(args).pipe(
        Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ cause: Cause.fail(e) }))),
        Effect.flatMap(mutator.mutator),
        Effect.provideService(transaction[ContextSymbol], tx),
        Effect.runPromiseWith(context),
      );

  // Zero's custom mutators: a plain object of functions, at the root or nested under namespaces.
  const unwrapMutators = (mutators: Record<string, unknown>): Record<string, unknown> =>
    Rec.map(mutators, (value) =>
      isDefinition(value) ? unwrapMutator(value) : unwrapMutators(value as Record<string, unknown>),
    );

  return yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new Zero({
          // Zero's own logs (at its `logLevel`, "warn" by default) go through Effect's logger, unless `options` sets
          // a `logSink`.
          logSink: {
            log: (level, logContext, ...args) =>
              Match.value(level).pipe(
                Match.withReturnType<LogLevel.Severity>(),
                Match.when("debug", () => "Debug"),
                Match.when("info", () => "Info"),
                Match.when("warn", () => "Warn"),
                Match.when("error", () => "Error"),
                Match.exhaustive,
                (severity) => Effect.logWithLevel(severity)(...args),
                Effect.annotateLogs({ ...logContext, logger: "zero" }),
                Effect.runForkWith(context),
              ),
          },
          ...options,
          schema: transaction[SchemaSymbol],
          mutators: unwrapMutators(mutators) as ZeroOptions<
            Mutator.ZeroSchemaOf<TRegistry>,
            UnwrapMutators<Mutator.ZeroSchemaOf<TRegistry>, TRegistry>
          >["mutators"],
        }),
    ),
    (zero) => Effect.promise(() => zero.close()),
  );
});

const isDefinition = (u: unknown): u is Mutator.AnyDefinition =>
  Predicate.hasProperty(u, "payload") && Schema.isSchema(u.payload);

type UnwrapMutators<TSchema extends ZeroSchema, T> = {
  readonly [K in Extract<keyof T, string>]: T[K] extends Mutator.Definition<
    infer _Type,
    infer Encoded,
    infer _E,
    infer _R
  >
    ? (transaction: ZeroTransaction<TSchema>, args: Encoded) => Promise<void>
    : UnwrapMutators<TSchema, T[K]>;
};

export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) && !Schema.isSchemaError(error) ? error.message : "Internal error";
  }
}
