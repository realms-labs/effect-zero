import { Zero, type ZeroOptions } from "@rocicorp/zero";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import type * as LogLevel from "effect/LogLevel";
import * as Match from "effect/Match";
import type * as Unify from "effect/Unify";
import { type ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import * as Mutator from "./mutator.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export type Options<TRegistry extends Mutator.AnyRegistry> = Omit<
  ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>>,
  "schema" | "mutators" | "context"
>;

/**
 * Makes a Zero client that runs the registry's mutators with the services in scope here.
 * Run mutations with `zero.mutate(mutators.todo.create(args))`.
 */
export const make = Effect.fn(function* <TRegistry extends Mutator.AnyRegistry>(
  mutators: TRegistry,
  options: Options<TRegistry>,
) {
  const context =
    yield* Effect.context<
      Exclude<Mutator.Requirements<TRegistry>, Mutator.Transaction<TRegistry>[typeof ContextSymbol]["Identifier"]>
    >();

  return yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new Zero<Mutator.ZeroSchemaOf<TRegistry>>({
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
          schema: mutators[Mutator.RegistryTypeId].transaction[SchemaSymbol],
          mutators,
          // The mutators run with these services.
          context,
        }),
    ),
    (zero) => Effect.promise(() => zero.close()),
  );
});
