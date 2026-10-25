import {
  type MutatorResultDetails,
  type MutatorResultErrorDetails,
  type MutatorResultSuccessDetails,
  type ReadonlyJSONValue,
  Zero,
  type ZeroOptions,
} from "@rocicorp/zero";
import * as Data from "effect/Data";
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
 * Run mutations with {@link mutate}.
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

/** A mutation sent with {@link mutate}. Its client and server outcomes settle independently. */
export interface Mutation {
  /** Completes once the mutation is applied locally; fails if Zero or the client mutator rejected it. */
  readonly client: Effect.Effect<void, MutationError>;
  /** Completes once the server applied the mutation; fails if it was rejected anywhere. */
  readonly server: Effect.Effect<void, MutationError>;
}

/**
 * Sends a mutation with `zero.mutate(request)`. Its `client` and `server` outcomes can be awaited in any order, any
 * number of times, or not at all: they read Zero's result, and never send the mutation again.
 */
export const mutate = Effect.fn(function* <Type>(
  // biome-ignore lint/suspicious/noExplicitAny: any Zero instance made with `make`
  zero: Zero<any, any, any>,
  request: Mutator.MutateRequest<Type>,
) {
  const mutator = request.mutator.mutatorName;
  const result = yield* Effect.sync(() => zero.mutate(request));

  const client = Effect.promise(() => result.client).pipe(
    Effect.filterOrElse(isApplied, (details) => new MutationError({ mutator, origin: "client", ...details.error })),
    Effect.asVoid,
  );
  const server = Effect.promise(() => result.server).pipe(
    Effect.filterOrElse(
      isApplied,
      Effect.fnUntraced(function* (details) {
        // A write the client rejected is never pushed; Zero reports the client's error for the server too.
        const origin = isApplied(yield* Effect.promise(() => result.client)) ? "server" : "client";
        return yield* new MutationError({ mutator, origin, ...details.error });
      }),
    ),
    Effect.asVoid,
  );

  return { client, server };
});

const isApplied = (details: MutatorResultDetails): details is MutatorResultSuccessDetails => details.type === "success";

/**
 * Zero rejected a mutation, with Zero's own error details: `type` is `"zero"` when Zero itself refused it
 * (e.g. offline), and `"app"` when a mutator failed. `origin` is where it failed.
 */
export class MutationError extends Data.TaggedError("MutationError")<{
  readonly mutator: string;
  readonly origin: "client" | "server";
  readonly type: MutatorResultErrorDetails["error"]["type"];
  readonly message: string;
  readonly details?: ReadonlyJSONValue | undefined;
}> {}
