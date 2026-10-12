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
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Fn from "effect/Function";
import type { NodeInspectSymbol } from "effect/Inspectable";
import type * as LogLevel from "effect/LogLevel";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Scope from "effect/Scope";
import type * as Tracer from "effect/Tracer";
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
 * Sends a mutation with `zero.mutate(request)` and traces it. Awaiting `client` or `server` is optional; both
 * outcomes are always recorded on the `zero.mutation` span, as the `zero.mutation.client` and
 * `zero.mutation.server` child spans.
 */
export const mutate = Effect.fn(function* <Type>(
  // biome-ignore lint/suspicious/noExplicitAny: any Zero instance made with `make`
  zero: Zero<any, any, any>,
  request: Mutator.MutateRequest<Type>,
) {
  const mutator = request.mutator.mutatorName;
  const connection = zero.connection.state.current;
  // The span outlives this call: it ends when this scope closes, once both outcomes are known.
  const scope = yield* Scope.make();
  const span = yield* Effect.makeSpanScoped("zero.mutation", {
    attributes: {
      "zero.mutator": mutator,
      "zero.clientID": zero.clientID,
      "zero.connection.state": connection.name,
      ...Rec.getSomes({
        "zero.connection.reason": Option.fromNullishOr("reason" in connection ? connection.reason : undefined).pipe(
          Option.map((reason) => (Predicate.isString(reason) ? reason : reason.type)),
        ),
      }),
    },
  }).pipe(Scope.provide(scope));

  const result = yield* Effect.sync(() => zero.mutate(request)).pipe(
    Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause))),
  );

  const client = yield* Effect.promise(() => result.client).pipe(
    Effect.filterOrElse(isApplied, (details) => new MutationError({ mutator, origin: "client", ...details.error })),
    Effect.asVoid,
    traceOutcome({ origin: "client", parent: span, mutator }),
    Effect.forkDetach,
  );
  const server = yield* Effect.promise(() => result.server).pipe(
    Effect.filterOrElse(
      isApplied,
      Effect.fnUntraced(function* (details) {
        // A write the client rejected is never pushed; Zero reports the client's error for the server too.
        const origin = Exit.isFailure(yield* Fiber.await(client)) ? "client" : "server";
        return yield* new MutationError({ mutator, origin, ...details.error });
      }),
    ),
    Effect.asVoid,
    traceOutcome({ origin: "server", parent: span, mutator }),
    Effect.forkDetach,
  );

  yield* Effect.gen(function* () {
    span.attribute("zero.clientGroupID", yield* Effect.promise(() => zero.clientGroupID));
    const clientExit = yield* Fiber.await(client);
    const serverExit = yield* Fiber.await(server);
    yield* Scope.close(scope, Exit.isFailure(clientExit) ? clientExit : serverExit);
  }).pipe(Effect.forkDetach);

  // Joining a fiber never re-runs it, so either outcome can be awaited any number of times.
  return { client: Fiber.join(client), server: Fiber.join(server) };
});

const isApplied = (details: MutatorResultDetails): details is MutatorResultSuccessDetails => details.type === "success";

/** Runs a mutation's outcome in its `zero.mutation.client` or `zero.mutation.server` span. */
const traceOutcome: {
  (options: TraceOutcomeOptions): <E>(self: Effect.Effect<void, E>) => Effect.Effect<void, E>;
  <E>(self: Effect.Effect<void, E>, options: TraceOutcomeOptions): Effect.Effect<void, E>;
} = Fn.dual(2, <E>(self: Effect.Effect<void, E>, { origin, parent, mutator }: TraceOutcomeOptions) =>
  self.pipe(
    Effect.tapError((error) =>
      error instanceof MutationError
        ? Effect.annotateCurrentSpan({
            "zero.mutation.error.type": error.type,
            "zero.mutation.error.origin": error.origin,
          })
        : Effect.void,
    ),
    Effect.withSpan(`zero.mutation.${origin}`, { parent, attributes: { "zero.mutator": mutator } }),
  ),
);

interface TraceOutcomeOptions {
  readonly origin: "client" | "server";
  readonly parent: Tracer.Span;
  readonly mutator: string;
}

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
