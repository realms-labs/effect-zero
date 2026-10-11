import { type MutatorResultDetails, type ReadonlyJSONValue, Zero, type ZeroOptions } from "@rocicorp/zero";
import type * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Scope from "effect/Scope";
import type * as Unify from "effect/Unify";
import { type ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import { type PendingWrite, pendingWrites, toTraceparent, unwrapEnvelope } from "./internal/mutation.js";
import * as Mutator from "./mutator.js";

export { ClientArgsParseError } from "./mutator.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export type Options<TRegistry extends Mutator.AnyRegistry> = Omit<
  ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>>,
  "schema" | "mutators" | "context"
> & {
  /**
   * Sends Zero's own logs (from its `logLevel`, `"warn"` by default) through Effect's logger, annotated with
   * `logger: "zero"`.
   */
  readonly logs?: boolean | undefined;
};

/**
 * Makes a Zero client that runs the registry's mutators with the services in scope here.
 * Run mutations with {@link mutate}.
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
  const { logs, ...zeroOptions } = options;
  const runLog = Effect.runForkWith(context);

  return yield* Effect.acquireRelease(
    Effect.sync(
      () =>
        new Zero({
          ...(logs
            ? {
                logSink: {
                  log: (level: ZeroLogLevel, logContext: Record<string, unknown> | undefined, ...args: unknown[]) =>
                    void runLog(
                      Effect.logWithLevel(logLevels[level])(...args).pipe(
                        Effect.annotateLogs({ ...logContext, logger: "zero" }),
                      ),
                    ),
                },
              }
            : {}),
          ...zeroOptions,
          schema: transaction[SchemaSymbol],
          // A Zero mutator registry: its mutators are the very objects `ZfxClient.mutate` sends.
          mutators: mutators as never,
          // The mutators run with these services.
          context: context as never,
        } as ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>>),
    ),
    (zero) => Effect.promise(() => zero.close()),
  );
});

type ZeroLogLevel = "debug" | "info" | "warn" | "error";

const logLevels = { debug: "Debug", info: "Info", warn: "Warn", error: "Error" } as const;

/** A mutation sent with {@link mutate}. Its client and server outcomes settle independently. */
export interface Mutation<E> {
  /** Completes once the mutation is applied locally; fails if Zero or the client mutator rejected it. */
  readonly client: Effect.Effect<void, MutationError | Mutator.ClientArgsParseError | E>;
  /** Completes once the server applied the mutation; fails if it was rejected anywhere. */
  readonly server: Effect.Effect<void, MutationError>;
}

/**
 * Zero rejected a mutation. `kind` is `"zero"` when Zero itself refused it (e.g. offline), and `"app"` when a mutator
 * failed. `origin` is where it failed.
 */
export class MutationError extends Data.TaggedError("MutationError")<{
  readonly mutator: string;
  readonly origin: "client" | "server";
  readonly kind: "zero" | "app";
  readonly message: string;
  readonly details?: ReadonlyJSONValue | undefined;
}> {}

/**
 * Sends a mutation, like `zero.mutate(request)`, and traces it. Awaiting `client` or `server` is optional; both
 * outcomes are always recorded on the `zero.mutation` span, as the `zero.mutation.client` and
 * `zero.mutation.server` child spans.
 */
export const mutate = <Type, E>(
  // biome-ignore lint/suspicious/noExplicitAny: any Zero instance made with `make`
  zero: Zero<any, any, any>,
  request: Mutator.MutateRequest<Type, E>,
): Effect.Effect<Mutation<E>> =>
  Effect.gen(function* () {
    const name = request.mutator.mutatorName;
    const connection = zero.connection.state.current;
    // The span outlives this call, so it ends when this scope closes, once both outcomes are known.
    const scope = yield* Scope.make();
    const span = yield* Effect.makeSpanScoped("zero.mutation", {
      attributes: {
        "zero.mutator": name,
        "zero.clientID": zero.clientID,
        "zero.connection.state": connection.name,
        ...("reason" in connection && typeof connection.reason === "string"
          ? { "zero.connection.reason": connection.reason }
          : {}),
      },
    }).pipe(Scope.provide(scope));

    const pending: PendingWrite = { span, clientFailure: undefined };
    const envelope = { ...unwrapEnvelope(request.args), traceparent: toTraceparent(span) };
    pendingWrites.set(envelope, pending);
    const result = yield* Effect.sync(() => zero.mutate({ ...request, args: envelope })).pipe(
      Effect.onError((cause) => Scope.close(scope, Exit.failCause(cause))),
    );

    const traced = <E2>(origin: "client" | "server", outcome: Effect.Effect<void, E2>) =>
      outcome.pipe(
        Effect.tapError((error) =>
          error instanceof MutationError
            ? Effect.annotateCurrentSpan({
                "zero.mutation.error.kind": error.kind,
                "zero.mutation.error.origin": error.origin,
              })
            : Effect.void,
        ),
        Effect.withSpan(`zero.mutation.${origin}`, { parent: span, attributes: { "zero.mutator": name } }),
        // Forked so both outcomes are recorded even if nobody awaits them; joining a fiber never re-runs it.
        Effect.forkDetach,
      );

    const client = yield* traced(
      "client",
      Effect.flatMap(
        Effect.promise(() => result.client),
        (details): Effect.Effect<void, MutationError | Mutator.ClientArgsParseError | E> =>
          details.type === "success"
            ? Effect.void
            : pending.clientFailure !== undefined
              ? // The client mutator's own (typed) failure.
                Effect.failCause(pending.clientFailure as Cause.Cause<Mutator.ClientArgsParseError | E>)
              : Effect.fail(toMutationError(name, "client", details.error)),
      ),
    );
    const server = yield* traced(
      "server",
      Effect.flatMap(
        Effect.promise(() => result.server),
        (details) =>
          details.type === "success"
            ? Effect.void
            : // A client failure is reported for the server too: the write was never pushed.
              Effect.fail(
                toMutationError(name, pending.clientFailure !== undefined ? "client" : "server", details.error),
              ),
      ),
    );

    yield* Effect.forkDetach(
      Effect.gen(function* () {
        span.attribute("zero.clientGroupID", yield* Effect.promise(() => zero.clientGroupID));
        const clientExit = yield* Effect.exit(Fiber.join(client));
        const serverExit = yield* Effect.exit(Fiber.join(server));
        yield* Scope.close(scope, Exit.isFailure(clientExit) ? clientExit : serverExit);
      }),
    );

    return { client: Fiber.join(client), server: Fiber.join(server) };
  });

const toMutationError = (
  mutator: string,
  origin: "client" | "server",
  error: Extract<MutatorResultDetails, { type: "error" }>["error"],
) =>
  new MutationError({
    mutator,
    origin,
    kind: error.type,
    message: error.message,
    details: error.type === "app" ? error.details : undefined,
  });
