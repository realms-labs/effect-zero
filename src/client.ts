import { type MutatorResultDetails, type ReadonlyJSONValue, Zero, type ZeroOptions } from "@rocicorp/zero";
import * as Clock from "effect/Clock";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import type * as Tracer from "effect/Tracer";
import type * as Unify from "effect/Unify";
import { type ContextSymbol, SchemaSymbol } from "./client-transaction.js";
import {
  type ClientState,
  ClientStateSymbol,
  makeWriteId,
  type PendingWrite,
  toTraceparent,
  unwrapEnvelope,
  type ZeroContext,
} from "./internal/mutation.js";
import * as Mutator from "./mutator.js";

export { ClientArgsParseError } from "./mutator.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

type ZeroLogLevel = "debug" | "info" | "warn" | "error";

export type Options<TRegistry extends Mutator.AnyRegistry> = Omit<
  ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>>,
  "schema" | "mutators" | "context"
> & {
  /** Sends Zero's own logs through Effect's logger, annotated with `logger: "zero"`, from `level` up. */
  readonly logs?: { readonly level?: ZeroLogLevel | undefined } | undefined;
};

const states = new WeakMap<object, ClientState>();

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
  const state: ClientState = { context: context as ClientState["context"], pending: new Map() };
  const { logs, ...zeroOptions } = options;
  const runLog = Effect.runForkWith(context);

  return yield* Effect.acquireRelease(
    Effect.sync(() => {
      const zero = new Zero<Mutator.ZeroSchemaOf<TRegistry>, undefined, ZeroContext>({
        ...(logs === undefined
          ? {}
          : {
              logLevel: logs.level ?? "warn",
              logSink: {
                log: (level: ZeroLogLevel, logContext: Record<string, unknown> | undefined, ...args: unknown[]) =>
                  void runLog(
                    Effect.logWithLevel(logLevels[level])(...args).pipe(
                      Effect.annotateLogs({ ...logContext, logger: "zero" }),
                    ),
                  ),
              },
            }),
        ...zeroOptions,
        schema: transaction[SchemaSymbol],
        // A Zero mutator registry: its mutators are the very objects `ZfxClient.mutate` sends.
        mutators: mutators as never,
        context: { [ClientStateSymbol]: state },
      } as ZeroOptions<Mutator.ZeroSchemaOf<TRegistry>, undefined, ZeroContext>);
      states.set(zero, state);
      // The context only carries effect-zero's own state, so it isn't part of the client's type.
      return zero as unknown as Zero<Mutator.ZeroSchemaOf<TRegistry>>;
    }),
    (zero) => Effect.promise(() => zero.close()),
  );
});

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
    const state = states.get(zero);
    if (state === undefined) {
      return yield* Effect.die(new Error("ZfxClient.mutate requires a Zero instance made with ZfxClient.make"));
    }

    const name = request.mutator.mutatorName;
    const id = makeWriteId();
    const connection = zero.connection.state.current;
    const span = yield* Effect.makeSpan("zero.mutation", {
      attributes: {
        "zero.mutator": name,
        "zero.mutation.id": id,
        "zero.clientID": zero.clientID,
        "zero.connection.state": connection.name,
        ...("reason" in connection && typeof connection.reason === "string"
          ? { "zero.connection.reason": connection.reason }
          : {}),
      },
    });

    const pending: PendingWrite = { span, clientFailure: undefined };
    state.pending.set(id, pending);

    const result = yield* Effect.sync(() =>
      zero.mutate({
        ...request,
        args: { ...unwrapEnvelope(request.args), meta: { id, traceparent: toTraceparent(span) } },
      }),
    ).pipe(Effect.onError((cause) => endSpan(span, Exit.failCause(cause)).pipe(Effect.andThen(forget(state, id)))));

    const client = yield* Deferred.make<void, MutationError | Mutator.ClientArgsParseError | E>();
    const server = yield* Deferred.make<void, MutationError>();

    const settle = (origin: "client" | "server", details: Promise<MutatorResultDetails>) =>
      Effect.promise(() => details).pipe(
        Effect.flatMap((details): Effect.Effect<void, unknown> => {
          if (details.type === "success") return Effect.void;
          // The client mutator's own failure. Zero reports it for the server too: the write was never pushed.
          if (pending.clientFailure !== undefined) {
            return origin === "client"
              ? Effect.failCause(pending.clientFailure)
              : Effect.fail(toMutationError(name, "client", details.error));
          }
          return Effect.fail(toMutationError(name, origin, details.error));
        }),
        Effect.tapError((error) =>
          error instanceof MutationError
            ? Effect.annotateCurrentSpan({
                "zero.mutation.error.kind": error.kind,
                "zero.mutation.error.origin": error.origin,
              })
            : Effect.void,
        ),
        Effect.withSpan(`zero.mutation.${origin}`, { parent: span, attributes: { "zero.mutator": name } }),
        Effect.exit,
      );

    yield* Effect.gen(function* () {
      span.attribute("zero.clientGroupID", yield* Effect.promise(() => zero.clientGroupID));
      const [clientExit, serverExit] = yield* Effect.all(
        [
          settle("client", result.client).pipe(
            Effect.tap((exit) => Deferred.done(client, exit as Exit.Exit<void, MutationError | E>)),
          ),
          settle("server", result.server).pipe(
            Effect.tap((exit) => Deferred.done(server, exit as Exit.Exit<void, MutationError>)),
          ),
        ],
        { concurrency: 2 },
      );
      yield* endSpan(span, Exit.isFailure(clientExit) ? clientExit : serverExit);
    }).pipe(Effect.ensuring(forget(state, id)), Effect.forkDetach);

    return { client: Deferred.await(client), server: Deferred.await(server) };
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

const endSpan = (span: Tracer.Span, exit: Exit.Exit<unknown, unknown>) =>
  Effect.flatMap(Clock.currentTimeNanos, (now) => Effect.sync(() => span.end(now, exit)));

const forget = (state: ClientState, id: string) => Effect.sync(() => state.pending.delete(id));
