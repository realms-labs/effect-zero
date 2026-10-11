import type {
  ReadonlyJSONValue,
  Schema as ZeroSchema,
  ServerTransaction as ZeroServerTransaction,
} from "@rocicorp/zero";
import { ApplicationError, handleMutateRequest, handleQueryRequest, OutOfOrderMutation } from "@rocicorp/zero/server";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type * as Unify from "effect/Unify";
import { fromTraceparent, unwrapEnvelope } from "./internal/mutation.js";
import { TransactCallbackNotInvokedError, TransactInternalError } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import * as Mutator from "./mutator.js";
import { type MakeQueryResult, QueryNameSymbol, RunQuerySymbol } from "./query.js";
import type * as ServerTransaction from "./server-transaction.js";
import { DatabaseSymbol, ServerTransactionCallbackSymbol } from "./server-transaction.js";
import type { TransformRequestMessage } from "./types/custom-queries.js";
import type { MutateParams } from "./types/mutate-server.js";
import type { PushBody } from "./types/push.js";

// Updated to:
// https://github.com/rocicorp/mono/blob/0eeabd495a26d0d67b9a5a81c424d8a76ef004b7/packages/zero-server/src/process-mutations.ts#L214

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

// The userId type, pulled from the upstream handlers. We read it off the response type rather than
// `Parameters<...>[0]` because the handlers are overloaded (so `Parameters` resolves to the wrong
// overload) and their argument type isn't exported.
type MutateUserId = Extract<Awaited<ReturnType<typeof handleMutateRequest>>, { kind: "MutateResponse" }>["userID"];
type QueryUserId = Extract<Awaited<ReturnType<typeof handleQueryRequest>>, { kind: "QueryResponse" }>["userID"];

/**
 * Handles Zero's push endpoint, running each mutation's server implementation: its override from
 * `ZfxMutator.server`, or else its shared body in a server transaction.
 */
export const handleMutate = Effect.fn(function* <
  TSchema extends ZeroSchema,
  TTransaction,
  TMutators extends Mutator.AnyRegistry | Mutator.AnyServerMutators,
>({
  transaction,
  mutators,
  query,
  body,
  userId,
}: {
  transaction: ServerTransaction.Context<TSchema, TTransaction>;
  mutators: TMutators;
  query: MutateParams;
  body: PushBody;
  userId: MutateUserId;
}) {
  const ctx =
    yield* Effect.context<
      Exclude<
        Mutator.ServerRequirements<TMutators>,
        | (typeof transaction)[typeof ServerTransactionCallbackSymbol]["Identifier"]
        | ServerSynchronizationContext.ServerSynchronizationContext
      >
    >();

  return yield* Effect.tryPromise({
    try: () =>
      handleMutateRequest({
        dbProvider: transaction[DatabaseSymbol],
        handler: (transact_, mutation) => {
          const { args, traceparent } = unwrapEnvelope(mutation.args[0]) ?? {};
          const clientSpan = fromTraceparent(traceparent);
          return Effect.gen(function* () {
            const response = yield* Deferred.make<Awaited<ReturnType<typeof transact_>>>();

            const transact = Effect.fn(function* <A, E, R>(
              fn: (transaction: ZeroServerTransaction<TSchema, TTransaction>) => Effect.Effect<A, E, R>,
            ) {
              const innerCtx = yield* Effect.context<R>();
              const exitDeferred = yield* Deferred.make<A, E>();

              yield* Effect.tryPromise({
                try: (signal) =>
                  transact_((transaction) =>
                    fn(transaction).pipe(
                      Effect.onExit((exit) => Deferred.done(exitDeferred, exit)),
                      Effect.catchCause((cause) => Effect.die(makeApplicationError(cause))),
                      Effect.asVoid,
                      (effect) => Effect.runPromiseWith(innerCtx)(effect, { signal }),
                    ),
                  ),
                catch: (error) => new TransactInternalError({ cause: Cause.fail(error) }),
              }).pipe(Effect.flatMap((value) => Deferred.succeed(response, value)));

              return yield* Deferred.poll(exitDeferred).pipe(
                Effect.flatMap(Effect.fromOption(() => new TransactCallbackNotInvokedError())),
                Effect.flatten,
              );
            }, ServerSynchronizationContext.guard);

            const exit: Exit.Exit<unknown, unknown> = yield* runMutation(
              mutators,
              transaction,
              mutation.name,
              args,
            ).pipe(
              ServerSynchronizationContext.finalize,
              Effect.provide([
                Layer.succeed(transaction[ServerTransactionCallbackSymbol], { transact }),
                ServerSynchronizationContext.layer,
              ]),
              Effect.exit,
            );

            return yield* Deferred.poll(response).pipe(
              Effect.flatMap(Effect.fromOption),
              Effect.catchTag("NoSuchElementError", () =>
                Effect.failCause(Exit.isFailure(exit) ? exit.cause : Cause.empty),
              ),
              Effect.flatten,
            );
          }).pipe(
            Effect.withSpan("zero.mutation.handle", {
              attributes: {
                "zero.mutator": mutation.name,
                "zero.clientID": mutation.clientID,
                "zero.mutationID": mutation.id,
              },
              // A link, not the parent: the push request has its own trace, and a write can be pushed more than once.
              links: clientSpan === undefined ? [] : [{ span: clientSpan, attributes: {} }],
            }),
            Effect.catchCause((cause) => {
              const error = cause.pipe(Cause.squash, (squashed) =>
                TransactInternalError.is(squashed) ? Cause.squash(squashed.cause) : squashed,
              );
              // Split out-of-order mutations from application errors, as `handleMutateRequest` expects.
              return Effect.fail(error instanceof OutOfOrderMutation ? error : makeApplicationError(Cause.fail(error)));
            }),
            Effect.provide(ctx),
            Effect.runPromise,
          );
        },
        query,
        body: body as ReadonlyJSONValue,
        userID: userId,
      }),
    catch: (e) => new HandleMutateError({ cause: Cause.fail(e) }),
  });
});

const runMutation = (
  mutators: Mutator.AnyRegistry | Mutator.AnyServerMutators,
  // biome-ignore lint/suspicious/noExplicitAny: any server transaction
  transaction: ServerTransaction.Context<any, any>,
  name: string,
  encodedArgs: unknown,
  // `handleMutate` requires the services of whichever implementation runs.
): Effect.Effect<unknown, unknown> =>
  Effect.gen(function* () {
    const { mutator, override } = yield* Effect.fromNullishOr(lookup(mutators, name)).pipe(
      Effect.mapError(() => new MutatorNotFoundError({ name })),
    );
    const args = yield* Schema.decodeUnknownEffect(mutator.payload)(encodedArgs).pipe(
      Effect.catchTag("SchemaError", (e) => Effect.fail(new ServerArgsParseError({ cause: Cause.fail(e) }))),
    );
    return yield* override === undefined ? transaction.execute(mutator.mutator(args)) : override(args);
  }) as Effect.Effect<unknown, unknown>;

/** Finds a mutator by its dotted name, with its server override if it has one. */
const lookup = (mutators: Mutator.AnyRegistry | Mutator.AnyServerMutators, name: string) => {
  const [registry, overrides] =
    Mutator.ServerMutatorsTypeId in mutators ? [mutators.registry, mutators.overrides] : [mutators, undefined];
  const at = (root: unknown) =>
    name.split(".").reduce<unknown>((node, key) => (Predicate.hasProperty(node, key) ? node[key] : undefined), root);
  const mutator = at(registry);
  const override = at(overrides);
  return Predicate.isFunction(mutator) && Predicate.hasProperty(mutator, "mutatorName")
    ? {
        mutator: mutator as Mutator.AnyMutator,
        // biome-ignore lint/suspicious/noExplicitAny: any server implementation
        override: Predicate.isFunction(override) ? (override as (args: any) => Effect.Effect<unknown>) : undefined,
      }
    : undefined;
};

export const makeApplicationError = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  const message = Predicate.isError(error) ? error.message : "Internal error";
  return new ApplicationError(message, { cause: error });
};

class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  readonly name: string;
}> {
  override get message() {
    return `Mutator not found: ${this.name}`;
  }
}

class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class HandleMutateError extends Data.TaggedError("HandleMutateError")<{ readonly cause: Cause.Cause<unknown> }> {}

export const handleQuery = Effect.fn(function* <E, R1, R2>({
  queries,
  schema,
  body,
  userId,
}: {
  queries: MakeQueryResult<E, R1, R2>[];
  schema: ZeroSchema;
  body: TransformRequestMessage;
  userId: QueryUserId;
}) {
  const ctx = yield* Effect.context<R1 | R2>();
  return yield* Effect.tryPromise({
    try: () =>
      handleQueryRequest({
        handler: (name, args) =>
          Arr.findFirst(queries, (q) => q[QueryNameSymbol] === name).pipe(
            Effect.fromOption(() => new QueryNotFound({ name })),
            Effect.flatMap((query) => query[RunQuerySymbol]({ _tag: "Encoded", args })),
            Effect.runSyncWith(ctx),
          ),
        schema,
        // `query` params are unused by the query endpoint at runtime but required by the options type.
        query: {},
        body: body as ReadonlyJSONValue,
        userID: userId,
      }),
    catch: (e) => new HandleQueryError({ cause: Cause.fail(e) }),
  });
});

class QueryNotFound extends Data.TaggedError("QueryNotFound")<{ name: string }> {
  override get message() {
    return `Query not found: ${this.name}`;
  }
}

class HandleQueryError extends Data.TaggedError("HandleQueryError")<{ cause: Cause.Cause<unknown> }> {}
