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
import * as Fn from "effect/Function";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Str from "effect/String";
import type * as Unify from "effect/Unify";
import { TransactCallbackNotInvokedError, TransactInternalError } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { normalizeArgs } from "./internal/utils.js";
import * as Mutators from "./mutators.js";
import { type MakeQueryResult, QueryNameSymbol, RunQuerySymbol } from "./query.js";
import type * as ServerTransaction from "./server-transaction.js";
import { DatabaseSymbol, ServerTransactionCallbackSymbol } from "./server-transaction.js";
import type * as Types from "./types/push.js";
import type { TransformRequestMessage } from "./types/queries.js";

// Updated to:
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/process-mutations.ts

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const handleMutate = Effect.fn(function* <
  TSchema extends ZeroSchema,
  TTransaction,
  TMutators extends Mutators.AnyMutators,
>(
  transaction: ServerTransaction.Context<TSchema, TTransaction>,
  mutators: TMutators,
  params: Types.PushParams,
  request: Types.PushBody,
  // The authenticated user ID (or `null`/`undefined` for logged-out clients). Since Zero 1.5 this is
  // echoed back so zero-cache can enforce that only tabs belonging to the same user share a client group.
  userID: string | null | undefined,
) {
  const ctx =
    yield* Effect.context<
      Exclude<
        Mutators.ExtractMutatorsRequirements<TMutators>,
        | (typeof transaction)[typeof ServerTransactionCallbackSymbol]["Identifier"]
        | ServerSynchronizationContext.ServerSynchronizationContext
      >
    >();

  return yield* Effect.tryPromise({
    try: () =>
      handleMutateRequest({
        dbProvider: transaction[DatabaseSymbol],
        handler: (transact_, mutation) =>
          Effect.gen(function* () {
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
                      Effect.provide(innerCtx),
                      (effect) => Effect.runPromise(effect, { signal }),
                    ),
                  ),
                catch: (error) => new TransactInternalError({ cause: Cause.fail(error) }),
              }).pipe(Effect.flatMap((value) => Deferred.succeed(response, value)));

              return yield* Deferred.poll(exitDeferred).pipe(
                Effect.flatMap(Effect.fromOption),
                Effect.catchTag("NoSuchElementError", () => Effect.fail(new TransactCallbackNotInvokedError())),
                Effect.flatten,
              );
            }, ServerSynchronizationContext.guard);

            const exit: Exit.Exit<unknown, unknown> = yield* runMutation<
              Mutators.ExtractMutatorsRequirements<TMutators>
            >(mutators, mutation).pipe(
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
            Effect.catchCause((cause) => {
              const error = cause.pipe(Cause.squash, (squashed) =>
                TransactInternalError.is(squashed) ? Cause.squash(squashed.cause) : squashed,
              );
              // Split out-of-order mutations from application errors, as `handleMutateRequest` expects.
              return Effect.fail(error instanceof OutOfOrderMutation ? error : makeApplicationError(Cause.fail(error)));
            }),
            Effect.provide(ctx),
            Effect.runPromise,
          ),
        query: params,
        body: request as ReadonlyJSONValue,
        userID,
      }),
    catch: (e) => new HandleMutateError({ cause: Cause.fail(e) }),
  });
});

const runMutation = Effect.fn(function* <R>(mutators: Mutators.AnyMutators<R>, mutation: Types.Mutation) {
  // Support both "namespace|name" and "namespace.name" formats, and single-segment names.
  const [namespace, name] = mutation.name.includes("|") ? Str.split(mutation.name, "|") : Str.split(mutation.name, ".");

  const mutator = yield* Fn.pipe(
    mutators,
    Rec.get<string>(namespace),
    Option.flatMap((mutator) =>
      Match.value([mutator, name]).pipe(
        Match.when([Predicate.isObject, Predicate.isString], ([mutator, name]) => Rec.get<string>(name)(mutator)),
        Match.when([Predicate.isFunction, Predicate.isUndefined], ([mutator]) => Option.some(mutator)),
        Match.orElse(() => Option.none()),
      ),
    ),
    Effect.fromOption,
    Effect.catchTag("NoSuchElementError", () => Effect.fail(new MutatorNotFoundError({ name: mutation.name }))),
  );

  const args = yield* Schema.decodeUnknownEffect(mutator[Mutators.MutatorSchemaSymbol])(
    normalizeArgs(mutation.args[0]),
  ).pipe(Effect.catchTag("SchemaError", (e) => Effect.fail(new ServerArgsParseError({ cause: Cause.fail(e) }))));

  return yield* mutator(args);
});

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

export const handleQuery = Effect.fn(function* <E, R1, R2>(
  queries: MakeQueryResult<E, R1, R2>[],
  schema: ZeroSchema,
  payload: TransformRequestMessage,
  // The authenticated user ID (or `null`/`undefined` for logged-out clients), echoed back to Zero.
  userID: string | null | undefined,
) {
  const ctx = yield* Effect.context<R1 | R2>();
  return yield* Effect.tryPromise({
    try: () =>
      handleQueryRequest({
        handler: (name, args) =>
          Arr.findFirst(queries, (q) => q[QueryNameSymbol] === name).pipe(
            Effect.fromOption,
            Effect.catchTag("NoSuchElementError", () => Effect.fail(new QueryNotFound({ name }))),
            Effect.flatMap((query) => query[RunQuerySymbol]({ _tag: "Encoded", args })),
            Effect.runSyncWith(ctx),
          ),
        schema,
        // `query` params are unused by the query endpoint at runtime but required by the options type.
        query: {},
        body: payload as ReadonlyJSONValue,
        userID,
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
