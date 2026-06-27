import type {
  ReadonlyJSONValue,
  Schema as ZeroSchema,
  ServerTransaction as ZeroServerTransaction,
} from "@rocicorp/zero";
import { handleMutateRequest, handleQueryRequest, OutOfOrderMutation } from "@rocicorp/zero/server";
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
import {
  ExecuteTransactError,
  MutationShortCircuit,
  MutatorNotFoundError,
  OutOfOrderMutationError,
  ServerArgsParseError,
  toApplicationError,
} from "./internal/server.js";
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
      handleMutateRequest(
        transaction[DatabaseSymbol],
        (transact_, mutation) =>
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
                      Effect.catchCause((cause) => Effect.die(toApplicationError(cause))),
                      Effect.asVoid,
                      Effect.provide(innerCtx),
                      (effect) => Effect.runPromise(effect, { signal }),
                    ),
                  ),
                // Out-of-order is thrown by upstream's `transact` before our mutator runs; wrap it in a
                // tagged error so it stays in the Effect channel, and re-raise the raw error only at the
                // top level (see below), where upstream recognizes it.
                catch: (error) =>
                  error instanceof OutOfOrderMutation
                    ? new OutOfOrderMutationError({ cause: Cause.fail(error) })
                    : new ExecuteTransactError({ cause: Cause.fail(error) }),
              }).pipe(Effect.flatMap((value) => Deferred.succeed(response, value)));

              return yield* Deferred.poll(exitDeferred).pipe(
                Effect.flatMap(Effect.fromOption),
                Effect.catchTag("NoSuchElementError", () => Effect.fail(new MutationShortCircuit())),
                Effect.flatten,
              );
            });

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
            // Unwrap and re-raise out-of-order errors as the raw `OutOfOrderMutation`: upstream
            // `handleMutateRequest` recognizes it (by identity) and turns it into a top-level `PushFailed`.
            // Every other failure becomes a per-mutation `ApplicationError` (info-hiding `message`).
            Effect.catchCause((cause) => {
              const error = Cause.squash(cause);
              return OutOfOrderMutationError.is(error)
                ? Effect.failCause(error.cause)
                : Effect.fail(toApplicationError(cause));
            }),
            Effect.provide(ctx),
            Effect.runPromise,
          ),
        params,
        request as ReadonlyJSONValue,
      ),
    // Mirrors handleQuery's QueryRequestError: only genuine infra/defect rejections reach here.
    // (Out-of-order resolves to a top-level PushFailed value, so it does not land in this catch.)
    catch: (e) => new HandleMutateError({ cause: Cause.fail(e) }),
  });
});

const runMutation = Effect.fn(function* <R>(
  mutators: Mutators.AnyMutators<R>,
  mutation: { readonly name: string; readonly args: ReadonlyArray<ReadonlyJSONValue> },
) {
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

class HandleMutateError extends Data.TaggedError("HandleMutateError")<{ readonly cause: Cause.Cause<unknown> }> {}

export const handleQuery = Effect.fn(function* <E, R1, R2>(
  queries: MakeQueryResult<E, R1, R2>[],
  schema: ZeroSchema,
  payload: TransformRequestMessage,
) {
  const ctx = yield* Effect.context<R1 | R2>();
  return yield* Effect.tryPromise({
    try: () =>
      handleQueryRequest(
        (name, args) =>
          Arr.findFirst(queries, (q) => q[QueryNameSymbol] === name).pipe(
            Effect.fromOption,
            Effect.catchTag("NoSuchElementError", () => Effect.fail(new QueryNotFound({ name }))),
            Effect.flatMap((query) => query[RunQuerySymbol]({ _tag: "Encoded", args })),
            Effect.runSyncExitWith(ctx),
            (exit) => {
              if (Exit.isFailure(exit)) {
                // Rethrow the underlying error so upstream's `getErrorMessage`/`getErrorDetails` produce a
                // clean per-query `{ error: "app", message, details? }` response — rather than wrapping the
                // Effect `Cause` (which upstream would serialize into a `"Parsed message: {…}"` blob).
                throw Cause.squash(exit.cause);
              }
              return exit.value;
            },
          ),
        schema,
        payload as ReadonlyJSONValue,
      ),
    // `handleQueryRequest` resolves per-query failures to a `transformFailed`/per-query error and does not
    // reject under normal operation, so this only catches a genuine infra/defect — mirrors handleMutate.
    catch: (e) => new QueryRequestError({ cause: Cause.fail(e) }),
  });
});

class QueryNotFound extends Data.TaggedError("QueryNotFound")<{ name: string }> {
  override get message() {
    return `Query not found: ${this.name}`;
  }
}

class QueryRequestError extends Data.TaggedError("QueryRequestError")<{ cause: Cause.Cause<unknown> }> {}
