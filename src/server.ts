import type { ReadonlyJSONValue, Schema as ZeroSchema } from "@rocicorp/zero";
import { handleMutateRequest, handleQueryRequest, OutOfOrderMutation } from "@rocicorp/zero/server";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import * as SynchronizedRef from "effect/SynchronizedRef";
import type * as Unify from "effect/Unify";
import { decodeArgs, lookupLeaf } from "./internal/mutator-tree.js";
import { MutatorNotFoundError, ServerArgsParseError, toApplicationError } from "./internal/server.js";
import { prefixId } from "./internal/utils.js";
import type * as Mutators from "./mutators.js";
import { type MakeQueryResult, QueryNameSymbol, RunQuerySymbol } from "./query.js";
import type * as ServerTransaction from "./server-transaction.js";
import type * as Types from "./types/push.js";
import type { TransformRequestMessage } from "./types/queries.js";

// Updated to:
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/process-mutations.ts

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

// A thin, Effect-ful wrapper around the upstream `handleMutateRequest`. It mirrors `handleQuery`:
// upstream owns the per-mutation loop, last-mutation-id ordering, app-error responses and out-of-order
// handling; we only resolve+run the matching Effect mutator for each mutation. The mutator drives the
// actual transaction through `serverTransaction.execute`, which uses the upstream `transact` callback.
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
  // Capture only the mutators' genuinely external requirements: the per-mutation `Mutation` service is
  // provided per-mutation below, so it must not leak into `handleMutate`'s own requirements. The
  // exclusion exactly matches what is provided, so `Effect.provide(ctx)` discharges the rest.
  const ctx =
    yield* Effect.context<
      Exclude<Mutators.ExtractMutatorsRequirements<TMutators>, (typeof transaction.Mutation)["Identifier"]>
    >();

  return yield* Effect.tryPromise({
    try: () =>
      handleMutateRequest(
        transaction.database,
        (transact, mutation) => {
          // Wrap the upstream `transact` so this mutation's `MutationResponse` is captured for us to
          // return. `execute` only ever sees this single wrapped function (the `Mutation` service) —
          // it does not deal with the response slot.
          let response = Option.none<Awaited<ReturnType<typeof transact>>>();
          const runTransact: typeof transact = (callback) =>
            transact(callback).then((mutationResponse) => {
              response = Option.some(mutationResponse);
              return mutationResponse;
            });

          const program = Effect.gen(function* () {
            const executed = yield* SynchronizedRef.make(false);
            return yield* lookupAndDecode<Mutators.ExtractMutatorsRequirements<TMutators>>(mutators, mutation).pipe(
              Effect.flatMap(({ mutator, args }) => transaction.finalize(mutator(args))),
              // Provide the single per-mutation `Mutation` service (wrapped transact + sync flag) so the
              // residual requirement is `Exclude<..., Mutation>`, which exactly matches `ctx`.
              Effect.provideService(transaction.Mutation, { transact: runTransact, executed }),
              Effect.exit,
            );
          }).pipe(Effect.provide(ctx));

          return Effect.runPromise(program).then((exit) => {
            // A transaction ran: return its captured response (success, app error, or already-processed).
            if (Option.isSome(response)) return response.value;
            // No transaction ran: a pre-transaction failure (or NoTransactionError).
            const cause = Exit.isFailure(exit) ? exit.cause : Cause.empty;
            const error = Cause.squash(cause);
            // Out-of-order must reach handleMutateRequest's top-level handler (-> PushFailed), so it
            // must NOT be converted into an application error.
            if (error instanceof OutOfOrderMutation) throw error;
            throw toApplicationError(cause);
          });
        },
        params,
        request as ReadonlyJSONValue,
      ),
    // Mirrors handleQuery's QueryRequestError: only genuine infra/defect rejections reach here.
    // (Out-of-order resolves to a top-level PushFailed value, so it does not land in this catch.)
    catch: (e) => new HandleMutateError({ cause: Cause.fail(e) }),
  });
});

const lookupAndDecode = Effect.fn(function* <R>(
  mutators: Mutators.AnyMutators<R>,
  mutation: { readonly name: string; readonly args: ReadonlyArray<ReadonlyJSONValue> },
) {
  const mutator = yield* lookupLeaf(mutators, mutation.name).pipe(
    Effect.fromOption,
    Effect.catchTag("NoSuchElementError", () => Effect.fail(new MutatorNotFoundError({ name: mutation.name }))),
  );

  const args = yield* decodeArgs(mutator, mutation.args[0]).pipe(
    Effect.catchTag("SchemaError", (e) => Effect.fail(new ServerArgsParseError({ cause: Cause.fail(e) }))),
  );

  return { mutator, args };
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
                throw new QueryUserError<E>({ cause: exit.cause });
              }
              return exit.value;
            },
          ),
        schema,
        payload as ReadonlyJSONValue,
      ),
    catch: (e) =>
      Option.liftPredicate(e, QueryUserError.is<E>).pipe(
        Option.flatMap((e) => Cause.findErrorOption(e.cause)),
        Option.getOrElse(() => new QueryRequestError({ cause: Cause.fail(e) })),
      ),
  });
});

class QueryNotFound extends Data.TaggedError("QueryNotFound")<{ name: string }> {
  override get message() {
    return `Query not found: ${this.name}`;
  }
}

const QueryUserErrorTypeId = Symbol.for(prefixId("QueryUserError"));
class QueryUserError<E> extends Data.TaggedError("QueryUserError")<{
  cause: Cause.Cause<E | Schema.SchemaError | QueryNotFound>;
}> {
  readonly [QueryUserErrorTypeId] = QueryUserErrorTypeId;
  static is<E>(e: unknown): e is QueryUserError<E> {
    return Predicate.hasProperty(e, QueryUserErrorTypeId);
  }
}

class QueryRequestError extends Data.TaggedError("QueryRequestError")<{ cause: Cause.Cause<unknown> }> {}
