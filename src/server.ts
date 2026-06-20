import type { ReadonlyJSONValue, Schema as ZeroSchema } from "@rocicorp/zero";
import { handleMutateRequest, handleQueryRequest, OutOfOrderMutation } from "@rocicorp/zero/server";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fn from "effect/Function";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Layer from "effect/Layer";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Str from "effect/String";
import type * as Unify from "effect/Unify";
import {
  MutatorNotFoundError,
  ServerArgsParseError,
  ServerTransactionInput,
  toApplicationError,
} from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { normalizeArgs, prefixId } from "./internal/utils.js";
import * as Mutators from "./mutators.js";
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
  // Capture only the mutators' genuinely external requirements: the per-mutation plumbing services
  // (the upstream `transact`, the response slot, the mutation input and the synchronization context)
  // are provided per-mutation below, so they must not leak into `handleMutate`'s own requirements.
  // The exclusion set is the exact union provided below, so `Effect.provide(ctx)` discharges the rest.
  const ctx =
    yield* Effect.context<
      Exclude<
        Mutators.ExtractMutatorsRequirements<TMutators>,
        | (typeof transaction.Transact)["Identifier"]
        | (typeof transaction.ResponseStore)["Identifier"]
        | ServerTransactionInput
        | ServerSynchronizationContext.ServerSynchronizationContext
      >
    >();

  return yield* Effect.tryPromise({
    try: () =>
      handleMutateRequest(
        transaction.database,
        (transact, mutation) => {
          const program = Effect.gen(function* () {
            const responseStore = yield* Ref.make(Option.none<Awaited<ReturnType<typeof transact>>>());

            const exit = yield* lookupAndDecode<Mutators.ExtractMutatorsRequirements<TMutators>>(
              mutators,
              mutation,
            ).pipe(
              Effect.flatMap(({ mutator, args }) => mutator(args).pipe(ServerSynchronizationContext.finalize)),
              // Provide all per-mutation plumbing in a single layer so the residual requirement is a
              // single `Exclude<..., union>` that exactly matches `ctx` and cancels to `never`.
              Effect.provide([
                Layer.succeed(transaction.Transact, transact),
                Layer.succeed(transaction.ResponseStore, responseStore),
                Layer.succeed(ServerTransactionInput, {
                  clientID: mutation.clientID,
                  mutationID: mutation.id,
                  clientGroupID: request.clientGroupID,
                  upstreamSchema: params.schema,
                }),
                ServerSynchronizationContext.layer,
              ]),
              Effect.exit,
            );

            return { exit, response: yield* Ref.get(responseStore) };
          }).pipe(Effect.provide(ctx));

          return Effect.runPromise(program).then(({ exit, response }) => {
            // A transaction ran: return its response (success, app error, or already-processed).
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
