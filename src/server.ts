import type { ReadonlyJSONValue, Schema as ZeroSchema } from "@rocicorp/zero";
import { handleQueryRequest } from "@rocicorp/zero/server";
import * as Arr from "effect/Array";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fn from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Str from "effect/String";
import { MutationAlreadyProcessedError, OutOfOrderMutationError, ServerTransactionInput } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import { prefixId } from "./internal/utils.js";
import * as Mutators from "./mutators.js";
import { type MakeQueryResult, QueryNameSymbol, RunQuerySymbol } from "./query.js";
import type * as ServerTransaction from "./server-transaction.js";
import * as Types from "./types/push.js";
import type { TransformRequestMessage } from "./types/queries.js";

// Updated to:
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts
// https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/process-mutations.ts

export const processPush = Effect.fn(function* <
  TSchema extends ZeroSchema,
  TTransaction,
  TMutators extends Mutators.AnyMutators,
>(
  transaction: ServerTransaction.Context<TSchema, TTransaction>,
  mutators: TMutators,
  params: Types.PushParams,
  request: Types.PushBody,
) {
  if (request.pushVersion !== 1) {
    return { error: "unsupportedPushVersion" as const };
  }

  const responses = yield* Stream.fromIterable(request.mutations).pipe(
    Stream.mapEffect(
      Effect.fn(function* (mutation) {
        if (mutation.type !== "custom") {
          return yield* new CustomMutationExpectedError();
        }

        return yield* processMutation<Mutators.ExtractMutatorsRequirements<TMutators>>(mutators, mutation).pipe(
          Effect.catch((e) => processMutationError(transaction, e)),
          Effect.map((result) =>
            Types.MutationResponse.make({ id: { id: mutation.id, clientID: mutation.clientID }, result }),
          ),
          Effect.provideService(ServerTransactionInput, {
            clientID: mutation.clientID,
            mutationID: mutation.id,
            clientGroupID: request.clientGroupID,
            upstreamSchema: params.schema,
          }),
          Effect.provide(ServerSynchronizationContext.layer),
        );
      }),
    ),
    // We only stop processing if the mutation is out of order.
    // If the mutation has already been processed or if it returns an application error,
    // we continue processing the next mutation.
    Stream.takeUntil(({ result }) => Predicate.hasProperty(result, "error") && result.error === "oooMutation"),
    Stream.runCollect,
  );

  return { mutations: responses } satisfies Types.PushResponse;
});

const processMutation = Effect.fn(function* <R>(mutators: Mutators.AnyMutators<R>, mutation: Types.Mutation) {
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

  const args = yield* Schema.decodeUnknownEffect(mutator[Mutators.MutatorSchemaSymbol])(mutation.args[0]).pipe(
    Effect.catchTag("SchemaError", (e) => Effect.fail(new ServerArgsParseError({ cause: Cause.fail(e) }))),
  );

  return yield* mutator(args).pipe(
    ServerSynchronizationContext.finalize,
    Effect.as<Types.MutationResult>({}),
    Effect.catchIf(OutOfOrderMutationError.is, (e) =>
      Effect.logError(e.message).pipe(
        Effect.as({ error: "oooMutation", details: e.message } satisfies Types.ZeroError),
      ),
    ),
    Effect.catchIf(MutationAlreadyProcessedError.is, (e) =>
      Effect.logWarning(e.message).pipe(
        Effect.as({ error: "alreadyProcessed", details: e.message } satisfies Types.ZeroError),
      ),
    ),
    // Case #5 "Zero transactions then fail" / #6 "Fail before transaction"
    // Catches all errors that are produced before the transaction is executed
    Effect.catchCause((cause) => Effect.fail(new MutationUserError({ cause }))),
  );
});

const processMutationError = Effect.fn(function* <TSchema extends ZeroSchema, TTransaction>(
  transaction: ServerTransaction.Context<TSchema, TTransaction>,
  e: unknown,
) {
  const { clientID, mutationID } = yield* ServerTransactionInput;

  yield* Effect.logError(`Unexpected error processing mutation ${mutationID} for client ${clientID}`, Cause.fail(e));

  const errorMessage = Match.value(e).pipe(
    Match.when(MutationUserError.is, (e) => e.message),
    Match.orElse(() => "Internal error"),
  );

  const appError = {
    error: "app",
    details: errorMessage,
  } satisfies Types.AppError;

  yield* transaction
    .execute(
      Effect.gen(function* () {
        const { transactionHooks } = yield* transaction.Context;
        return yield* Effect.tryPromise({
          try: () => transactionHooks.writeMutationResult({ id: { id: mutationID, clientID }, result: appError }),
          catch: (error) => new WriteMutationResultError({ cause: Cause.fail(error) }),
        });
      }),
    )
    .pipe(Effect.catchCause(Effect.logError));

  yield* Effect.logWarning(`Mutation ${mutationID} for client ${clientID} was retried after an error: ${e}`);

  return appError;
});

class WriteMutationResultError extends Data.TaggedError("WriteMutationResultError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class CustomMutationExpectedError extends Data.TaggedError("CustomMutationExpectedError") {}
class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  readonly name: string;
}> {}

class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<Schema.SchemaError>;
}> {}

const MutationUserErrorTypeId = Symbol.for(prefixId("MutationUserError"));
class MutationUserError extends Data.TaggedError("MutationUserError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  readonly [MutationUserErrorTypeId] = MutationUserErrorTypeId;
  static is(e: unknown): e is MutationUserError {
    return Predicate.hasProperty(e, MutationUserErrorTypeId);
  }

  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}

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
