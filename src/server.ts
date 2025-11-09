import * as Cause from "effect/Cause";
import * as Chunk from "effect/Chunk";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Fn from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import type * as ParseResult from "effect/ParseResult";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Str from "effect/String";
import * as Mutators from "./mutators.js";
import {
  MutationAlreadyProcessedError,
  OutOfOrderMutationError,
  ServerSynchronizationContext,
  ServerTransactionInput,
} from "./server-internal.js";
import type * as ServerTransaction from "./server-transaction.js";
import * as Types from "./types.js";
import { prefixId } from "./utils.js";

type ServerTransactionContext<TTransaction> = Omit<
  ReturnType<typeof ServerTransaction.make<string, TTransaction>>,
  "use"
>;

export const processPush = Effect.fn(function* <TTransaction, TMutators extends Mutators.AnyMutators>(
  transaction: ServerTransactionContext<TTransaction>,
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
          return yield* new CustomMutationExpectedError({});
        }
        return yield* processMutation<Mutators.ExtractMutatorsRequirements<TMutators>>(mutators, mutation).pipe(
          Effect.catchAll((e) => processMutationError(transaction, e)),
          Effect.map((result) =>
            Types.MutationResponse.make({ id: { id: mutation.id, clientID: mutation.clientID }, result }),
          ),
          Effect.provideService(ServerTransactionInput, {
            clientID: mutation.clientID,
            mutationID: mutation.id,
            clientGroupID: request.clientGroupID,
            upstreamSchema: params.schema,
          }),
        );
      }),
    ),
    // We only stop processing if the mutation is out of order.
    // If the mutation has already been processed or if it returns an application error,
    // we continue processing the next mutation.
    Stream.takeUntil(({ result }) => Predicate.hasProperty(result, "error") && result.error === "oooMutation"),
    Stream.runCollect,
  );

  return { mutations: Chunk.toArray(responses) } satisfies Types.PushResponse;
}, Effect.provide(ServerSynchronizationContext.Default));

const processMutation = Effect.fn(function* <R>(mutators: Mutators.AnyMutators<R>, mutation: Types.Mutation) {
  // Support both "namespace|name" and "namespace.name" formats, and single-segment names.
  const [namespace, name] = mutation.name.includes("|") ? Str.split(mutation.name, "|") : Str.split(mutation.name, ".");

  const mutator = yield* Fn.pipe(
    mutators,
    Rec.get<string>(namespace),
    Option.flatMap((mutator) =>
      Match.value([mutator, name]).pipe(
        Match.when([Predicate.isRecord, Predicate.isString], ([mutator, name]) => Rec.get<string>(name)(mutator)),
        Match.when([Predicate.isFunction, Predicate.isUndefined], ([mutator]) => Option.some(mutator)),
        Match.orElse(() => Option.none()),
      ),
    ),
    Effect.catchTag("NoSuchElementException", () => new MutatorNotFoundError({ name: mutation.name })),
  );

  const args = yield* Schema.decode(mutator[Mutators.MutatorSchemaSymbol])(mutation.args[0]).pipe(
    Effect.catchTag("ParseError", (e) => new ServerArgsParseError({ cause: Cause.fail(e) })),
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
    Effect.catchAllCause((cause) => new MutationUserError({ cause })),
  );
});

const processMutationError = Effect.fn(function* <TTransaction>(
  transaction: ServerTransactionContext<TTransaction>,
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
      Effect.flatMap(transaction, ({ transactionHooks }) => {
        return Effect.tryPromise({
          try: () => transactionHooks.writeMutationResult({ id: { id: mutationID, clientID }, result: appError }),
          catch: (error) => new WriteMutationResultError({ cause: Cause.fail(error) }),
        });
      }),
    )
    .pipe(Effect.catchAllCause(Effect.logError));

  yield* Effect.logWarning(`Mutation ${mutationID} for client ${clientID} was retried after an error: ${e}`);

  return appError;
});

class WriteMutationResultError extends Data.TaggedError("WriteMutationResultError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class CustomMutationExpectedError extends Data.TaggedError("CustomMutationExpectedError")<object> {}
class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  readonly name: string;
}> {}

class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<ParseResult.ParseError>;
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
