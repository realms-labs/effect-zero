import type { Database, TransactionProviderHooks, TransactionProviderInput } from "@rocicorp/zero/pg";
import * as Cause from "effect/Cause";
import * as Chunk from "effect/Chunk";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fn from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import type * as ParseResult from "effect/ParseResult";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Str from "effect/String";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { type AnyMutators, type ExtractMutatorsRequirements, MutatorSchemaSymbol } from "./mutators.js";
import {
  type ZeroAppError,
  type ZeroError,
  type ZeroMutation,
  ZeroMutationResponse,
  type ZeroMutationResult,
  type ZeroPushBody,
  type ZeroPushParams,
  type ZeroPushResponse,
} from "./types.js";
import { prefixId } from "./utils.js";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

export interface ZeroServerTransactionContext {
  readonly _tag: unique symbol;
}

// NOTE(zero): Using the "Lifting the Generic" technique.
// NOTE(zero): Could choose to not accept `Database<T>` as a param and instead create some `ZeroDatabase` service inside the
// `make` function, which various other functions require. However, this means that the user will have to manually
// specify the `T` type parameter which seemed a bit clunky.
export const makeServer = <T, I = never>(options: { database: Database<T>; clientTransaction?: Context.Tag<I, T> }) => {
  const ZeroServerTransactionContext = Context.Tag(prefixId("ZeroServerTransactionContext"))<
    ZeroServerTransactionContext,
    { transaction: T; transactionHooks: TransactionProviderHooks }
  >();

  // TODO: Maybe prefix this / suffix this with a tag passed in to `make`?
  const execute = Effect.fn(function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
    const runtime =
      // TODO: Is there a cleaner way to write this?
      yield* Effect.runtime<Exclude<Exclude<R | ZeroTransactionInput, ZeroServerTransactionContext>, I>>();
    const result = yield* Deferred.make<A, E | Effect.Effect.Error<typeof checkAndIncrementLastMutationID>>();

    const transactionInput = yield* ZeroTransactionInput;
    yield* Effect.tryPromise({
      try: (signal) =>
        options.database.transaction(async (transaction, transactionHooks) => {
          const exit = await Effect.zipRight(checkAndIncrementLastMutationID, effect).pipe(
            Effect.provideService(ZeroServerTransactionContext, { transaction, transactionHooks }),
            options.clientTransaction
              ? Effect.provideService(options.clientTransaction, transaction)
              : // TODO: Is there a cleaner way to write this?
                <A, E, R>(effect: Effect.Effect<A, E, R>) => effect as Effect.Effect<A, E, Exclude<R, I>>,
            (effect) => Runtime.runPromiseExit(runtime, effect, { signal }),
          );
          Deferred.unsafeDone(result, exit);
          return Exit.getOrElse(exit, () => {
            // This error's purpose is to differentiate between "external" errors
            // that originate from the user-defined mutator code and "internal" errors
            // that originate from our own code and the Zero API.
            // Both types are caught in the "catch" block below, but at this point we only need to handle
            // the "internal" errors wrapping them in a `ZeroDatabaseError`, because "external" errors
            // are already covered by passing the Exit result to the Deferred, which is why
            // we have the ZeroTransactionUserError silenced below in the pipe.
            throw new ZeroTransactionUserError();
          });
        }, transactionInput),
      catch: (error) => {
        if (ZeroTransactionUserError.is(error)) {
          return error;
        }
        // This is for errors that occur when calling `database.transaction` despite the provided `effect` succeeding.
        // This can be caused by e.g. the database connection timing out or other database-related issues.
        return new ZeroDatabaseError({ cause: Cause.fail(error) });
      },
    }).pipe(Effect.catchTag("ZeroTransactionUserError", () => Effect.void));

    return yield* result;
  }, ZeroServerMutationContext.guard);

  const use = <A>(fn: (transaction: T, options: { readonly signal: AbortSignal }) => PromiseLike<A>) =>
    Effect.flatMap(ZeroServerTransactionContext, (ctx) =>
      Effect.tryPromise({
        try: (signal) => fn(ctx.transaction, { signal }),
        catch: Fn.identity,
      }),
    );

  const checkAndIncrementLastMutationID = Effect.gen(function* () {
    const { transactionHooks } = yield* ZeroServerTransactionContext;
    const { clientID, mutationID: receivedMutationID } = yield* ZeroTransactionInput;

    const { lastMutationID } = yield* Effect.tryPromise({
      try: () => transactionHooks.updateClientMutationID(),
      catch: (error) => new UpdateClientMutationIDError({ cause: Cause.fail(error) }),
    });

    if (receivedMutationID < lastMutationID) {
      return yield* new MutationAlreadyProcessedError({
        clientID,
        received: receivedMutationID,
        actual: lastMutationID,
      });
    }
    if (receivedMutationID > lastMutationID) {
      return yield* new OutOfOrderMutationError({
        clientID,
        receivedMutationID,
        lastMutationID,
      });
    }
  });

  const processPush = Effect.fn(function* <T extends AnyMutators>(
    mutators: T,
    params: ZeroPushParams,
    request: ZeroPushBody,
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
          return yield* processMutation<ExtractMutatorsRequirements<T>>(mutators, mutation).pipe(
            Effect.map((result) =>
              ZeroMutationResponse.make({ id: { id: mutation.id, clientID: mutation.clientID }, result }),
            ),
            Effect.provideService(ZeroTransactionInput, {
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
      Effect.annotateLogs({ module: prefixId("ZeroServer"), method: "processPush" }),
    );

    return { mutations: Chunk.toArray(responses) } satisfies ZeroPushResponse;
  });

  const processMutation = Effect.fn(
    function* <R>(mutators: AnyMutators<R>, mutation: ZeroMutation) {
      // Support both "namespace|name" and "namespace.name" formats, and single-segment names.
      const [namespace, name] = mutation.name.includes("|")
        ? Str.split(mutation.name, "|")
        : Str.split(mutation.name, ".");

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
        Effect.catchTag("NoSuchElementException", () => new MutatorNotFoundError({ mutationName: mutation.name })),
      );

      const args = yield* Schema.decode(mutator[MutatorSchemaSymbol])(mutation.args[0]).pipe(
        Effect.catchTag("ParseError", (e) => new ZeroServerArgsParseError({ cause: Cause.fail(e) })),
      );

      return yield* mutator(args).pipe(
        // Case #2 "One transaction then fail"
        // If the transaction was executed successfully, swallow the error and just log it, otherwise re-throw it
        Effect.catchAllCause(
          Effect.fn(function* (e) {
            const wasTransactionExecuted = yield* ZeroServerMutationContext.wasTransactionExecuted;
            if (wasTransactionExecuted) {
              return yield* Effect.logError("Error occurred after transaction execution completed", e);
            }
            return yield* Effect.failCause(e);
          }),
        ),
        // Case #4 "Zero transactions then succeed"
        // Check that the transaction was executed during the mutation
        Effect.tap(
          Effect.gen(function* () {
            const wasTransactionExecuted = yield* ZeroServerMutationContext.wasTransactionExecuted;
            if (!wasTransactionExecuted) {
              return yield* new NoTransactionError();
            }
          }),
        ),
        Effect.as<ZeroMutationResult>({}),
        Effect.catchIf(OutOfOrderMutationError.is, (e) =>
          Effect.logError(e.message).pipe(Effect.as({ error: "oooMutation", details: e.message } satisfies ZeroError)),
        ),
        Effect.catchIf(MutationAlreadyProcessedError.is, (e) =>
          Effect.logWarning(e.message).pipe(
            Effect.as({ error: "alreadyProcessed", details: e.message } satisfies ZeroError),
          ),
        ),
        // Case #5 "Zero transactions then fail" / #6 "Fail before transaction"
        // Catches all errors that are produced before the transaction is executed
        Effect.catchAllCause((cause) => new ZeroMutationUserError({ cause })),
      );
    },
    Effect.catchAll((e) => processMutationError(e)),
    Effect.provide(ZeroServerMutationContext.Default),
  );

  const processMutationError = Effect.fn(function* (e: unknown) {
    const { clientID, mutationID } = yield* ZeroTransactionInput;

    yield* Effect.logError(`Unexpected error processing mutation ${mutationID} for client ${clientID}`, Cause.fail(e));

    const errorMessage = Match.value(e).pipe(
      Match.when(ZeroMutationUserError.is, (e) => e.message),
      Match.orElse(() => "Internal error"),
    );

    const appError = {
      error: "app",
      details: errorMessage,
    } satisfies ZeroAppError;

    yield* execute(
      Effect.flatMap(ZeroServerTransactionContext, ({ transactionHooks }) => {
        return Effect.tryPromise({
          try: () => transactionHooks.writeMutationResult({ id: { id: mutationID, clientID }, result: appError }),
          catch: (error) => new WriteMutationResultError({ cause: Cause.fail(error) }),
        });
      }),
    ).pipe(Effect.catchAllCause(Effect.logError));

    yield* Effect.logWarning(`Mutation ${mutationID} for client ${clientID} was retried after an error: ${e}`);

    return appError;
  });

  return {
    Transaction: Object.assign(ZeroServerTransactionContext, { execute, use }),
    processPush,
  };
};

class ZeroTransactionInput extends Context.Tag(prefixId("ZeroTransactionInput"))<
  ZeroTransactionInput,
  TransactionProviderInput
>() {}

class ZeroServerMutationContext extends Effect.Service<ZeroServerMutationContext>()(
  prefixId("ZeroServerMutationContext"),
  {
    effect: Effect.gen(function* () {
      return { wasTransactionExecuted: yield* SynchronizedRef.make(false) };
    }),
  },
) {
  static wasTransactionExecuted = Effect.flatMap(ZeroServerMutationContext, (ctx) => ctx.wasTransactionExecuted);

  // Ensures that only one transaction is executed at a time and checks that another transaction wasn't already executed.
  static guard = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    Effect.flatMap(this, ({ wasTransactionExecuted }) =>
      SynchronizedRef.modifyEffect(
        wasTransactionExecuted,
        Effect.fn(function* (wasTransactionExecuted) {
          if (wasTransactionExecuted) {
            return yield* new MultipleTransactionsError();
          }
          const result = yield* effect;
          return [result, true];
        }),
      ),
    );
}

class ZeroDatabaseError extends Data.TaggedError("ZeroDatabaseError")<{
  cause: Cause.Cause<unknown>;
}> {}

class UpdateClientMutationIDError extends Data.TaggedError("UpdateClientMutationIDError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}

class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  mutationName: string;
}> {
  override message = `Mutator not found for mutation ${this.mutationName}`;
}

class WriteMutationResultError extends Data.TaggedError("WriteMutationResultError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class CustomMutationExpectedError extends Data.TaggedError("CustomMutationExpectedError")<object> {}

export class MultipleTransactionsError extends Data.TaggedError("MultipleTransactionsError") {
  override message = "Multiple transactions detected in a mutation, only one transaction is allowed.";
}

export class NoTransactionError extends Data.TaggedError("NoTransactionError") {
  override message = "No transaction detected in a mutation, a transaction is required.";
}

const OutOfOrderMutationErrorTypeId = Symbol.for(prefixId("OutOfOrderMutationError"));
export class OutOfOrderMutationError extends Data.TaggedError("OutOfOrderMutationError")<{
  readonly clientID: string;
  readonly receivedMutationID: number;
  readonly lastMutationID: number | bigint;
}> {
  readonly [OutOfOrderMutationErrorTypeId] = OutOfOrderMutationErrorTypeId;
  override get message() {
    return `Client ${this.clientID} sent mutation ID ${this.receivedMutationID} but expected ${this.lastMutationID}`;
  }
  static is(e: unknown): e is OutOfOrderMutationError {
    return Predicate.hasProperty(e, OutOfOrderMutationErrorTypeId);
  }
}

const MutationAlreadyProcessedErrorTypeId = Symbol.for(prefixId("MutationAlreadyProcessedError"));
export class MutationAlreadyProcessedError extends Data.TaggedError("MutationAlreadyProcessedError")<{
  readonly clientID: string;
  readonly received: number;
  readonly actual: number | bigint;
}> {
  readonly [MutationAlreadyProcessedErrorTypeId] = MutationAlreadyProcessedErrorTypeId;
  override get message() {
    return `Ignoring mutation from ${this.clientID} with ID ${this.received} as it was already processed. Expected: ${this.actual}`;
  }
  static is(e: unknown): e is MutationAlreadyProcessedError {
    return Predicate.hasProperty(e, MutationAlreadyProcessedErrorTypeId);
  }
}

const ZeroTransactionUserErrorTypeId = Symbol.for(prefixId("ZeroTransactionUserError"));
export class ZeroTransactionUserError extends Data.TaggedError("ZeroTransactionUserError") {
  readonly [ZeroTransactionUserErrorTypeId] = ZeroTransactionUserErrorTypeId;
  static is(e: unknown): e is ZeroTransactionUserError {
    return Predicate.hasProperty(e, ZeroTransactionUserErrorTypeId);
  }
}

const ZeroMutationUserErrorTypeId = Symbol.for(prefixId("ZeroMutationUserError"));
export class ZeroMutationUserError extends Data.TaggedError("ZeroMutationUserError")<{
  cause: Cause.Cause<unknown>;
}> {
  readonly [ZeroMutationUserErrorTypeId] = ZeroMutationUserErrorTypeId;
  static is(e: unknown): e is ZeroMutationUserError {
    return Predicate.hasProperty(e, ZeroMutationUserErrorTypeId);
  }

  override get message() {
    const err = Cause.squash(this.cause);
    return err instanceof Error ? err.message : "exception was not of type `Error`";
  }
}

class ZeroServerArgsParseError extends Data.TaggedError("ZeroServerArgsParseError")<{
  cause: Cause.Cause<ParseResult.ParseError>;
}> {}
