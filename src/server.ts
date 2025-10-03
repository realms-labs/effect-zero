import type { Database, TransactionProviderHooks, TransactionProviderInput } from "@rocicorp/zero/pg";
import * as Cause from "effect/Cause";
import * as Chunk from "effect/Chunk";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { identity, pipe } from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Stream from "effect/Stream";
import * as Str from "effect/String";
import * as Ref from "effect/Ref";

import { prefixId } from "./utils";
import type { MutatorArgs, MutatorSchema } from "./mutators";
import {
  type ZeroAppError,
  type ZeroError,
  type ZeroMutation,
  ZeroMutationResponse,
  type ZeroMutationResult,
  type ZeroPushBody,
  type ZeroPushParams,
  type ZeroPushResponse,
} from "./types";

// Updated to: https://github.com/rocicorp/mono/blob/3082c9fa061891067b4bd7dc9fe74f798270d8d7/packages/zero-server/src/push-processor.ts

// TODO(zero) thing that prevents using multiple transactions in a mutator.
// - update: Ah, maybe handled by `OutOfOrderMutationError` and `MutationAlreadyProcessedError`?
// TODO(zero) I think this thing also needs to ensure that *exactly* one transaction occurs, as otherwise the
// lastMutationID won't be incremented.

// NOTE(zero): Using the "Lifting the Generic" technique.
// NOTE(zero): Could not accept `Database<T>` as a param and instead create some `ZeroDatabase` service inside the
// `make` function, which various other functions require. However, this means that the user will have to manually
// specify the `T` type parameter which seemed a bit clunky.
export const makeServer = <T, I = never>(options: { database: Database<T>; clientTransaction?: Context.Tag<I, T> }) => {
  // TODO: Maybe prefix this / suffix this with a tag passed in to `make`?
  class ZeroServerTransactionContext extends Context.Tag(prefixId("ZeroServerTransactionContext"))<
    ZeroServerTransactionContext,
    { transaction: T; transactionHooks: TransactionProviderHooks }
  >() {
    static execute = Effect.fn(
      function* <A, E, R>(effect: Effect.Effect<A, E, R>) {
        const mutationCtx = yield* ZeroServerMutationContext;
        const wasTransactionExecuted = yield* Ref.get(mutationCtx.wasTransactionExecuted);
        if (wasTransactionExecuted) {
          return yield* new MultipleTransactionsError();
        }

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
                /*
                This error's purpose is to differentiate between "external" errors
                that originate from the user-defined mutator code and "internal" errors
                that originate from our own code and the Zero API.
                Both types are caught in the "catch" block below, but at this point we only need to handle
                the "internal" errors wrapping them in a `ZeroDatabaseError`, because "external" errors
                are already covered by passing the Exit result to the Deferred, which is why
                we have the ZeroTransactionUserError silenced below in the pipe.
                */
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
      },
      Effect.tap(ZeroServerMutationContext.pipe(Effect.flatMap((ctx) => Ref.set(ctx.wasTransactionExecuted, true)))),
    );

    static use = <A>(fn: (transaction: T, options: { readonly signal: AbortSignal }) => PromiseLike<A>) =>
      Effect.andThen(ZeroServerTransactionContext, (ctx) =>
        Effect.tryPromise({
          try: (signal) => fn(ctx.transaction, { signal }),
          catch: identity,
        }),
      );
  }

  /** @internal */
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

  const processPush = Effect.fn(function* <R>(
    mutators: MutatorSchema<R>,
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
          return yield* processMutation(mutators, mutation).pipe(
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

  /** @internal */
  const processMutation = Effect.fn(
    function* <R>(mutators: MutatorSchema<R>, mutation: ZeroMutation) {
      // Support both "namespace|name" and "namespace.name" formats, and single-segment names.
      const [namespace, name] = mutation.name.includes("|")
        ? Str.split(mutation.name, "|")
        : Str.split(mutation.name, ".");
      const mutator = yield* pipe(
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

      return yield* mutator(mutation.args[0]).pipe(
        // If the transaction was executed successfully, swallow the error and just log it, otherwise re-throw it
        Effect.catchAllCause((e) =>
          Effect.gen(function* () {
            const ctx = yield* ZeroServerMutationContext;
            const wasTransactionExecuted = yield* Ref.get(ctx.wasTransactionExecuted);
            if (wasTransactionExecuted) {
              return yield* Effect.logError(e);
            }
            return yield* Effect.failCause(e);
          }),
        ),
        // Check that the transaction was executed during the mutation
        Effect.tap(
          Effect.gen(function* () {
            const ctx = yield* ZeroServerMutationContext;
            const wasTransactionExecuted = yield* Ref.get(ctx.wasTransactionExecuted);
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
        Effect.catchAllCause((e) => new ZeroMutationUserError({ cause: e })),
      );
    },
    Effect.catchAll((e) => processMutationError(e)),
    ZeroServerMutationContext.provide,
  );

  /** @internal */
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

    yield* ZeroServerTransactionContext.execute(
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

  const mutators =
    <M extends MutatorArgs>() =>
    <R>(mutators: MutatorSchema<R, M>) =>
      mutators;

  return {
    Transaction: ZeroServerTransactionContext,
    processPush,
    mutators,
  };
};

/** @internal */
class ZeroTransactionInput extends Context.Tag(prefixId("ZeroTransactionInput"))<
  ZeroTransactionInput,
  TransactionProviderInput
>() {}

class ZeroServerMutationContext extends Context.Tag(prefixId("ZeroServerMutationContext"))<
  ZeroServerMutationContext,
  { wasTransactionExecuted: Ref.Ref<boolean> }
>() {
  static provide = Effect.provideServiceEffect(
    ZeroServerMutationContext,
    Effect.gen(function* () {
      return {
        wasTransactionExecuted: yield* Ref.make(false),
      };
    }),
  );
}

class ZeroDatabaseError extends Data.TaggedError("ZeroDatabaseError")<{ cause: Cause.Cause<unknown> }> {}
class UpdateClientMutationIDError extends Data.TaggedError("UpdateClientMutationIDError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{ mutationName: string }> {
  override message = `Mutator not found for mutation ${this.mutationName}`;
}
class WriteMutationResultError extends Data.TaggedError("WriteMutationResultError")<{
  readonly cause: Cause.Cause<unknown>;
}> {}
class CustomMutationExpectedError extends Data.TaggedError("CustomMutationExpectedError")<object> {}

const OutOfOrderMutationErrorTypeId = Symbol.for(prefixId("OutOfOrderMutationError"));
/** @internal */
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
/** @internal */
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

const MultipleTransactionsErrorTypeId = Symbol.for(prefixId("MultipleTransactionsError"));
/** @internal */
export class MultipleTransactionsError extends Data.TaggedError("MultipleTransactionsError") {
  readonly [MultipleTransactionsErrorTypeId] = MultipleTransactionsErrorTypeId;
  override get message() {
    return `Multiple transactions detected in a mutation, only one transaction is allowed.`;
  }
  static is(e: unknown): e is MultipleTransactionsError {
    return Predicate.hasProperty(e, MultipleTransactionsErrorTypeId);
  }
}

const NoTransactionErrorTypeId = Symbol.for(prefixId("NoTransactionError"));
export class NoTransactionError extends Data.TaggedError("NoTransactionError") {
  readonly [NoTransactionErrorTypeId] = NoTransactionErrorTypeId;
  override get message() {
    return `No transaction detected in a mutation, a transaction is required.`;
  }
  static is(e: unknown): e is NoTransactionError {
    return Predicate.hasProperty(e, NoTransactionErrorTypeId);
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
