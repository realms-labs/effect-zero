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
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Str from "effect/String";
import type * as Unify from "effect/Unify";
import { TransactCallbackNotInvokedError, TransactInternalError } from "./internal/server.js";
import * as ServerSynchronizationContext from "./internal/server-synchronization-context.js";
import type * as Mutator from "./mutator.js";
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
  TMutators extends Mutator.AnyServerMutators,
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
              mutation.args[0],
            ).pipe(
              ServerSynchronizationContext.finalize,
              Effect.provide([
                Layer.succeed(transaction[ServerTransactionCallbackSymbol], { transact }),
                ServerSynchronizationContext.layer,
              ]),
              Effect.withSpan("zero.mutation.handle", {
                attributes: {
                  "zero.mutator": mutation.name,
                  "zero.clientID": mutation.clientID,
                  "zero.mutationID": mutation.id,
                },
              }),
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
        query,
        body: body as ReadonlyJSONValue,
        userID: userId,
      }),
    catch: (e) => new HandleMutateError({ cause: Cause.fail(e) }),
  });
});

/**
 * Runs a mutation: its override if it has one, or else its shared body in a server transaction. Like upstream, names
 * are split on "." or "|" (the separator of Zero's legacy custom mutators). Their services aren't tracked from here
 * on: `handleMutate` already requires every implementation's.
 */
const runMutation = Effect.fnUntraced(function* <TSchema extends ZeroSchema, TTransaction>(
  mutators: Mutator.AnyServerMutators,
  transaction: ServerTransaction.Context<TSchema, TTransaction>,
  name: string,
  encodedArgs: unknown,
) {
  const { registry, overrides } = Rec.map(mutators, (root) =>
    Arr.reduce(Str.split(name, /[.|]/), Option.some<unknown>(root), (node, key) =>
      node.pipe(
        Option.filter(Predicate.hasProperty(key)),
        Option.map((value) => value[key]),
      ),
    ),
  );
  const mutator = yield* registry.pipe(
    Option.filter(
      (u): u is Mutator.Mutator<unknown, unknown, unknown, never> =>
        Predicate.isFunction(u) && Predicate.hasProperty(u, "mutatorName"),
    ),
    Effect.fromOption(() => new MutatorNotFoundError({ name })),
  );
  const args = yield* Schema.decodeUnknownEffect(mutator.payload)(encodedArgs).pipe(
    Effect.catchTag("SchemaError", (error) => new ServerArgsParseError({ cause: Cause.fail(error) })),
  );
  return yield* overrides.pipe(
    Option.filter((u): u is (args: unknown) => Effect.Effect<unknown, unknown> => Predicate.isFunction(u)),
    Option.match({
      onNone: () => transaction.execute(mutator.mutator(args)),
      onSome: (run) => run(args),
    }),
  );
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
