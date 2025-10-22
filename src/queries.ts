import { type AnyQuery, type ReadonlyJSONValue, syncedQuery, type Schema as ZeroSchema } from "@rocicorp/zero";
import { handleGetQueriesRequest } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Option from "effect/Option";
import type * as ParseResult from "effect/ParseResult";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Runtime from "effect/Runtime";
import * as Schema from "effect/Schema";
import type { ZeroTransformRequestMessage } from "./types/queries.js";
import { prefixId } from "./utils.js";

export const makeQuery = <
  N extends string,
  A extends ReadonlyJSONValue[],
  B extends ReadonlyJSONValue[],
  Q extends AnyQuery,
  E,
  R1,
  R2,
>(options: {
  name: N;
  payload: Schema.Schema<readonly [...B], readonly [...A], R1>;
  query: (...args: NoInfer<B>) => Effect.Effect<Q, E, R2>;
}) => {
  return Effect.fn(function* (...args: A) {
    const parsed = yield* Schema.decode(options.payload)(args);
    return yield* options.query(...parsed).pipe(Effect.map((query) => query.nameAndArgs(options.name, parsed) as Q));
  });
};

export const handleGetQueries = Effect.fn(function* <E, R1, R2>(
  // biome-ignore lint/suspicious/noExplicitAny: accept any query
  queries: Record<string, ReturnType<typeof makeQuery<string, any, any, AnyQuery, E, R1, R2>>>,
  schema: ZeroSchema,
  payload: ZeroTransformRequestMessage,
) {
  const runtime = yield* Effect.runtime<R1 | R2>();
  return yield* Effect.tryPromise({
    try: () =>
      handleGetQueriesRequest(
        (name, args) =>
          Rec.get(queries, name).pipe(
            Effect.catchTag("NoSuchElementException", () => new QueryNotFound()),
            Effect.flatMap((query) => query(...args)),
            Effect.map((query) => ({ query })),
            (effect) => Runtime.runPromiseExit(runtime, effect),
            (exit) =>
              exit.then(
                Exit.getOrElse((cause) => {
                  throw new QueryUserError<E | ParseResult.ParseError | QueryNotFound>({ cause });
                }),
              ),
          ),
        schema,
        payload as ReadonlyJSONValue,
      ),
    catch: (e) =>
      Option.liftPredicate(e, QueryUserError.is<E>).pipe(
        Option.flatMap((e) => Cause.failureOption(e.cause)),
        Option.getOrElse(() => new GetQueriesError({ cause: Cause.fail(e) })),
      ),
  });
});

class QueryNotFound extends Data.TaggedError("QueryNotFound") {}

const QueryUserErrorTypeId = Symbol.for(prefixId("QueryUserError"));
class QueryUserError<E> extends Data.TaggedError("QueryUserError")<{
  cause: Cause.Cause<E | ParseResult.ParseError | QueryNotFound>;
}> {
  readonly [QueryUserErrorTypeId] = QueryUserErrorTypeId;
  static is<E>(e: unknown): e is QueryUserError<E> {
    return Predicate.hasProperty(e, QueryUserErrorTypeId);
  }
}

class GetQueriesError extends Data.TaggedError("GetQueriesError")<{ cause: Cause.Cause<unknown> }> {}
