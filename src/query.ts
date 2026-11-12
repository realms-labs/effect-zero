import type {
  AnyQuery,
  HumanReadable,
  ReadonlyJSONValue,
  Zero,
  Query as ZeroQuery,
  Schema as ZeroSchema,
} from "@rocicorp/zero";
import { asQueryInternals, emptyViewValue } from "@rocicorp/zero/bindings";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Exit from "effect/Exit";
import * as Hash from "effect/Hash";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Queue from "effect/Queue";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";
import * as QueryResult from "./internal/query-result.js";
import { prefixId } from "./internal/utils.js";
import { QueryError } from "./result.js";
import { getSnapshot } from "./snapshot.js";

// biome-ignore lint/suspicious/noConfusingVoidType: necessary to allow Schema.Void
type QueryArgs<A extends ReadonlyJSONValue | void, B> = { _tag: "Encoded"; args: A } | { _tag: "Decoded"; args: B };

export const RunQuerySymbol = Symbol.for(prefixId("RunQuery"));
export const QueryNameSymbol = Symbol.for(prefixId("QueryName"));

export const make = <
  N extends string,
  // The encoded format that is sent over the wire, must conform to JSON
  // biome-ignore lint/suspicious/noConfusingVoidType: necessary to allow Schema.Void
  A extends ReadonlyJSONValue | void,
  // The decoded format, can be anything
  B,
  T extends keyof S["tables"] & string,
  S extends ZeroSchema,
  R,
  E,
  R1,
  R2,
>(options: {
  name: N;
  payload: Schema.Codec<B, A, R1, R1>;
  query: (args: NoInfer<B>) => Effect.Effect<ZeroQuery<T, S, R>, E, R2>;
}) => {
  // Typed as the plain Zero query, without its `Equal`: TypeScript doesn't check a singular or aggregate query
  // intersected with `Equal` as Zero's `AnyQuery`, which `MakeQueryResult` takes.
  const runQuery = Effect.fn(function* (
    args: QueryArgs<A, B>,
  ): Effect.fn.Return<ZeroQuery<T, S, R>, Schema.SchemaError | E, R1 | R2> {
    const { encoded, decoded } = yield* Match.valueTags(args, {
      Encoded: ({ args: encoded }) =>
        Effect.map(Schema.decodeEffect(options.payload)(encoded), (decoded) => ({ encoded, decoded })),
      Decoded: ({ args: decoded }) =>
        Effect.map(Schema.encodeEffect(options.payload)(decoded), (encoded) => ({ encoded, decoded })),
    });

    return yield* options.query(decoded).pipe(
      Effect.map((rawQuery) => {
        const query = asQueryInternals(rawQuery).nameAndArgs(
          options.name,
          // We pass the encoded `args` to the `nameAndArgs` method, as that is the wire format which
          // is sent to the server.
          encoded !== undefined ? [encoded] : [],
        ) as ZeroQuery<T, S, R> & Equal.Equal;
        // Adapted from https://github.com/rocicorp/mono/blob/17171f975e61f7ec93c61569da7bda1d962ac962/packages/zero-protocol/src/query-hash.ts#L17
        query[Hash.symbol] = () => Hash.string(`${options.name}:${JSON.stringify(encoded)}`);
        query[Equal.symbol] = function (that) {
          if (Hash.isHash(that)) {
            return Equal.equals(this[Hash.symbol](), that[Hash.symbol]());
          }
          return false;
        };
        return query;
      }),
    );
  });

  // We return a callable object for ease of use on the frontend.
  return Object.assign((args: B) => runQuery({ _tag: "Decoded", args }), {
    [QueryNameSymbol]: options.name,
    // Internally, we include the full `runQuery` function for use in library's server logic.
    [RunQuerySymbol]: runQuery,
  });
};

/**
 * Any query `make` returns, whatever its result: rows, a row, or an aggregate. `E` and `R` are what building it can
 * fail with and needs.
 */
// biome-ignore lint/suspicious/noExplicitAny: accept any query
export interface MakeQueryResult<E = any, R = any> {
  readonly [QueryNameSymbol]: string;
  // biome-ignore lint/suspicious/noExplicitAny: accept any query's arguments
  readonly [RunQuerySymbol]: (args: QueryArgs<any, any>) => Effect.Effect<AnyQuery, E, R>;
}

/**
 * Streams a query's results. Each subscription is traced: a `zero.query` span runs from subscribing until the query's
 * first complete result, which for rows this client hasn't synced yet is zero-cache's first sync of the query. Its
 * `zero.query.first_data` event marks the first result with rows (or the first cached or complete result, if that
 * comes first), which may come from the client's local store before the server confirms it. Zero doesn't compute a
 * top-level aggregate locally, so its first data is its first cached or complete result. An error result fails
 * the span with its `QueryError`, and a subscriber that stops first interrupts it.
 *
 * Results are Zero's own objects, shared with its view, so don't mutate them. Rows that didn't change keep their
 * identity from one result to the next. Like `useQuery`'s, rows carry Zero's symbol-keyed bookkeeping, which deep
 * equality (`Equal.equals`, vitest's `toEqual`) compares.
 */
export const stream = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  zero: Zero<S>,
  query: ZeroQuery<T, S, R>,
) =>
  Effect.gen(function* () {
    const internals = asQueryInternals(query);
    const { aggregate } = internals.format;
    // Closed by the first complete or error result, or as interrupted when the stream ends first.
    const spanScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
      Scope.close(scope, Exit.isSuccess(exit) ? Exit.interrupt() : exit),
    );
    const span = yield* Effect.makeSpanScoped("zero.query", {
      attributes: {
        "zero.query.hash": internals.hash(),
        "zero.clientID": zero.clientID,
        ...Rec.getSomes({
          "zero.query.name": Option.fromNullishOr(internals.customQueryID?.name),
          "zero.query.aggregate": Option.fromNullishOr(aggregate?.fn),
        }),
      },
    }).pipe(Scope.provide(spanScope));
    const view = yield* Effect.acquireRelease(
      Effect.sync(() => zero.materialize(query)),
      (view) => Effect.sync(() => view.destroy()),
    );

    return Stream.callback<Parameters<Parameters<(typeof view)["addListener"]>[0]>>((queue) =>
      Effect.sync(() => view.addListener((...args) => Queue.offerUnsafe(queue, args))),
    ).pipe(
      Stream.map(([data, resultType, error]) =>
        // logic here borrowed from: https://github.com/rocicorp/mono/blob/d489a3218ad603b3fd65e33cec76ae77cea7c7af/packages/zero-react/src/use-query.tsx
        // A view's data is immutable once its listeners have it (each change copies what it touches), so it isn't
        // copied here: rows that didn't change keep their identity from one result to the next.
        getSnapshot<R>(
          internals.format.singular,
          data as HumanReadable<R>,
          resultType,
          // TODO: Look into this. It seems like this is difficult but not impossible to model in the Effect paradigm.
          // Likely need some kind of stateful piece between the publisher and and subscriber, allowing the subscriber to swap out
          // the publisher when this function is called.
          () => Effect.die("retry not available in `effect-zero`"),
          error,
        ),
      ),
      Stream.map((snapshot) => QueryResult.make(snapshot, aggregate)),
      // Records `first_data` once: at the first result with rows, or the first confirmed (cached or complete) result if
      // that comes first. The state is whether that's settled, which an error result (ending the span) also does.
      Stream.mapAccumEffect(
        () => false,
        Effect.fnUntraced(function* (settled, result) {
          if (settled || Predicate.isTagged(result, "Error")) return [true, [result]] as const;
          const rows = QueryResult.rowCount(result.value);
          if (Predicate.isTagged(result, "Partial") && (aggregate !== undefined || rows === 0)) {
            return [false, [result]] as const;
          }
          span.event("zero.query.first_data", yield* Clock.currentTimeNanos, {
            "zero.query.result_type": Match.valueTags(result, {
              Partial: () => "unknown",
              Cached: () => "cached",
              Complete: () => "complete",
            }),
            // An aggregate's result is a value, not rows.
            ...(aggregate === undefined ? { "zero.query.rows": rows } : {}),
          });
          return [true, [result]] as const;
        }),
      ),
      Stream.tap(
        Match.valueTags({
          Partial: () => Effect.void,
          Cached: () => Effect.void,
          Complete: () => Scope.close(spanScope, Exit.void),
          Error: ({ details }) => Scope.close(spanScope, Exit.fail(new QueryError({ details }))),
        }),
      ),
    );
  }).pipe(Stream.unwrap);

/**
 * What a query shows before its first result: an empty list, no row, or a top-level aggregate over no rows (`0` for
 * `count()`, `null` for the others), as a `Partial` result.
 */
export const initialValue = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  query: ZeroQuery<T, S, R>,
) => {
  const { format } = asQueryInternals(query);
  // `emptyViewValue` takes any format, so TypeScript can't tie its value to `R`.
  const value = emptyViewValue(format) as unknown as HumanReadable<R>;
  return QueryResult.make<R>([value, { type: "unknown" }], format.aggregate);
};
