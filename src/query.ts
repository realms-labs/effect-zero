import type { HumanReadable, ReadonlyJSONValue, Zero, Query as ZeroQuery, Schema as ZeroSchema } from "@rocicorp/zero";
import { asQueryInternals } from "@rocicorp/zero/bindings";
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
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot.js";

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
  const runQuery = Effect.fn(function* (args: QueryArgs<A, B>) {
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

// biome-ignore lint/suspicious/noExplicitAny: accept any query
export type MakeQueryResult<E = any, R1 = any, R2 = any> = ReturnType<
  // biome-ignore lint/suspicious/noExplicitAny: accept any query
  typeof make<string, any, any, string, ZeroSchema, Record<string, any> | undefined, E, R1, R2>
>;

/**
 * Streams a query's results. Each subscription is traced: a `zero.query` span runs from subscribing until the query's
 * first complete result, which for rows this client hasn't synced yet is zero-cache's first sync of the query. Its
 * `zero.query.first_data` event marks the first result with rows (or the complete result, if that comes first), which
 * may come from the client's local store before the server confirms it. An error result fails the span with its
 * `QueryError`, and a subscriber that stops first interrupts it.
 */
export const stream = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  zero: Zero<S>,
  query: ZeroQuery<T, S, R>,
) =>
  Effect.gen(function* () {
    const internals = asQueryInternals(query);
    // Closed by the first complete or error result, or as interrupted when the stream ends first.
    const spanScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
      Scope.close(scope, Exit.isSuccess(exit) ? Exit.interrupt() : exit),
    );
    const span = yield* Effect.makeSpanScoped("zero.query", {
      attributes: {
        "zero.query.hash": internals.hash(),
        "zero.clientID": zero.clientID,
        ...Rec.getSomes({ "zero.query.name": Option.fromNullishOr(internals.customQueryID?.name) }),
      },
    }).pipe(Scope.provide(spanScope));
    const view = yield* Effect.acquireRelease(
      Effect.sync(() => zero.materialize(query)),
      (view) => Effect.sync(() => view.destroy()),
    );

    return Stream.callback<Parameters<Parameters<(typeof view)["addListener"]>[0]>>((queue) =>
      Effect.sync(() => view.addListener((...args) => Queue.offerUnsafe(queue, args))),
    ).pipe(
      Stream.mapEffect(([data, resultType, error]) =>
        Effect.sync(() => {
          // logic here borrowed from: https://github.com/rocicorp/mono/blob/8e0f600fb3a9185facf60cfd4971d260b266690e/packages/zero-react/src/use-query.tsx#L543
          const cloned = data === undefined ? data : (deepClone(data as ReadonlyJSONValue) as HumanReadable<R>);
          return getSnapshot<R>(
            internals.format.singular,
            cloned,
            resultType,
            // TODO: Look into this. It seems like this is difficult but not impossible to model in the Effect paradigm.
            // Likely need some kind of stateful piece between the publisher and and subscriber, allowing the subscriber to swap out
            // the publisher when this function is called.
            () => Effect.die("retry not available in `effect-zero`"),
            error,
          );
        }),
      ),
      Stream.map(QueryResult.make),
      // Records `first_data` once: at the first result with rows, or the complete result if that comes first. The
      // state is whether that's settled, which an error result (ending the span) also does.
      Stream.mapAccumEffect(
        () => false,
        Effect.fnUntraced(function* (settled, result) {
          if (settled || Predicate.isTagged(result, "Error")) return [true, [result]] as const;
          // A list's length, or 0 or 1 for a singular query's optional row.
          const rows = Array.isArray(result.value) ? result.value.length : Predicate.isUndefined(result.value) ? 0 : 1;
          if (Predicate.isTagged(result, "Partial") && rows === 0) return [false, [result]] as const;
          span.event("zero.query.first_data", yield* Clock.currentTimeNanos, {
            "zero.query.result_type": Match.valueTags(result, { Partial: () => "unknown", Complete: () => "complete" }),
            "zero.query.rows": rows,
          });
          return [true, [result]] as const;
        }),
      ),
      Stream.tap(
        Match.valueTags({
          Partial: () => Effect.void,
          Complete: () => Scope.close(spanScope, Exit.void),
          Error: ({ details }) => Scope.close(spanScope, Exit.fail(new QueryError({ details }))),
        }),
      ),
    );
  }).pipe(Stream.unwrap);

export const initialValue = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  query: ZeroQuery<T, S, R>,
) => QueryResult.make<R>(getDefaultSnapshot(asQueryInternals(query).format.singular));
