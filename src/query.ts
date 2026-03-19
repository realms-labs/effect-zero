import type { HumanReadable, ReadonlyJSONValue, Zero, Query as ZeroQuery, Schema as ZeroSchema } from "@rocicorp/zero";
import { asQueryInternals } from "@rocicorp/zero/bindings";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Hash from "effect/Hash";
import * as Match from "effect/Match";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Subscribable from "effect/Subscribable";
import * as QueryResult from "./internal/query-result.js";
import { prefixId } from "./internal/utils.js";
import { deepClone, getDefaultSnapshot, getSnapshot } from "./snapshot.js";

type QueryArgs<
  A extends ReadonlyJSONValue[],
  B extends unknown[],
> = // biome-ignore lint/suspicious/noExplicitAny: handles "any query" case
| { _tag: "Encoded"; args: any extends A ? any : readonly [...A] }
// biome-ignore lint/suspicious/noExplicitAny: handles "any query" case
| { _tag: "Decoded"; args: any extends B ? any : readonly [...B] };

export const RunQuerySymbol = Symbol.for(prefixId("RunQuery"));
export const QueryNameSymbol = Symbol.for(prefixId("QueryName"));

export const make = <
  N extends string,
  // The encoded format that is sent over the wire, must conform to JSON
  A extends ReadonlyJSONValue[],
  // The decoded format, can be anything
  B extends unknown[],
  T extends keyof S["tables"] & string,
  S extends ZeroSchema,
  R,
  E,
  R1,
  R2,
>(options: {
  name: N;
  payload: Schema.Schema<readonly [...B], readonly [...A], R1>;
  query: (...args: NoInfer<B>) => Effect.Effect<ZeroQuery<T, S, R>, E, R2>;
}) => {
  const runQuery = Effect.fn(function* (args: QueryArgs<A, B>) {
    const { encoded, decoded } = yield* Match.valueTags(args, {
      Encoded: ({ args: encoded }) =>
        Effect.map(Schema.decode(options.payload)(encoded), (decoded) => ({ encoded, decoded })),
      Decoded: ({ args: decoded }) =>
        Effect.map(Schema.encode(options.payload)(decoded), (encoded) => ({ encoded, decoded })),
    });

    return yield* options.query(...decoded).pipe(
      Effect.map((rawQuery) => {
        const query = asQueryInternals(rawQuery).nameAndArgs(
          options.name,
          // We pass the encoded `args` to the `nameAndArgs` method, as that is the wire format which
          // is sent to the server.
          encoded,
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
  return Object.assign((...args: B) => runQuery({ _tag: "Decoded", args }), {
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

export const stream = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  zero: Zero<S>,
  query: ZeroQuery<T, S, R>,
) =>
  Effect.gen(function* () {
    const view = yield* Effect.acquireRelease(
      Effect.sync(() => zero.materialize(query)),
      (view) => Effect.sync(() => view.destroy()),
    );

    return Stream.asyncEffect<Parameters<Parameters<(typeof view)["addListener"]>[0]>>((emit) =>
      Effect.sync(() => view.addListener((...args) => emit.single(args))),
    ).pipe(
      Stream.mapEffect(([data, resultType]) =>
        Effect.sync(() => {
          // logic here borrowed from: https://github.com/rocicorp/mono/blob/288b00ec94f5a9ae6e988513423af25c281dbb2a/packages/zero-react/src/use-query.tsx#L295
          const cloned = data === undefined ? data : (deepClone(data as ReadonlyJSONValue) as HumanReadable<R>);
          return getSnapshot<R>(asQueryInternals(query).format.singular, cloned, resultType);
        }),
      ),
      Stream.map(QueryResult.make),
    );
  }).pipe(Stream.unwrapScoped);

export const initialValue = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  query: ZeroQuery<T, S, R>,
) => QueryResult.make<R>(getDefaultSnapshot(asQueryInternals(query).format.singular));

export const subscribable = <T extends keyof S["tables"] & string, S extends ZeroSchema, R>(
  zero: Zero<S>,
  query: ZeroQuery<T, S, R>,
) => {
  return Subscribable.make({
    get: Effect.promise(() => zero.run(query, { type: "unknown" })).pipe(
      Effect.map((value) => ({ _tag: "Partial", value }) satisfies QueryResult.QueryResult.Partial<R>),
    ),
    changes: stream(zero, query),
  });
};
