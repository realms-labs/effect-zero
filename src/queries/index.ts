import type { AnyQuery, ReadonlyJSONValue } from "@rocicorp/zero";
import * as Console from "effect/Console";
import * as Effect from "effect/Effect";
import type * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ZeroClientProvider } from "../client.js";
import { prefixId } from "../utils.js";

export const queryDelegateSymbol = Symbol.for(prefixId("queryDelegate"));

export type Query<Q extends AnyQuery = AnyQuery> = Q & {
  [queryDelegateSymbol]: Option.Option<ZeroClientProvider["Type"]>;
};

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
    yield* Console.log("retrieving zero");
    const zero = yield* Effect.serviceOption(ZeroClientProvider);
    const parsed = yield* Schema.decode(options.payload)(args);
    return yield* options.query(...parsed).pipe(
      Effect.map((query) => {
        query = query.nameAndArgs(options.name, parsed) as Q;
        (query as Query<Q>)[queryDelegateSymbol] = zero;
        return query;
      }),
    );
  });
};
