import { describe, expectTypeOf, it } from "@effect/vitest";
import type { MutateRequest as ZeroMutateRequest } from "@rocicorp/zero";
import { queryInternalsTag } from "@rocicorp/zero/bindings";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaGetter from "effect/SchemaGetter";
import type * as Scope from "effect/Scope";
import * as Client from "./client.js";
import * as ClientTransaction from "./client-transaction.js";
import * as Mutator from "./mutator.js";
import * as Query from "./query.js";
import * as Server from "./server.js";
import * as ServerTransaction from "./server-transaction.js";

const DateFromNumber = Schema.Number.pipe(
  Schema.decodeTo(Schema.Date, {
    decode: SchemaGetter.transform((n: number) => new Date(n)),
    encode: SchemaGetter.transform((d: Date) => d.getTime()),
  }),
);

// Mock Zero schema for type tests
const mockZeroSchema = {
  tables: {
    messages: {
      tableName: "messages",
      columns: {
        id: { type: "string" },
        body: { type: "string" },
      },
      primaryKey: ["id"],
    },
  },
} as any;

// Mock database for type tests
const mockDatabase = {} as any;

describe("Type tests", () => {
  describe("Mutators", () => {
    class DummyTag extends Context.Service<DummyTag, {}>()("effect-zero/test/DummyTag") {}
    class DummyTag2 extends Context.Service<DummyTag2, {}>()("effect-zero/test/DummyTag2") {}
    class DummyError extends Data.TaggedError("DummyError") {}

    const clientTransaction = ClientTransaction.make("TestClientTransaction", mockZeroSchema);
    const serverTransaction = ServerTransaction.make("TestServerTransaction", mockDatabase, clientTransaction);
    type ClientTransactionId = (typeof clientTransaction)[typeof ClientTransaction.ContextSymbol]["Identifier"];

    const mutators = Mutator.registry(clientTransaction, {
      messages: {
        create: Mutator.make({
          payload: Schema.Struct({ id: Schema.String, body: Schema.String }),
          mutator: Effect.fn(function* (msg) {
            expectTypeOf(msg).toEqualTypeOf<{ readonly id: string; readonly body: string }>();
            yield* clientTransaction.use(async () => {});
          }),
        }),
        deleteAll: Mutator.make({
          payload: Schema.Void,
          mutator: Effect.fn(function* () {
            yield* DummyTag;
            yield* clientTransaction.use(async () => {});
          }),
        }),
      },
      transformArgs: Mutator.make({
        payload: Schema.Struct({ at: DateFromNumber }),
        mutator: Effect.fn(function* ({ at }) {
          expectTypeOf(at).toEqualTypeOf<Date>();
          yield* DummyTag2;
          return yield* new DummyError();
        }),
      }),
    });

    it("mutators take their decoded args and produce requests", () => {
      expectTypeOf<Parameters<typeof mutators.messages.create>>().toEqualTypeOf<
        [{ readonly id: string; readonly body: string }]
      >();
      // A mutator without args is called without args.
      mutators.messages.deleteAll();
      expectTypeOf<Parameters<typeof mutators.transformArgs>>().toEqualTypeOf<[{ readonly at: Date }]>();
      const nullable = Mutator.make({ payload: Schema.NullOr(Schema.String), mutator: () => Effect.void });
      expectTypeOf<Parameters<typeof nullable.mutator>>().toEqualTypeOf<[string | null]>();

      // A request is also a Zero `MutateRequest`, so `zero.mutate(request)` accepts it.
      expectTypeOf(mutators.messages.create({ id: "1", body: "a" })).toExtend<ZeroMutateRequest<any, any, any, any>>();
      expectTypeOf(mutators.messages.create.mutatorName).toEqualTypeOf<string>();
    });

    it("mutate's outcomes fail with Zero's error details", () => {
      const mutation = Client.mutate({} as any, mutators.transformArgs({ at: new Date() }));
      expectTypeOf<Effect.Success<typeof mutation>>().toExtend<Client.Mutation>();
      expectTypeOf<Effect.Error<Client.Mutation["client"]>>().toEqualTypeOf<Client.MutationError>();
      expectTypeOf<Effect.Error<Client.Mutation["server"]>>().toEqualTypeOf<Client.MutationError>();
      expectTypeOf<Effect.Services<typeof mutation>>().toEqualTypeOf<never>();
    });

    it("the registry's requirements propagate, without its client transaction", () => {
      expectTypeOf<Mutator.Requirements<typeof mutators>>().toEqualTypeOf<ClientTransactionId | DummyTag | DummyTag2>();

      const zero = Client.make(mutators, { userID: null, server: null });
      expectTypeOf<Effect.Services<typeof zero>>().toEqualTypeOf<Scope.Scope | DummyTag | DummyTag2>();
    });

    it("handleMutate requires the shared bodies' services when nothing is overridden", () => {
      const effect = Server.handleMutate({
        transaction: serverTransaction,
        mutators: Mutator.server(mutators, {}),
        query: {} as any,
        body: {} as any,
        userId: undefined,
      });
      expectTypeOf<Effect.Services<typeof effect>>().toEqualTypeOf<DummyTag | DummyTag2>();

      // A registry goes through `ZfxMutator.server` before `handleMutate` takes it.
      expectTypeOf(mutators).not.toExtend<Mutator.AnyServerMutators>();
    });

    it("overrides replace the requirements of the mutators they override", () => {
      const serverMutators = Mutator.server(mutators, {
        messages: {
          deleteAll: Effect.fn(function* () {}),
        },
        transformArgs: Effect.fn(function* ({ at }) {
          expectTypeOf(at).toEqualTypeOf<Date>();
          yield* mutators.transformArgs.mutator({ at }).pipe(serverTransaction.execute);
        }),
      });

      const effect = Server.handleMutate({
        transaction: serverTransaction,
        mutators: serverMutators,
        query: {} as any,
        body: {} as any,
        userId: undefined,
      });
      // `deleteAll`'s override drops DummyTag; `transformArgs` reuses its body through the server transaction.
      expectTypeOf<Effect.Services<typeof effect>>().toEqualTypeOf<DummyTag2>();
    });

    it("handleMutate has no extra requirements when mutators have none", () => {
      const simple = Mutator.registry(clientTransaction, {
        simple: Mutator.make({
          payload: Schema.Void,
          mutator: () => clientTransaction.use(async () => {}),
        }),
      });

      const effect = Server.handleMutate({
        transaction: serverTransaction,
        mutators: Mutator.server(simple, {}),
        query: {} as any,
        body: {} as any,
        userId: undefined,
      });
      expectTypeOf<Effect.Services<typeof effect>>().toEqualTypeOf<never>();
    });
  });

  describe("Query", () => {
    // biome-ignore lint/suspicious/noExplicitAny: allowed in tests
    const makeMockQuery = (): any => {
      const mockQuery = {
        [queryInternalsTag]: true,
        format: { singular: false },
        nameAndArgs: () => mockQuery,
      };
      return mockQuery;
    };

    it("query should accept and passthrough encoded non-JSON args", () => {
      const transformArgsQuery = Query.make({
        name: "transformArgs",
        payload: DateFromNumber,
        query: Effect.fn(function* (a) {
          // Inside the query, 'a' should be the decoded type (Date)
          expectTypeOf<typeof a>().toEqualTypeOf<Date>();
          return yield* Effect.succeed(makeMockQuery());
        }),
      });

      // Query function should accept decoded type (Date)
      expectTypeOf<Parameters<typeof transformArgsQuery>>().toEqualTypeOf<[Date]>();
    });

    it("query with string tuple should have correct parameter types", () => {
      const stringQuery = Query.make({
        name: "stringQuery",
        payload: Schema.String,
        query: Effect.fn(function* (s) {
          expectTypeOf<typeof s>().toEqualTypeOf<string>();
          return yield* Effect.succeed(makeMockQuery());
        }),
      });

      expectTypeOf<Parameters<typeof stringQuery>>().toEqualTypeOf<[string]>();
    });

    // Queries were changed to only support a single argument in 0.25
    //
    // it("query with multiple args should have correct parameter types", () => {
    //   const multiArgQuery = Query.make({
    //     name: "multiArgQuery",
    //     payload: Schema.Tuple(Schema.String, Schema.Number),
    //     query: Effect.fn(function* (s, n) {
    //       expectTypeOf<typeof s>().toEqualTypeOf<string>();
    //       expectTypeOf<typeof n>().toEqualTypeOf<number>();
    //       return yield* Effect.succeed(makeMockQuery());
    //     }),
    //   });

    //   expectTypeOf<Parameters<typeof multiArgQuery>>().toEqualTypeOf<[string, number]>();
    // });

    it("query with no args should have empty parameter types", () => {
      const noArgQuery = Query.make({
        name: "noArgQuery",
        payload: Schema.Void,
        query: Effect.fn(function* () {
          return yield* Effect.succeed(makeMockQuery());
        }),
      });

      // biome-ignore lint/suspicious/noConfusingVoidType: Necessary for type check
      expectTypeOf<Parameters<typeof noArgQuery>>().toEqualTypeOf<[arg?: void]>();
    });

    it("query with complex struct arg should have correct parameter types", () => {
      const complexQuery = Query.make({
        name: "complexQuery",
        payload: Schema.Struct({
          id: Schema.String,
          nested: Schema.Struct({ value: Schema.Number }),
        }),
        query: Effect.fn(function* (arg: { id: string; nested: { value: number } }) {
          expectTypeOf<typeof arg>().toEqualTypeOf<{ id: string; nested: { value: number } }>();
          return yield* Effect.succeed(makeMockQuery());
        }),
      });

      expectTypeOf<Parameters<typeof complexQuery>>().toEqualTypeOf<
        [{ readonly id: string; readonly nested: { readonly value: number } }]
      >();
    });

    it("MakeQueryResult should be satisfied by query results", () => {
      const query1 = Query.make({
        name: "q1",
        payload: Schema.Tuple([Schema.String]),
        query: () => Effect.succeed(makeMockQuery()),
      });

      const query2 = Query.make({
        name: "q2",
        payload: Schema.Tuple([]),
        query: () => Effect.succeed(makeMockQuery()),
      });

      // This should compile without error
      const queries = [query1, query2] satisfies Query.MakeQueryResult[];
      expectTypeOf(queries).toExtend<Query.MakeQueryResult[]>();
    });
  });
});
