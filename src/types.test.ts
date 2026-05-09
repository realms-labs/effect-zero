import { describe, expectTypeOf, it } from "@effect/vitest";
import { queryInternalsTag } from "@rocicorp/zero/bindings";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as ClientTransaction from "./client-transaction.js";
import * as Mutators from "./mutators.js";
import * as Query from "./query.js";
import * as Server from "./server.js";
import * as ServerTransaction from "./server-transaction.js";

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
    it("mutators should have correct argument types", () => {
      const mutatorSchema = Mutators.schema({
        messages: {
          create: Schema.Struct({
            id: Schema.String,
            body: Schema.String,
          }),
        },
        optionalVoidArg: Schema.Void,
        noArgs: Schema.Void,
        transformArgs: Schema.Struct({
          foo: Schema.NumberFromString,
        }),
      });

      const clientTransaction = ClientTransaction.make("TestClientTransaction", mockZeroSchema);

      const mutators = Mutators.make(mutatorSchema, {
        messages: {
          create: (msg) =>
            clientTransaction.use(async () => {
              // Use msg to verify type
              const _id: string = msg.id;
              const _body: string = msg.body;
            }),
        },
        optionalVoidArg: Effect.fn(function* (_a) {}),
        noArgs: Effect.fn(function* () {}),
        transformArgs: Effect.fn(function* (_a) {}),
      });

      // Test void/undefined optional parameter
      expectTypeOf<Parameters<typeof mutators.optionalVoidArg>>().toEqualTypeOf<[(void | undefined)?]>();

      // Test no-arg mutator
      expectTypeOf<Parameters<typeof mutators.noArgs>>().toEqualTypeOf<[]>();

      // Test nested mutator with struct schema
      expectTypeOf<Parameters<typeof mutators.messages.create>>().toEqualTypeOf<
        [Schema.Schema.Type<typeof mutatorSchema.messages.create>]
      >();

      // Test transformed args - mutator receives decoded (Type) version
      expectTypeOf<Parameters<typeof mutators.transformArgs>>().toEqualTypeOf<
        [Schema.Schema.Type<typeof mutatorSchema.transformArgs>]
      >();
    });

    it("nested mutators using Effect.fn should have correct argument types", () => {
      const mutatorSchema = Mutators.schema({
        todo: {
          create: Schema.Struct({
            id: Schema.String,
            title: Schema.String,
          }),
          toggle: Schema.Struct({
            id: Schema.String,
            done: Schema.Boolean,
          }),
          delete: Schema.Void,
        },
      });

      const clientTransaction = ClientTransaction.make("TestClientTransactionNested", mockZeroSchema);

      // Note: When using Effect.fn with destructuring, TypeScript infers the parameter as `any`.
      // The runtime still correctly validates/transforms args via the schema.
      // To get proper type inference, use explicit parameter types or regular arrow functions.
      const mutators = Mutators.make(mutatorSchema, {
        todo: {
          // Using arrow function with explicit type for best inference
          create: (args: { id: string; title: string }) =>
            clientTransaction.use(async () => {
              const _id: string = args.id;
              const _title: string = args.title;
            }),
          // Using Effect.fn with explicit type annotation
          toggle: Effect.fn(function* (args: { id: string; done: boolean }) {
            yield* clientTransaction.use(async () => {});
          }),
          // Using Effect.fn with no args (void schema)
          delete: Effect.fn(function* () {
            yield* clientTransaction.use(async () => {});
          }),
        },
      });

      // Test nested mutator with arrow function - preserves type
      expectTypeOf<Parameters<typeof mutators.todo.create>>().toEqualTypeOf<[{ id: string; title: string }]>();

      // Test nested mutator with Effect.fn and explicit type annotation - preserves type
      expectTypeOf<Parameters<typeof mutators.todo.toggle>>().toEqualTypeOf<[{ id: string; done: boolean }]>();

      // Test nested mutator with void schema - delete
      expectTypeOf<Parameters<typeof mutators.todo.delete>>().toEqualTypeOf<[]>();
    });

    it("nested mutator requirements should propagate", () => {
      class NestedDummyTag extends Effect.Service<NestedDummyTag>()("effect-zero/test/NestedDummyTag", {
        succeed: {},
      }) {}

      class NestedDummyTag2 extends Effect.Service<NestedDummyTag2>()("effect-zero/test/NestedDummyTag2", {
        succeed: {},
      }) {}

      const clientTransaction = ClientTransaction.make("TestClientTransactionNestedReq", mockZeroSchema);
      const serverTransaction = ServerTransaction.make(
        "TestServerTransactionNestedReq",
        mockDatabase,
        clientTransaction,
      );

      const mutatorSchema = Mutators.schema({
        nested: {
          withTag1: Schema.Void,
          withTag2: Schema.Void,
          noTags: Schema.Void,
        },
      });

      const mutators = Mutators.make(mutatorSchema, {
        nested: {
          withTag1: Effect.fn(function* () {
            yield* NestedDummyTag;
          }),
          withTag2: Effect.fn(function* () {
            yield* NestedDummyTag2;
          }),
          noTags: Effect.fn(function* () {}),
        },
      });

      const serverEffect = Server.processPush(serverTransaction, mutators, {} as any, {} as any);

      // Server effect should require both NestedDummyTag and NestedDummyTag2
      expectTypeOf<Effect.Services<typeof serverEffect>>().toEqualTypeOf<NestedDummyTag | NestedDummyTag2>();
    });

    it("mutator requirements should propagate", () => {
      class DummyTag extends Effect.Service<DummyTag>()("effect-zero/test/DummyTag", {
        succeed: {},
      }) {}

      class DummyTag2 extends Effect.Service<DummyTag2>()("effect-zero/test/DummyTag2", {
        succeed: {},
      }) {}

      const clientTransaction = ClientTransaction.make("TestClientTransaction2", mockZeroSchema);
      const serverTransaction = ServerTransaction.make("TestServerTransaction", mockDatabase, clientTransaction);

      const mutatorSchema = Mutators.schema({
        dummy: Schema.Void,
        dummy2: Schema.Void,
      });

      const mutators = Mutators.make(mutatorSchema, {
        dummy: Effect.fn(function* () {
          yield* DummyTag;
        }),
        dummy2: Effect.fn(function* () {
          yield* DummyTag2;
        }),
      });

      const serverEffect = Server.processPush(serverTransaction, mutators, {} as any, {} as any);

      // Server effect should require both DummyTag and DummyTag2
      expectTypeOf<Effect.Services<typeof serverEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
    });

    it("processPush has no extra requirements when mutators have none", () => {
      const clientTransaction = ClientTransaction.make("TestClientTransaction3", mockZeroSchema);
      const serverTransaction = ServerTransaction.make("TestServerTransaction2", mockDatabase, clientTransaction);

      const mutatorSchema = Mutators.schema({
        simple: Schema.Void,
      });

      const mutators = Mutators.make(mutatorSchema, {
        simple: Effect.fn(function* () {}),
      });

      const effect = Server.processPush(serverTransaction, mutators, {} as any, {} as any);

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
        payload: Schema.DateFromNumber,
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
