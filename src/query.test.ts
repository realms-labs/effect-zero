import { describe, expect, it } from "@effect/vitest";
import { createBuilder, createSchema, number, string, table } from "@rocicorp/zero";
import { queryInternalsTag } from "@rocicorp/zero/bindings";
import * as Effect from "effect/Effect";
import * as Equal from "effect/Equal";
import * as Hash from "effect/Hash";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Query from "./query.js";

describe("Query hashing", () => {
  // biome-ignore lint/suspicious/noExplicitAny: allowed in tests
  const makeMockQuery = (): any => {
    const mockQuery = {
      [queryInternalsTag]: true,
      format: { singular: false },
      nameAndArgs: () => mockQuery,
    };
    return mockQuery;
  };

  const makeTestQuery = (name: string) =>
    Query.make({
      name,
      payload: Schema.String,
      query: () => Effect.succeed(makeMockQuery()),
    });

  it.effect(
    "queries with the same name and arguments should have the same hash",
    Effect.fn(function* ({ expect }) {
      const testQuery = makeTestQuery("testQuery");

      const query1 = yield* testQuery("arg1");
      const query2 = yield* testQuery("arg1");

      expect(Hash.hash(query1)).toBe(Hash.hash(query2));
    }),
  );

  it.effect(
    "queries with different arguments should have different hashes",
    Effect.fn(function* ({ expect }) {
      const testQuery = makeTestQuery("testQuery");

      const query1 = yield* testQuery("arg1");
      const query2 = yield* testQuery("arg2");

      expect(Hash.hash(query1)).not.toBe(Hash.hash(query2));
    }),
  );

  it.effect(
    "queries with different names should have different hashes",
    Effect.fn(function* ({ expect }) {
      const testQuery1 = makeTestQuery("queryA");
      const testQuery2 = makeTestQuery("queryB");

      const query1 = yield* testQuery1("sameArg");
      const query2 = yield* testQuery2("sameArg");

      expect(Hash.hash(query1)).not.toBe(Hash.hash(query2));
    }),
  );

  it.effect(
    "Equal.equals should return true for queries with same name and arguments",
    Effect.fn(function* ({ expect }) {
      const testQuery = makeTestQuery("testQuery");

      const query1 = yield* testQuery("arg1");
      const query2 = yield* testQuery("arg1");

      expect(Equal.equals(query1, query2)).toBe(true);
    }),
  );

  it.effect(
    "Equal.equals should return false for queries with different arguments",
    Effect.fn(function* ({ expect }) {
      const testQuery = makeTestQuery("testQuery");

      const query1 = yield* testQuery("arg1");
      const query2 = yield* testQuery("arg2");

      expect(Equal.equals(query1, query2)).toBe(false);
    }),
  );

  it.effect(
    "Equal.equals should return false for queries with different names",
    Effect.fn(function* ({ expect }) {
      const testQuery1 = makeTestQuery("queryA");
      const testQuery2 = makeTestQuery("queryB");

      const query1 = yield* testQuery1("sameArg");
      const query2 = yield* testQuery2("sameArg");

      expect(Equal.equals(query1, query2)).toBe(false);
    }),
  );

  it.effect(
    "hash should be deterministic based on name and JSON-serialized args",
    Effect.fn(function* ({ expect }) {
      const testQuery = makeTestQuery("deterministicTest");

      // Create the same query multiple times
      const query1 = yield* testQuery("testArg");
      const query2 = yield* testQuery("testArg");
      const query3 = yield* testQuery("testArg");

      const hash1 = Hash.hash(query1);
      const hash2 = Hash.hash(query2);
      const hash3 = Hash.hash(query3);

      expect(hash1).toBe(hash2);
      expect(hash2).toBe(hash3);
    }),
  );

  it.effect(
    "hash should work with complex arguments",
    Effect.fn(function* ({ expect }) {
      const complexQuery = Query.make({
        name: "complexQuery",
        payload: Schema.Struct({ id: Schema.String, nested: Schema.Struct({ value: Schema.Number }) }),
        query: () => Effect.succeed(makeMockQuery()),
      });

      const query1 = yield* complexQuery({ id: "test", nested: { value: 42 } });
      const query2 = yield* complexQuery({ id: "test", nested: { value: 42 } });
      const query3 = yield* complexQuery({ id: "test", nested: { value: 43 } });

      expect(Hash.hash(query1)).toBe(Hash.hash(query2));
      expect(Hash.hash(query1)).not.toBe(Hash.hash(query3));
    }),
  );
});

describe("initialValue", () => {
  const issue = table("issue").columns({ id: string(), votes: number() }).primaryKey("id");
  const zql = createBuilder(createSchema({ tables: [issue] }));

  it("is an empty list, no row, or an aggregate over no rows", () => {
    expect(Query.initialValue(zql.issue)).toEqual({ _tag: "Partial", value: [], aggregate: Option.none() });
    expect(Query.initialValue(zql.issue.one())).toEqual({
      _tag: "Partial",
      value: undefined,
      aggregate: Option.none(),
    });
    expect(Query.initialValue(zql.issue.count())).toEqual({
      _tag: "Partial",
      value: 0,
      aggregate: Option.some({ fn: "count" }),
    });
    expect(Query.initialValue(zql.issue.max("votes"))).toEqual({
      _tag: "Partial",
      value: null,
      aggregate: Option.some({ fn: "max", field: "votes" }),
    });
  });
});
