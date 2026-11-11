import { describe, expect, it } from "@effect/vitest";
import type { QueryResult as ZeroQueryResult } from "@rocicorp/zero/react";
import * as Cause from "effect/Cause";
import * as Option from "effect/Option";
import * as AsyncResult from "effect/reactivity/AsyncResult";
import * as QueryResult from "./internal/query-result.js";
import * as Result from "./result.js";

type Row = { readonly id: string };
const row = { id: "a" };

// Results as Zero reports them: [data, details].
const zeroResult = <A>(value: A, type: "unknown" | "cached" | "complete") =>
  Result.make<A, never>(AsyncResult.success(QueryResult.make([value, { type }] as unknown as ZeroQueryResult<A>)));

// What a mode makes of a result: its value, or "Initial" while it waits.
const accepted = <A>(result: Result.Result<A>, mode: Result.AcceptPartialMode) =>
  AsyncResult.match(Result.acceptPartialMode(result, mode), {
    onInitial: () => "Initial" as const,
    onSuccess: (success) => success.value,
    onFailure: () => "Failure" as const,
  });

describe("Result", () => {
  it("maps Zero's result types to tags", () => {
    expect(zeroResult([row], "unknown")).toMatchObject({ value: { _tag: "Partial", value: [row] } });
    expect(zeroResult([row], "cached")).toMatchObject({ value: { _tag: "Cached", value: [row] } });
    expect(zeroResult([row], "complete")).toMatchObject({ value: { _tag: "Complete", value: [row] } });
  });

  it("match calls the handler for each state", () => {
    const match = Result.match({
      onInitial: () => "initial",
      onFailure: () => "failure",
      onPartial: () => "partial",
      onCached: () => "cached",
      onComplete: () => "complete",
    });
    expect(match(AsyncResult.initial())).toBe("initial");
    expect(match(AsyncResult.failure(Cause.fail(new Error("outside the query"))))).toBe("failure");
    expect(match(zeroResult([], "unknown"))).toBe("partial");
    expect(match(zeroResult([], "cached"))).toBe("cached");
    expect(match(zeroResult([], "complete"))).toBe("complete");
  });

  // What each mode accepts, as `acceptPartialMode`'s docs describe.
  describe.each([
    {
      result: "Partial with rows",
      list: zeroResult([row], "unknown"),
      singular: zeroResult<Row | undefined>(row, "unknown"),
      acceptLocal: { list: [row], singular: row },
      acceptConfirmed: { list: [row], singular: row },
      waitForServer: { list: "Initial", singular: "Initial" },
    },
    {
      result: "Partial with no rows",
      list: zeroResult([], "unknown"),
      singular: zeroResult<Row | undefined>(undefined, "unknown"),
      acceptLocal: { list: [], singular: "Initial" },
      acceptConfirmed: { list: "Initial", singular: "Initial" },
      waitForServer: { list: "Initial", singular: "Initial" },
    },
    {
      result: "Cached with rows",
      list: zeroResult([row], "cached"),
      singular: zeroResult<Row | undefined>(row, "cached"),
      acceptLocal: { list: [row], singular: row },
      acceptConfirmed: { list: [row], singular: row },
      waitForServer: { list: "Initial", singular: "Initial" },
    },
    {
      result: "Cached with no rows",
      list: zeroResult([], "cached"),
      singular: zeroResult<Row | undefined>(undefined, "cached"),
      acceptLocal: { list: [], singular: undefined },
      acceptConfirmed: { list: [], singular: undefined },
      waitForServer: { list: "Initial", singular: "Initial" },
    },
    {
      result: "Complete with no rows",
      list: zeroResult([], "complete"),
      singular: zeroResult<Row | undefined>(undefined, "complete"),
      acceptLocal: { list: [], singular: undefined },
      acceptConfirmed: { list: [], singular: undefined },
      waitForServer: { list: [], singular: undefined },
    },
  ] as const)("acceptPartialMode on $result", (cases) => {
    it.each(["acceptLocal", "acceptConfirmed", "waitForServer"] as const)("%s", (mode) => {
      expect(accepted(cases.list, mode)).toEqual(cases[mode].list);
      expect(accepted(cases.singular, mode)).toEqual(cases[mode].singular);
    });
  });

  it("a failure keeps its previous success only if the mode accepts it", () => {
    const failure = (previous: Result.Result<readonly Row[], Error>) =>
      AsyncResult.failureWithPrevious(Cause.fail(new Error("outside the query")), { previous: Option.some(previous) });
    const previousValue = (previous: Result.Result<readonly Row[], Error>) =>
      AsyncResult.value(Result.acceptPartialMode(failure(previous), "acceptConfirmed"));
    expect(previousValue(zeroResult([], "unknown"))).toEqual(Option.none());
    expect(previousValue(zeroResult([row], "unknown"))).toEqual(Option.some([row]));
  });
});
