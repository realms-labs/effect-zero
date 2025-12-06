import { describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Mutators from "../mutators.js";
import { processPush } from "../server.js";
import { NoTransactionError, ServerSynchronizationContext } from "./server.js";

describe("ServerSynchronizationContext", () => {
  it.effect(
    "finalize should return NoTransactionError when called without guard",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(
        ServerSynchronizationContext.finalize,
        Effect.provide(ServerSynchronizationContext.Default),
        Effect.exit,
      );
      Exit.match(result, {
        onSuccess: () => expect.fail("Expected failure"),
        onFailure: (cause) => expect(cause).toStrictEqual(Cause.fail(new NoTransactionError())),
      });
    }),
  );

  it.effect(
    "finalize shouldn't return errors when called with guard",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(
        ServerSynchronizationContext.guard,
        ServerSynchronizationContext.finalize,
        Effect.provide(ServerSynchronizationContext.Default),
        Effect.exit,
      );
      if (Exit.isFailure(result)) {
        expect.fail("Expected success", result.cause);
      }
    }),
  );

  it.effect(
    "multiple mutations in a push should each get their own context",
    Effect.fn(function* ({ expect }) {
      const completedCount = yield* Ref.make(0);

      const mutatorSchema = Mutators.schema({
        test: {
          mutation1: Schema.Struct({ value: Schema.String }),
          mutation2: Schema.Struct({ value: Schema.String }),
        },
      });

      const makeMutator = (_args: { value: string }) =>
        Effect.void.pipe(
          ServerSynchronizationContext.guard,
          Effect.tap(() => Ref.update(completedCount, (n) => n + 1)),
        );

      const mutators = Mutators.make(mutatorSchema, {
        test: { mutation1: makeMutator, mutation2: makeMutator },
      });

      const mockTransaction = { execute: <A, E, R>(effect: Effect.Effect<A, E, R>) => effect } as Parameters<
        typeof processPush
      >[0];

      const result = yield* processPush(
        mockTransaction,
        mutators,
        { schema: "test", appID: "test" },
        {
          pushVersion: 1,
          clientGroupID: "cg",
          mutations: [
            { type: "custom", id: 1, clientID: "c", name: "test|mutation1", args: [{ value: "a" }], timestamp: 0 },
            { type: "custom", id: 2, clientID: "c", name: "test|mutation2", args: [{ value: "b" }], timestamp: 0 },
          ],
          requestID: "r",
          timestamp: 0,
        },
      );

      expect(yield* Ref.get(completedCount)).toBe(2);
      if ("mutations" in result && result.mutations) {
        expect(result.mutations).toHaveLength(2);
        for (const r of result.mutations) expect(r.result).not.toHaveProperty("error");
      }
    }),
  );
});
