import { describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
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
});
