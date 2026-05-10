import { describe, it } from "@effect/vitest";
import { assertExitFailure } from "@effect/vitest/utils";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { finalize, guard, NoTransactionError, ServerSynchronizationContext } from "./server.js";

describe("ServerSynchronizationContext", () => {
  it.effect(
    "finalize should return NoTransactionError when called without guard",
    Effect.fn(function* () {
      const result = yield* Effect.void.pipe(
        finalize,
        Effect.provide(ServerSynchronizationContext.layer),
        Effect.exit,
      );
      assertExitFailure(result, Cause.fail(new NoTransactionError()));
    }),
  );

  it.effect(
    "finalize shouldn't return errors when called with guard",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(
        guard,
        finalize,
        Effect.provide(ServerSynchronizationContext.layer),
        Effect.exit,
      );
      if (Exit.isFailure(result)) {
        expect.fail("Expected success", result.cause);
      }
    }),
  );
});
