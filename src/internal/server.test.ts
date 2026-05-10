import { describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import { NoTransactionError } from "./server.js";
import * as ServerSynchronizationContext from "./server-synchronization-context.js";

describe("ServerSynchronizationContext", () => {
  it.effect(
    "finalize should return NoTransactionError when called without guard",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(
        ServerSynchronizationContext.finalize,
        Effect.provide(ServerSynchronizationContext.layer),
        Effect.exit,
      );
      if (Exit.isFailure(result)) {
        expect(Cause.squash(result.cause)).toBeInstanceOf(NoTransactionError);
      } else {
        expect.fail("Expected failure");
      }
    }),
  );

  it.effect(
    "finalize shouldn't return errors when called with guard",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(
        ServerSynchronizationContext.guard,
        ServerSynchronizationContext.finalize,
        Effect.provide(ServerSynchronizationContext.layer),
        Effect.exit,
      );
      if (Exit.isFailure(result)) {
        expect.fail("Expected success", result.cause);
      }
    }),
  );
});
