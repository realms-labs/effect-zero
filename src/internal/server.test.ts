import { describe, it } from "@effect/vitest";
import { assertExitFailure } from "@effect/vitest/utils";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SynchronizedRef from "effect/SynchronizedRef";
import { NoTransactionError } from "./server.js";
import { finalize, guard } from "./server-synchronization-context.js";

// A minimal stand-in for the per-mutation `Mutation` service, carrying just the synchronization state
// that `guard`/`finalize` read.
class Sync extends Context.Service<Sync, { readonly executed: SynchronizedRef.SynchronizedRef<boolean> }>()(
  "effect-zero/test/Sync",
) {}
const layer = Layer.effect(Sync)(
  Effect.gen(function* () {
    return { executed: yield* SynchronizedRef.make(false) };
  }),
);

describe("synchronization", () => {
  it.effect("finalize should return NoTransactionError when no transaction ran", () =>
    Effect.gen(function* () {
      const result = yield* Effect.void.pipe(finalize(Sync), Effect.provide(layer), Effect.exit);
      assertExitFailure(result, Cause.fail(new NoTransactionError()));
    }),
  );

  it.effect(
    "finalize shouldn't return errors when guarded by a transaction",
    Effect.fn(function* ({ expect }) {
      const result = yield* Effect.void.pipe(guard(Sync), finalize(Sync), Effect.provide(layer), Effect.exit);
      if (Exit.isFailure(result)) {
        expect.fail("Expected success", result.cause);
      }
    }),
  );
});
