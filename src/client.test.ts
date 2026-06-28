import { describe, it } from "@effect/vitest";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { ClientArgsParseError } from "./client.js";

describe("ClientArgsParseError", () => {
  // Regression guard: a `SchemaError` is NOT `instanceof Error`, so a getter gated on `instanceof Error`
  // silently masked the real validation text to "Internal error" (and removing the getter entirely lets a
  // `getErrorMessage`-style consumer serialize the `Cause` into a "Parsed message: {…}" blob). The getter
  // must surface the underlying message.
  it.effect(
    "surfaces the underlying schema error's message (not 'Internal error' or a serialized Cause)",
    Effect.fn(function* ({ expect }) {
      const exit = yield* Effect.exit(Schema.decodeUnknownEffect(Schema.Struct({ id: Schema.String }))({}));
      expect(Exit.isFailure(exit)).toBe(true);
      if (!Exit.isFailure(exit)) return;
      const schemaError = Cause.squash(exit.cause) as Schema.SchemaError;

      const error = new ClientArgsParseError({ cause: Cause.fail(schemaError) });

      expect(error.message).toBe(schemaError.message);
      expect(error.message).not.toBe("Internal error");
      expect(error.message).not.toContain("Parsed message");
    }),
  );
});
