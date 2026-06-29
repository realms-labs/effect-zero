import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { ClientArgsParseError } from "./client.js";

describe("ClientArgsParseError", () => {
  // A `SchemaError` is not `instanceof Error`, so the validation detail is masked to a generic
  // "Internal error" rather than leaked. The getter still exists so a `getErrorMessage`-style
  // consumer reads that clean string instead of serializing the raw `Cause` into a "Parsed message" blob.
  it.effect(
    "masks the schema validation detail to 'Internal error'",
    Effect.fn(function* ({ expect }) {
      const exit = yield* Effect.exit(Schema.decodeUnknownEffect(Schema.Struct({ id: Schema.String }))({}));
      expect(Exit.isFailure(exit)).toBe(true);
      if (!Exit.isFailure(exit)) return;

      const error = new ClientArgsParseError({ cause: exit.cause });

      expect(error.message).toBe("Internal error");
    }),
  );
});
