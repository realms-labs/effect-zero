import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { ClientArgsParseError } from "./client.js";
import { decodeArgs } from "./internal/args.js";

describe("ClientArgsParseError", () => {
  // Matches upstream's `InputValidationError`, which `wrapWithApplicationError` turns into the app error's
  // `message` and `details`, so a `getErrorMessage`-style consumer reads a clean string.
  it.effect(
    "reports every issue like a `defineMutator` validator",
    Effect.fn(function* ({ expect }) {
      const schema = Schema.Struct({ id: Schema.String, body: Schema.String });
      const error = yield* decodeArgs(schema, {}).pipe(
        Effect.flip,
        Effect.map((e) => new ClientArgsParseError({ mutator: "messages.create", issue: e.issue })),
      );

      expect(error.message).toBe("Validation failed for mutator messages.create: Missing key, Missing key");
      expect(error.details).toEqual({
        type: "InputValidationError",
        result: {
          issues: [
            { message: "Missing key", path: ["id"] },
            { message: "Missing key", path: ["body"] },
          ],
        },
      });
    }),
  );
});
