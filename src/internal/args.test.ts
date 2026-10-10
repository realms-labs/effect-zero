import { describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { decodeArgs, inputValidationError } from "./args.js";

describe("decodeArgs", () => {
  it.effect(
    "decodes arg-less (`null`) calls",
    Effect.fn(function* ({ expect }) {
      expect(yield* decodeArgs(Schema.Undefined, null)).toBeUndefined();
    }),
  );
});

describe("inputValidationError", () => {
  it.effect(
    "keeps nested paths JSON-serializable",
    Effect.fn(function* ({ expect }) {
      const key = Symbol.for("key");
      const schema = Schema.Struct({ items: Schema.Array(Schema.Struct({ [key]: Schema.Number })) });
      const error = yield* decodeArgs(schema, { items: [{ [key]: "1" }] }).pipe(Effect.flip);

      const { details } = inputValidationError("todo.create", error.issue);

      expect(details).toEqual({
        type: "InputValidationError",
        result: { issues: [{ message: expect.any(String), path: ["items", 0, "Symbol(key)"] }] },
      });
    }),
  );
});
