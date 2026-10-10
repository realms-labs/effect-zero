import type { ReadonlyJSONValue } from "@rocicorp/zero";
import type * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SchemaIssue from "effect/SchemaIssue";
import { normalizeArgs } from "./utils.js";

const toStandardSchemaFailure = SchemaIssue.makeFormatterStandardSchemaV1();

/**
 * Decode mutator args, collecting every issue like a `defineMutator` Standard Schema validator does.
 */
export const decodeArgs = <S extends Schema.Codec<unknown, unknown, unknown, unknown>>(
  schema: S,
  args: unknown,
): Effect.Effect<S["Type"], Schema.SchemaError, S["DecodingServices"]> =>
  Schema.decodeUnknownEffect(schema)(normalizeArgs(args), { errors: "all" });

/**
 * Message and details of upstream's `InputValidationError`, so a mutator whose args fail to decode reports the same
 * app error as one defined with `defineMutator(validator, fn)`:
 * https://github.com/rocicorp/mono/blob/905d08516ba0caaf314cd28e93a5a47e8acdd482/packages/zql/src/query/validate-input.ts
 */
export const inputValidationError = (mutator: string, issue: SchemaIssue.Issue) => {
  const issues = toStandardSchemaFailure(issue).issues.map(({ message, path }) => ({
    message,
    path: path?.map((segment) => {
      const key = typeof segment === "object" ? segment.key : segment;
      return typeof key === "symbol" ? String(key) : key;
    }),
  }));
  return {
    message: `Validation failed for mutator ${mutator}: ${issues.map((issue) => issue.message).join(", ")}`,
    details: { type: "InputValidationError", result: { issues } } as ReadonlyJSONValue,
  };
};
