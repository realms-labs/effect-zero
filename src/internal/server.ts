import { ApplicationError } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";

export class NoTransactionError extends Data.TaggedError("NoTransactionError") {
  message = "No transaction detected in a mutation, a transaction is required.";
}
export class MultipleTransactionsError extends Data.TaggedError("MultipleTransactionsError") {}

export class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  readonly name: string;
}> {}

export class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<Schema.SchemaError>;
}> {}

// Recursively unwrap Effect `cause` wrappers (e.g. ServerTransactionError) to find the root user
// message, so a mutator failure surfaces the original error text rather than an empty wrapper message.
const messageFromCause = (cause: Cause.Cause<unknown>): string => {
  const error = Cause.squash(cause);
  // Errors that originate from our own machinery (not the user's mutator) are reported generically.
  if (error instanceof MutatorNotFoundError || error instanceof ServerArgsParseError) {
    return "Internal error";
  }
  if (Predicate.hasProperty(error, "cause") && Cause.isCause(error.cause)) {
    const inner = messageFromCause(error.cause);
    if (inner) return inner;
  }
  if (error instanceof Error && error.message) return error.message;
  return "Internal error";
};

// Convert an Effect failure cause into an upstream `ApplicationError`, which `handleMutateRequest`
// recognizes (via `isApplicationError`) and turns into a per-mutation `{ error: "app", ... }` response.
// `details` is set explicitly because upstream `makeAppErrorResponse` only emits `details` when truthy.
export const toApplicationError = (cause: Cause.Cause<unknown>): ApplicationError => {
  const message = messageFromCause(cause);
  return new ApplicationError(message, { details: message, cause: Cause.squash(cause) });
};
