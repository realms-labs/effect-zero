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
}> {
  // Reported generically so this internal lookup failure doesn't leak into the app-error response.
  message = "Internal error";
}

export class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<Schema.SchemaError>;
}> {
  message = "Internal error";
}

// Raised when the upstream `transact` promise rejects for a reason other than out-of-order (e.g. a DB
// connection error) — i.e. a genuine infrastructure failure rather than a mutator/application error.
export class ExecuteTransactError extends Data.TaggedError("ExecuteTransactError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message(): string {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) && error.message ? error.message : "Internal error";
  }
}

// Raised when `transact` resolves without ever invoking our callback (e.g. an already-processed
// mutation, whose last-mutation-id check fails before the callback runs). There is no inner value to
// return, so the mutation short-circuits and `handleMutate` surfaces the captured response instead.
export class MutationShortCircuit extends Data.TaggedError("MutationShortCircuit") {}

// Convert an Effect failure cause into an upstream `ApplicationError`, which `handleMutateRequest`
// recognizes (via `isApplicationError`) and turns into a per-mutation `{ error: "app", ... }` response.
// Each of our errors defines its own user-facing `message` — machinery errors report "Internal error",
// and `cause`-wrapping errors (e.g. ServerTransactionError) delegate to the error they wrap — so the
// squashed error's `message` is already the right text. We deliberately leave `details` unset so the
// wire shape matches upstream's (`makeAppErrorResponse` emits `details` only when truthy): the human
// text lives in `message`, and `details` is reserved for structured data, not a copy of the message.
export const toApplicationError = (cause: Cause.Cause<unknown>): ApplicationError => {
  const error = Cause.squash(cause);
  const message = Predicate.isError(error) && error.message ? error.message : "Internal error";
  return new ApplicationError(message, { cause: error });
};
