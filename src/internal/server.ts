import { ApplicationError } from "@rocicorp/zero/server";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Predicate from "effect/Predicate";
import type * as Schema from "effect/Schema";
import { prefixId } from "./utils.js";

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

const TransactInternalErrorTypeId = Symbol.for(prefixId("TransactInternalError"));
// Wraps any rejection from upstream's `transact` (out-of-order, DB-infra, …) so it stays in the Effect
// error channel as a single tagged error rather than a raw JS error. The top-level handler unwraps it:
// out-of-order is re-raised raw (for upstream's top-level routing), everything else becomes an
// application error. Its `message` getter squashes the cause so the app-error message is clean (not a
// serialized `Cause`), and reports "Internal error" for non-`Error` causes. Discriminated via its TypeId
// (not `instanceof`) so detection survives duplicate copies of this module.
export class TransactInternalError extends Data.TaggedError("TransactInternalError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  readonly [TransactInternalErrorTypeId] = TransactInternalErrorTypeId;
  static is(e: unknown): e is TransactInternalError {
    return Predicate.hasProperty(e, TransactInternalErrorTypeId);
  }
  override get message() {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) && error.message ? error.message : "Internal error";
  }
}

// Raised when `transact` resolves without ever invoking our callback (e.g. an already-processed
// mutation, whose last-mutation-id check fails before the callback runs). There is no inner value to
// return, so the mutation short-circuits and `handleMutate` surfaces the captured response instead.
export class TransactCallbackNotInvokedError extends Data.TaggedError("TransactCallbackNotInvokedError") {}

// Convert an Effect failure cause into an upstream `ApplicationError`, which `handleMutateRequest`
// recognizes (via `isApplicationError`) and turns into a per-mutation `{ error: "app", ... }` response.
// Each of our errors defines its own user-facing `message` — machinery errors report "Internal error",
// and `cause`-wrapping errors (e.g. ServerTransactionError) delegate to the error they wrap — so the
// squashed error's `message` is already the right text. We deliberately leave `details` unset so the
// wire shape matches upstream's (`makeAppErrorResponse` emits `details` only when truthy): the human
// text lives in `message`, and `details` is reserved for structured data, not a copy of the message.
export const makeApplicationError = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  const message = Predicate.isError(error) && error.message ? error.message : "Internal error";
  return new ApplicationError(message, { cause: error });
};
