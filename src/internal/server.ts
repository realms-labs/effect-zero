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

// No `message`: this internal lookup failure has none, so `makeApplicationError` falls back to
// "Internal error" — the mutator name (kept here only for logging) never leaks into the response.
export class MutatorNotFoundError extends Data.TaggedError("MutatorNotFoundError")<{
  readonly name: string;
}> {}

// No `message`: a schema-validation failure must not leak field/structure detail, so
// `makeApplicationError` falls back to "Internal error".
export class ServerArgsParseError extends Data.TaggedError("ServerArgsParseError")<{
  readonly cause: Cause.Cause<Schema.SchemaError>;
}> {}

const TransactInternalErrorTypeId = Symbol.for(prefixId("TransactInternalError"));
// Purely internal wrapper: holds any rejection from upstream's `transact` (out-of-order, DB-infra, …) so
// it stays in the Effect error channel as a single tagged error rather than a raw JS error. It carries no
// `message` of its own — the top-level handler unwraps it (`TransactInternalError.is`) and either re-raises
// the raw out-of-order error or turns the underlying error into an application error. Discriminated via its
// TypeId (not `instanceof`) so detection survives duplicate copies of this module.
export class TransactInternalError extends Data.TaggedError("TransactInternalError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  readonly [TransactInternalErrorTypeId] = TransactInternalErrorTypeId;
  static is(e: unknown): e is TransactInternalError {
    return Predicate.hasProperty(e, TransactInternalErrorTypeId);
  }
}

// Raised when `transact` resolves without ever invoking our callback (e.g. an already-processed
// mutation, whose last-mutation-id check fails before the callback runs). There is no inner value to
// return, so the mutation short-circuits and `handleMutate` surfaces the captured response instead.
export class TransactCallbackNotInvokedError extends Data.TaggedError("TransactCallbackNotInvokedError") {}

// Convert an Effect failure cause into an upstream `ApplicationError`, which `handleMutateRequest`
// recognizes (via `isApplicationError`) and turns into a per-mutation `{ error: "app", ... }` response.
// Uses the squashed error's `message`, falling back to "Internal error" for errors without a usable one
// (our internal machinery errors deliberately carry no message, so they fall back rather than leak). We
// leave `details` unset so the wire shape matches upstream's (`makeAppErrorResponse` emits `details` only
// when truthy): the human text lives in `message`, and `details` is reserved for structured data.
export const makeApplicationError = (cause: Cause.Cause<unknown>) => {
  const error = Cause.squash(cause);
  const message = Predicate.isError(error) && error.message ? error.message : "Internal error";
  return new ApplicationError(message, { cause: error });
};
