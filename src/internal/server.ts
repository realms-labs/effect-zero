import type * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Predicate from "effect/Predicate";
import { prefixId } from "./utils.js";

export class NoTransactionError extends Data.TaggedError("NoTransactionError") {
  message = "No transaction detected in a mutation, a transaction is required.";
}
export class MultipleTransactionsError extends Data.TaggedError("MultipleTransactionsError") {}

const TransactInternalErrorTypeId = Symbol.for(prefixId("TransactInternalError"));
export class TransactInternalError extends Data.TaggedError("TransactInternalError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  readonly [TransactInternalErrorTypeId] = TransactInternalErrorTypeId;
  static is(e: unknown): e is TransactInternalError {
    return Predicate.hasProperty(e, TransactInternalErrorTypeId);
  }
}

export class TransactCallbackNotInvokedError extends Data.TaggedError("TransactCallbackNotInvokedError") {}
