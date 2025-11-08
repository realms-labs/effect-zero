import type { Schema as ZeroSchema, Transaction as ZeroTransaction } from "@rocicorp/zero";
import * as Context from "effect/Context";

// Necessary workaround for TS declaration generation
export interface ZeroClientTransaction {
  readonly _tag: unique symbol;
}

export const make = <const Id extends string, S extends ZeroSchema>(id: Id) =>
  Context.Tag(id)<ZeroClientTransaction, ZeroTransaction<S>>();
