import type * as Cause from "effect/Cause";
import type * as Option from "effect/Option";
import type * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import type * as Tracer from "effect/Tracer";

/**
 * The wire format of an effect-zero mutation's args: the encoded payload, plus the `zero.mutation` span that sent
 * it. Zero persists and pushes it as-is.
 */
export const Envelope = Schema.Struct({
  args: Schema.optionalKey(Schema.Unknown),
  span: Schema.OptionFromOptionalKey(
    Schema.Struct({ traceId: Schema.String, spanId: Schema.String, sampled: Schema.Boolean }),
  ),
});

/** A write sent with `ZfxClient.mutate`, as its optimistic run sees it. */
export interface PendingWrite {
  readonly span: Tracer.Span;
  /** The client mutator's failure on its optimistic run, which `ZfxClient.mutate` surfaces as a typed error. */
  readonly clientFailure: Ref.Ref<Option.Option<Cause.Cause<unknown>>>;
}

/**
 * Keyed by the envelope `ZfxClient.mutate` sends. Zero passes that very object to the mutator's optimistic run;
 * rebases and the server only see copies.
 */
export const pendingWrites = new WeakMap<object, PendingWrite>();
