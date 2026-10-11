import type * as Cause from "effect/Cause";
import * as Predicate from "effect/Predicate";
import * as Tracer from "effect/Tracer";

/**
 * The wire format of an effect-zero mutation's args: the encoded payload, plus the W3C traceparent of the
 * `zero.mutation` span that sent it. Zero persists and pushes it as-is.
 */
export interface Envelope {
  readonly args?: unknown;
  readonly traceparent?: string;
}

export const unwrapEnvelope = (envelope: unknown): Envelope | undefined =>
  Predicate.isObject(envelope) && !Array.isArray(envelope) ? (envelope as Envelope) : undefined;

/** A write sent with `ZfxClient.mutate`, as its optimistic run sees it. */
export interface PendingWrite {
  readonly span: Tracer.Span;
  /** The client mutator's failure on its optimistic run, which `ZfxClient.mutate` surfaces as a typed error. */
  clientFailure: Cause.Cause<unknown> | undefined;
}

/**
 * Keyed by the envelope `ZfxClient.mutate` sends. Zero passes that very object to the mutator's optimistic run;
 * rebases and the server only see copies.
 */
export const pendingWrites = new WeakMap<object, PendingWrite>();

export const toTraceparent = (span: Tracer.Span) => `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;

export const fromTraceparent = (traceparent: string | undefined): Tracer.ExternalSpan | undefined => {
  const match = traceparent?.match(/^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/);
  if (!match) return undefined;
  const [, traceId, spanId, flags] = match as unknown as [string, string, string, string];
  return Tracer.externalSpan({ traceId, spanId, sampled: (Number.parseInt(flags, 16) & 1) === 1 });
};
