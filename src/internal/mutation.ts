import type * as Cause from "effect/Cause";
import type * as Context from "effect/Context";
import * as Predicate from "effect/Predicate";
import * as Tracer from "effect/Tracer";
import { prefixId } from "./utils.js";

/**
 * The wire format of an effect-zero mutation's args: the encoded payload, plus metadata about the write that
 * `ZfxClient.mutate` attaches. Zero persists and pushes it as-is, so both the client mutator and the server see it.
 */
export interface Envelope {
  readonly args?: unknown;
  readonly meta?: EnvelopeMeta;
}

export interface EnvelopeMeta {
  /** Identifies the write within the client, so the client mutator can find the caller's pending write. */
  readonly id: string;
  /** W3C traceparent of the client's `zero.mutation` span, linked from the server's span. */
  readonly traceparent?: string;
}

export const unwrapEnvelope = (envelope: unknown): Envelope | undefined =>
  Predicate.isObject(envelope) && !Array.isArray(envelope) ? (envelope as Envelope) : undefined;

/** A write made through `ZfxClient.mutate` that hasn't settled on both the client and the server yet. */
export interface PendingWrite {
  readonly span: Tracer.Span;
  /** The client mutator's failure on its optimistic run, which `ZfxClient.mutate` surfaces as a typed error. */
  clientFailure: Cause.Cause<unknown> | undefined;
}

/** State shared by a Zero instance made with `ZfxClient.make` and the mutators it runs. */
export interface ClientState {
  readonly context: Context.Context<never>;
  readonly pending: Map<string, PendingWrite>;
}

export const ClientStateSymbol = Symbol.for(prefixId("ClientState"));

/** The Zero `context` that `ZfxClient.make` passes to every mutator. */
export interface ZeroContext {
  readonly [ClientStateSymbol]: ClientState;
}

let counter = 0;
const prefix = Math.random().toString(36).slice(2, 10);

export const makeWriteId = () => `${prefix}-${(counter++).toString(36)}`;

export const toTraceparent = (span: Tracer.Span) => `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;

export const fromTraceparent = (traceparent: string | undefined): Tracer.ExternalSpan | undefined => {
  const match = traceparent?.match(/^00-([0-9a-f]{32})-([0-9a-f]{16})-([0-9a-f]{2})$/);
  if (!match) return undefined;
  const [, traceId, spanId, flags] = match as unknown as [string, string, string, string];
  return Tracer.externalSpan({ traceId, spanId, sampled: (Number.parseInt(flags, 16) & 1) === 1 });
};
