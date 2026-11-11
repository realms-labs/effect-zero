import type { ZeroOptions } from "@rocicorp/zero";
import type * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as LogLevel from "effect/LogLevel";
import * as Match from "effect/Match";

/**
 * A Zero log sink that writes to Effect's logger, with `context`'s services (its logger, and the span the logs belong
 * to). Each log is annotated with Zero's log context and `logger: "zero"`.
 */
export const make = <R>(context: Context.Context<R>): NonNullable<ZeroOptions["logSink"]> => ({
  log: (level, logContext, ...args) =>
    Match.value(level).pipe(
      Match.withReturnType<LogLevel.Severity>(),
      Match.when("debug", () => "Debug"),
      Match.when("info", () => "Info"),
      Match.when("warn", () => "Warn"),
      Match.when("error", () => "Error"),
      Match.exhaustive,
      (severity) => Effect.logWithLevel(severity)(...args),
      Effect.annotateLogs({ ...logContext, logger: "zero" }),
      Effect.runForkWith(context),
    ),
});
