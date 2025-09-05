import type * as Effect from "effect/Effect";

// biome-ignore lint/suspicious/noExplicitAny: used as a function argument for the mutators
export type MutatorArgs = Record<string, Record<string, any>>;

export type MutatorSchema<R, M extends MutatorArgs = MutatorArgs> = {
  [A in keyof M]: {
    // TODO(zero): Allow errors, but they should probably be handled (?)
    [B in keyof M[A]]: (args: M[A][B]) => Effect.Effect<void, unknown, R>;
  };
};
