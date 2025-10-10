import * as Effect from "effect/Effect";
import * as Match from "effect/Match";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyZeroMutators<R = any> = {
  [K: string]: // biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
    | ((...args: any[]) => Effect.Effect<any, any, R>)
    | {
        // biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
        [K: string]: (...args: any[]) => Effect.Effect<any, any, R>;
      };
};

export type ExtractMutatorSchemaRequirements<T extends AnyZeroMutators> = T extends AnyZeroMutators<infer R>
  ? R
  : never;

export type ZeroMutatorSchemaShapeCore = Schema.Schema.Any;
export type ZeroMutatorSchemaShape = Record<
  string,
  ZeroMutatorSchemaShapeCore | Record<string, ZeroMutatorSchemaShapeCore>
>;

export type ZeroMutators<T extends ZeroMutatorSchemaShape> = {
  [A in keyof T]: T[A] extends ZeroMutatorSchemaShapeCore
    ? (args: Schema.Schema.Type<T[A]>) => Effect.Effect<unknown, unknown, unknown>
    : T[A] extends Record<string, ZeroMutatorSchemaShapeCore>
      ? ZeroMutators<T[A]>
      : never;
};

export class ZeroMutatorSchema<T extends ZeroMutatorSchemaShape> {
  private constructor(public schema: T) {}

  static make<T extends ZeroMutatorSchemaShape>(schema: T) {
    return new ZeroMutatorSchema(schema);
  }

  makeClientMutators<TMutators extends ZeroMutators<T>>(mutators: TMutators): TMutators;
  /** @internal */
  makeClientMutators(mutators: AnyZeroMutators, schema?: ZeroMutatorSchemaShape): AnyZeroMutators;
  makeClientMutators(mutators: AnyZeroMutators, schema: ZeroMutatorSchemaShape = this.schema): AnyZeroMutators {
    // Adds argument validation to the mutator
    function makeMutator<T extends (...args: unknown[]) => Effect.Effect<unknown, unknown, unknown>>(
      argsSchema: Schema.Schema.Any,
      fn: T,
    ) {
      return ((arg) => Effect.andThen(Schema.decode(argsSchema)(arg), fn)) as T;
    }

    return Rec.map(mutators, (v, name) => {
      return Match.value([v, schema[name]]).pipe(
        Match.when([Predicate.isFunction, Schema.isSchema], ([mutator, schema]) => makeMutator(schema, mutator)),
        Match.when([Predicate.isRecord, Predicate.isRecord], ([mutator, schema]) =>
          this.makeClientMutators(mutator, schema),
        ),
        Match.orElseAbsurd,
      );
    }) as AnyZeroMutators;
  }

  makeServerMutators<TMutators extends ZeroMutators<T>>(mutators: TMutators) {
    return mutators;
  }
}
