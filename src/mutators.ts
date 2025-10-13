import * as Effect from "effect/Effect";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorDef<R = any, TArgs extends any[] = any[]> = (...args: TArgs) => Effect.Effect<any, any, R>;

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorDefs<R = any> = {
  [K: string]:
    | AnyMutatorDef<R>
    | {
        [K: string]: AnyMutatorDef<R>;
      };
};

export type ExtractMutatorDefsRequirements<T extends AnyMutatorDefs> = T extends AnyMutatorDefs<infer R> ? R : never;

export type AnyMutatorSchemaArgs = Schema.Schema.Any;
export type AnyMutatorSchema = Record<string, AnyMutatorSchemaArgs | Record<string, AnyMutatorSchemaArgs>>;

export type MutatorDefs<TSchema extends AnyMutatorSchema> = {
  [K in keyof TSchema]: TSchema[K] extends AnyMutatorSchemaArgs
    ? AnyMutatorDef<unknown, [Schema.Schema.Type<TSchema[K]>]>
    : TSchema[K] extends infer TSchema extends Record<string, AnyMutatorSchemaArgs>
      ? {
          [K in keyof TSchema]: AnyMutatorDef<unknown, [Schema.Schema.Type<TSchema[K]>]>;
        }
      : never;
};

export type ZeroMutator<
  TFunc extends AnyMutatorDef = AnyMutatorDef,
  TSchema extends AnyMutatorSchemaArgs = AnyMutatorSchemaArgs,
> = TFunc & {
  // biome-ignore lint/suspicious/noExplicitAny: edge case for upper bound
  _inArgs: any[] extends Parameters<TFunc>
    ? // biome-ignore lint/suspicious/noExplicitAny: edge case for upper bound
      any[]
    : Parameters<TFunc> extends []
      ? []
      : [Schema.Schema.Encoded<TSchema>];
};

export type ZeroMutators<TSchema extends AnyMutatorSchema, TDefs extends AnyMutatorDefs> = {
  [K in keyof TDefs & keyof TSchema]: TDefs[K] extends AnyMutatorDef
    ? TSchema[K] extends AnyMutatorSchemaArgs
      ? ZeroMutator<TDefs[K], TSchema[K]>
      : never
    : TSchema[K] extends infer TSchema extends Record<string, AnyMutatorSchemaArgs>
      ? TDefs[K] extends infer TDefs extends Record<string, AnyMutatorDef>
        ? {
            [K in keyof TDefs & keyof TSchema]: ZeroMutator<TDefs[K], TSchema[K]>;
          }
        : never
      : never;
} & {};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyZeroMutator = ZeroMutator<(...args: any[]) => Effect.Effect<any, any, any>>;

export type AnyZeroMutators = Record<string, AnyZeroMutator | Record<string, AnyZeroMutator>>;

export class ZeroMutatorSchema<T extends AnyMutatorSchema> {
  private constructor(public schema: T) {}

  static make<T extends AnyMutatorSchema>(schema: T) {
    return new ZeroMutatorSchema(schema);
  }

  makeMutators<TDefs extends MutatorDefs<T>>(mutators: TDefs): ZeroMutators<T, TDefs>;
  makeMutators(mutators: AnyMutatorDefs): AnyZeroMutators {
    // Adds argument validation to the mutator
    function makeMutator(
      argsSchema: Schema.Schema.Any,
      fn: (args: unknown) => Effect.Effect<unknown, unknown, unknown>,
    ) {
      return ((args: unknown) => Effect.andThen(Schema.decode(argsSchema)(args), fn)) as AnyZeroMutator;
    }

    return Rec.map(mutators, (v, name) => {
      return Match.value([v, this.schema[name]]).pipe(
        Match.when([Predicate.isFunction, Schema.isSchema], ([mutator, schema]) => makeMutator(schema, mutator)),
        Match.when([Predicate.isRecord, Predicate.isRecord], ([mutator, schema]) =>
          Rec.map(mutator, (mutator, name) => makeMutator(Option.getOrThrow(Rec.get(schema, name)), mutator)),
        ),
        Match.orElseAbsurd,
      );
    });
  }
}
