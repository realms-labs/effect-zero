import type * as Effect from "effect/Effect";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import { prefixId } from "./utils";

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

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorSchemaArgs = Schema.Schema<any, any, never>;
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

export const MutatorArgsSchemaSym = Symbol.for(prefixId("MutatorArgsSchema"));

export type Mutator<
  TFunc extends AnyMutatorDef = AnyMutatorDef,
  TSchema extends AnyMutatorSchemaArgs = AnyMutatorSchemaArgs,
> = TFunc & {
  [MutatorArgsSchemaSym]: TSchema;
};

export type Mutators<TSchema extends AnyMutatorSchema, TDefs extends AnyMutatorDefs> = {
  [K in keyof TDefs & keyof TSchema]: TDefs[K] extends AnyMutatorDef
    ? TSchema[K] extends AnyMutatorSchemaArgs
      ? Mutator<TDefs[K], TSchema[K]>
      : never
    : TSchema[K] extends infer TSchema extends Record<string, AnyMutatorSchemaArgs>
      ? TDefs[K] extends infer TDefs extends Record<string, AnyMutatorDef>
        ? {
            [K in keyof TDefs & keyof TSchema]: Mutator<TDefs[K], TSchema[K]>;
          }
        : never
      : never;
} & {};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutator<R = any, E = any> = Mutator<(...args: any[]) => Effect.Effect<any, E, R>>;

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutators<R = any> = Record<string, AnyMutator<R> | Record<string, AnyMutator<R>>>;

export type ExtractMutatorsRequirements<T extends AnyMutators> = T extends AnyMutators<infer R> ? R : never;

export class MutatorSchema<T extends AnyMutatorSchema> {
  private constructor(public schema: T) {}

  static make<T extends AnyMutatorSchema>(schema: T) {
    return new MutatorSchema(schema);
  }

  // Associates mutator with its args schema
  private makeMutator(argsSchema: AnyMutatorSchemaArgs, fn: AnyMutatorDef): AnyMutator {
    return Object.assign(fn, { [MutatorArgsSchemaSym]: argsSchema });
  }

  makeMutators<TDefs extends MutatorDefs<T>>(mutators: TDefs): Mutators<T, TDefs>;
  makeMutators(mutators: AnyMutatorDefs): AnyMutators {
    return Rec.map(mutators, (v, name) => {
      return Match.value([v, this.schema[name]]).pipe(
        Match.when([Predicate.isFunction, Schema.isSchema], ([mutator, schema]) => this.makeMutator(schema, mutator)),
        Match.when([Predicate.isRecord, Predicate.isRecord], ([mutator, schema]) =>
          Rec.map(mutator, (mutator, name) => this.makeMutator(Option.getOrThrow(Rec.get(schema, name)), mutator)),
        ),
        Match.orElseAbsurd,
      );
    });
  }
}
