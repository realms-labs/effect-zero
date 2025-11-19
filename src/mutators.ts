import type * as Effect from "effect/Effect";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import { prefixId } from "./internal/utils.js";

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorFn<R = any, TArgs extends any[] = any[]> = (...args: TArgs) => Effect.Effect<any, any, R>;

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorFns<R = any> = {
  [K: string]: AnyMutatorFn<R> | { [K: string]: AnyMutatorFn<R> };
};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutatorSchema = Schema.Schema<any, any, never>;
export type AnyMutatorSchemas = Record<string, AnyMutatorSchema | Record<string, AnyMutatorSchema>>;

export type MutatorFns<TSchema extends AnyMutatorSchemas> = {
  [K in keyof TSchema]: TSchema[K] extends AnyMutatorSchema
    ? AnyMutatorFn<unknown, [Schema.Schema.Type<TSchema[K]>]>
    : TSchema[K] extends infer TSchema extends Record<string, AnyMutatorSchema>
      ? { [K in keyof TSchema]: AnyMutatorFn<unknown, [Schema.Schema.Type<TSchema[K]>]> }
      : never;
};

export const MutatorSchemaSymbol = Symbol.for(prefixId("MutatorSchema"));

export type Mutator<
  TFunc extends AnyMutatorFn = AnyMutatorFn,
  TSchema extends AnyMutatorSchema = AnyMutatorSchema,
> = TFunc & {
  [MutatorSchemaSymbol]: TSchema;
};

export type Mutators<TSchema extends AnyMutatorSchemas, TFns extends AnyMutatorFns> = {
  [K in keyof TFns & keyof TSchema]: TFns[K] extends AnyMutatorFn
    ? TSchema[K] extends AnyMutatorSchema
      ? Mutator<TFns[K], TSchema[K]>
      : never
    : TSchema[K] extends infer TSchema extends Record<string, AnyMutatorSchema>
      ? TFns[K] extends infer TFns extends Record<string, AnyMutatorFn>
        ? { [K in keyof TFns & keyof TSchema]: Mutator<TFns[K], TSchema[K]> }
        : never
      : never;
} & {};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutator<R = any, E = any> = Mutator<(...args: any[]) => Effect.Effect<any, E, R>>;

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutators<R = any> = Record<string, AnyMutator<R> | Record<string, AnyMutator<R>>>;

export type ExtractMutatorsRequirements<TMutators extends AnyMutators> = TMutators extends AnyMutators<infer R>
  ? R
  : never;

export const schema = <TSchema extends AnyMutatorSchemas>(schema: TSchema) => schema;

export function make<TSchema extends AnyMutatorSchemas, TFns extends MutatorFns<TSchema>>(
  schema: TSchema,
  mutators: TFns,
): Mutators<TSchema, TFns>;
export function make(schema: AnyMutatorSchemas, mutators: AnyMutatorFns): AnyMutators {
  function makeMutator(schema: AnyMutatorSchema, fn: AnyMutatorFn) {
    return Object.assign(fn, { [MutatorSchemaSymbol]: schema });
  }

  return Rec.map(mutators, (v, name) => {
    return Match.value([v, schema[name]]).pipe(
      Match.when([Predicate.isFunction, Schema.isSchema], ([mutator, schema]) => makeMutator(schema, mutator)),
      Match.when([Predicate.isRecord, Predicate.isRecord], ([mutator, schema]) =>
        Rec.map(mutator, (mutator, name) => makeMutator(Option.getOrThrow(Rec.get(schema, name)), mutator)),
      ),
      Match.orElseAbsurd,
    );
  });
}
