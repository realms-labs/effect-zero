import * as Effect from "effect/Effect";
import * as Fn from "effect/Function";
import * as Match from "effect/Match";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import * as Str from "effect/String";
import { type AnyMutator, type AnyMutators, MutatorSchemaSymbol } from "../mutators.js";
import { normalizeArgs } from "./utils.js";

// The shape of a mutator tree (flat mutators and/or one level of namespaced mutators) is understood in
// several places (client unwrapping, server lookup). These helpers own that knowledge in one spot.

// Apply `f` to every leaf mutator, preserving the (flat or one-level-nested) tree shape.
export const mapLeaves = <R, A>(mutators: AnyMutators<R>, f: (mutator: AnyMutator<R>) => A) =>
  Rec.map(mutators, (node) => Match.value(node).pipe(Match.when(Predicate.isFunction, f), Match.orElse(Rec.map(f))));

// Resolve a wire mutation name to its leaf mutator. Supports a flat "name" as well as the
// "namespace|name" and "namespace.name" namespaced forms.
export const lookupLeaf = <R>(mutators: AnyMutators<R>, name: string): Option.Option<AnyMutator<R>> =>
  Fn.pipe(name.includes("|") ? Str.split(name, "|") : Str.split(name, "."), ([namespace, leaf]) =>
    Fn.pipe(
      mutators,
      Rec.get<string>(namespace),
      Option.flatMap((mutator) =>
        Match.value([mutator, leaf]).pipe(
          Match.when([Predicate.isObject, Predicate.isString], ([mutator, leaf]) => Rec.get<string>(leaf)(mutator)),
          Match.when([Predicate.isFunction, Predicate.isUndefined], ([mutator]) => Option.some(mutator)),
          Match.orElse(() => Option.none()),
        ),
      ),
    ),
  );

// Normalize Zero's argument payload and decode it with the leaf's attached schema (fails with a
// SchemaError, which callers map to their own client/server parse error).
export const decodeArgs = (mutator: AnyMutator, raw: unknown) =>
  Schema.decodeUnknownEffect(mutator[MutatorSchemaSymbol])(normalizeArgs(raw));
