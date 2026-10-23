import type * as Effect from "effect/Effect";
import type * as Schema from "effect/Schema";
import type * as ClientTransaction from "./client-transaction.js";
import { prefixId } from "./internal/utils.js";

export const RegistryTypeId = Symbol.for(prefixId("MutatorRegistry"));

/**
 * A mutator, made with {@link make}: its payload codec and its Effect body. Like `defineMutator`, it gets its name
 * from its path in the {@link registry}.
 */
export interface Definition<Type, Encoded, E, R> {
  readonly payload: Schema.Codec<Type, Encoded, R>;
  readonly mutator: (args: Type) => Effect.Effect<unknown, E, R>;
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyDefinition = Definition<any, any, any, any>;

export type Definitions = { readonly [key: string]: AnyDefinition | Definitions };

/** A tree of registered mutators, made with {@link registry}. Pass it to `ZfxClient.make` and the server. */
export type Registry<D extends Definitions, TTransaction extends AnyClientTransaction> = D & {
  readonly [RegistryTypeId]: { readonly transaction: TTransaction };
};

export type AnyRegistry = Registry<Definitions, AnyClientTransaction>;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction
type AnyClientTransaction = ClientTransaction.Context<any>;

type Keys<T> = Extract<keyof T, string>;

/** The services a registry's mutators require. */
// biome-ignore lint/suspicious/noExplicitAny: matches any mutator
export type Requirements<T> = T extends Definition<any, any, any, infer R>
  ? R
  : { [K in Keys<T>]: Requirements<T[K]> }[Keys<T>];

/** The client transaction a registry's mutators run in. */
export type Transaction<T extends AnyRegistry> = T[typeof RegistryTypeId]["transaction"];

/** The Zero schema a registry's mutators run against. */
export type ZeroSchemaOf<T extends AnyRegistry> = Transaction<T> extends ClientTransaction.Context<infer S> ? S : never;

/** Defines a mutator. Its `payload` codec decodes the args on both the client and the server. */
export const make = <Type, Encoded, RD = never, A = unknown, E = never, R = never>(options: {
  readonly payload: Schema.Codec<Type, Encoded, RD, never>;
  readonly mutator: (args: NoInfer<Type>) => Effect.Effect<A, E, R>;
}): Definition<Type, Encoded, E, R | RD> => options;

/**
 * Registers mutators, at the root or nested under namespaces like `{ todo: { create } }`. Pass the registry to
 * `ZfxClient.make` and to {@link server}.
 */
export const registry = <TTransaction extends AnyClientTransaction, const D extends Definitions>(
  transaction: TTransaction,
  definitions: D,
): Registry<D, TTransaction> => ({ ...definitions, [RegistryTypeId]: { transaction } }) as Registry<D, TTransaction>;

type Implementations<T> = {
  readonly [K in Keys<T>]: T[K] extends Definition<infer Type, infer _Encoded, infer _E, infer _R>
    ? // biome-ignore lint/suspicious/noExplicitAny: any server implementation
      (args: Type) => Effect.Effect<unknown, any, any>
    : Implementations<T[K]>;
};

/** The server's view of a registry, made with {@link server}. */
export interface ServerMutators<TRegistry extends AnyRegistry, I> {
  readonly registry: TRegistry;
  readonly implementations: I;
}

export type AnyServerMutators = ServerMutators<AnyRegistry, unknown>;

/**
 * The registry's mutators as the server runs them, for `ZfxServer.handleMutate`. `implementations` has every
 * mutator's server implementation, which runs outside the transaction, so it can do work before and after
 * `serverTransaction.execute`. To run a mutator's shared body, use
 * `registry.todo.create.mutator(args).pipe(serverTransaction.execute)`.
 */
export const server = <TRegistry extends AnyRegistry, const I extends Implementations<TRegistry>>(
  registry: TRegistry,
  implementations: I,
): ServerMutators<TRegistry, I> => ({ registry, implementations });

type ServerTreeRequirements<T, I> = {
  [K in Keys<T>]: T[K] extends AnyDefinition
    ? K extends keyof I
      ? I[K] extends (args: never) => Effect.Effect<infer _A, infer _E, infer R>
        ? R
        : never
      : never
    : ServerTreeRequirements<T[K], K extends keyof I ? I[K] : unknown>;
}[Keys<T>];

/** The services the server needs to run these mutators: those of their implementations. */
export type ServerRequirements<T extends AnyServerMutators> = T extends ServerMutators<infer TRegistry, infer I>
  ? ServerTreeRequirements<TRegistry, I>
  : never;
