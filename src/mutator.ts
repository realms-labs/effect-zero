import type {
  MutateRequest as ZeroMutateRequest,
  Schema as ZeroSchema,
  Transaction as ZeroTransaction,
} from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Predicate from "effect/Predicate";
import * as Schema from "effect/Schema";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { ContextSymbol } from "./client-transaction.js";
import { ClientStateSymbol, unwrapEnvelope, type ZeroContext } from "./internal/mutation.js";
import { prefixId } from "./internal/utils.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const DefinitionTypeId = Symbol.for(prefixId("MutatorDefinition"));
export const MutatorTypeId = Symbol.for(prefixId("Mutator"));
export const RegistryTypeId = Symbol.for(prefixId("MutatorRegistry"));
export const MutateRequestTypeId = Symbol.for(prefixId("MutateRequest"));
export const ServerMutatorsTypeId = Symbol.for(prefixId("ServerMutators"));

/**
 * A mutator that isn't part of a registry yet, made with {@link make}. Like `defineMutator`, it gets its name
 * from its path in the {@link registry}.
 */
export interface Definition<Type, Encoded, E, R> {
  readonly [DefinitionTypeId]: typeof DefinitionTypeId;
  // biome-ignore lint/suspicious/noExplicitAny: decoding services are tracked in `R`
  readonly payload: Schema.Codec<Type, Encoded, any>;
  readonly mutator: (args: Type) => Effect.Effect<unknown, E, R>;
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyDefinition = Definition<any, any, any, any>;

export type Definitions = { readonly [key: string]: AnyDefinition | Definitions };

/**
 * A registered mutator. Calling it encodes `args` and returns a request for `ZfxClient.mutate(zero, request)`, or
 * for Zero's own `zero.mutate(request)`. It is also a Zero `Mutator`, so the registry can be passed to Zero.
 */
export interface Mutator<Type, Encoded, E, R> {
  (args: Type): MutateRequest<Type, E>;
  readonly [MutatorTypeId]: typeof MutatorTypeId;
  readonly mutatorName: string;
  // biome-ignore lint/suspicious/noExplicitAny: decoding services are tracked in `R`
  readonly payload: Schema.Codec<Type, Encoded, any>;
  /** The mutator's body. Reuse it on the server with `serverTransaction.execute`. */
  readonly mutator: (args: Type) => Effect.Effect<unknown, E, R>;
  /** Runs the mutator for Zero, which calls it with the transaction and the `context` from `ZfxClient.make`. */
  readonly fn: (options: {
    readonly args: unknown;
    readonly ctx: unknown;
    // biome-ignore lint/suspicious/noExplicitAny: any Zero schema
    readonly tx: ZeroTransaction<any>;
  }) => Promise<void>;
  readonly "~": "Mutator";
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutator = Mutator<any, any, any, any>;

/** A request to run a mutator, which `ZfxClient.mutate` runs. `E` is the client mutator's failure. */
// biome-ignore lint/suspicious/noExplicitAny: compatible with any Zero instance's `zero.mutate`
export interface MutateRequest<Type, E> extends ZeroMutateRequest<any, any, any, any> {
  readonly [MutateRequestTypeId]: { readonly _Type: (_: Type) => void; readonly _E: () => E };
}

type Tree<D> = {
  readonly [K in keyof D]: D[K] extends Definition<infer Type, infer Encoded, infer E, infer R>
    ? Mutator<Type, Encoded, E, R>
    : Tree<D[K]>;
};

/** A tree of registered mutators, made with {@link registry}. Pass it to `ZfxClient.make` and the server. */
export type Registry<D extends Definitions, TTransaction extends AnyClientTransaction> = Tree<D> & {
  readonly "~": "MutatorRegistry";
  readonly [RegistryTypeId]: { readonly transaction: TTransaction };
};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyRegistry = Registry<any, AnyClientTransaction>;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction
type AnyClientTransaction = ClientTransaction.Context<any>;

type Keys<T> = Exclude<Extract<keyof T, string>, "~">;

/** The services a registry's mutators require. */
export type Requirements<T> = T extends AnyMutator
  ? T extends Mutator<infer _Type, infer _Encoded, infer _E, infer R>
    ? R
    : never
  : { [K in Keys<T>]: Requirements<T[K]> }[Keys<T>];

/** The client transaction a registry's mutators run in. */
export type Transaction<T extends AnyRegistry> = T[typeof RegistryTypeId]["transaction"];

/** The Zero schema a registry's mutators run against. */
export type ZeroSchemaOf<T extends AnyRegistry> = Transaction<T> extends ClientTransaction.Context<infer S> ? S : never;

/**
 * Defines a mutator. Its `payload` codec decodes the args on both the client and the server; it must encode without
 * services, since calling a registered mutator encodes its args synchronously.
 */
export const make = <Type, Encoded, RD = never, A = unknown, E = never, R = never>(options: {
  readonly payload: Schema.Codec<Type, Encoded, RD, never>;
  readonly mutator: (args: NoInfer<Type>) => Effect.Effect<A, E, R>;
}): Definition<Type, Encoded, E, R | RD> => ({
  [DefinitionTypeId]: DefinitionTypeId,
  payload: options.payload,
  mutator: options.mutator,
});

export const isDefinition = (u: unknown): u is AnyDefinition => Predicate.hasProperty(u, DefinitionTypeId);

export const isMutator = (u: unknown): u is AnyMutator => Predicate.hasProperty(u, MutatorTypeId);

/**
 * Registers mutators under their path, like Zero's `defineMutators`: `{ todo: { create } }` registers
 * `"todo.create"`. The result is also a Zero mutator registry, so it can be passed to `new Zero({ mutators })`.
 */
export const registry = <TTransaction extends AnyClientTransaction, const D extends Definitions>(
  transaction: TTransaction,
  definitions: D,
): Registry<D, TTransaction> => {
  const build = (node: Definitions, path: ReadonlyArray<string>): Record<string, unknown> =>
    Object.fromEntries(
      Object.entries(node).map(([key, value]) => [
        key,
        isDefinition(value)
          ? makeMutator([...path, key].join("."), value, transaction)
          : build(value as Definitions, [...path, key]),
      ]),
    );

  return Object.assign(build(definitions, []), {
    "~": "MutatorRegistry" as const,
    [RegistryTypeId]: { transaction },
  }) as unknown as Registry<D, TTransaction>;
};

const makeMutator = (name: string, definition: AnyDefinition, transaction: AnyClientTransaction): AnyMutator => {
  const encode = Schema.encodeSync(definition.payload);
  const decode = Schema.decodeUnknownEffect(definition.payload);

  const mutator: AnyMutator = Object.assign(
    // biome-ignore lint/suspicious/noExplicitAny: typed through `Mutator`
    (args: any) =>
      ({ mutator, args: { args: encode(args) }, "~": "MutateRequest" }) as unknown as MutateRequest<unknown, unknown>,
    {
      [MutatorTypeId]: MutatorTypeId as typeof MutatorTypeId,
      mutatorName: name,
      payload: definition.payload,
      mutator: definition.mutator,
      "~": "Mutator" as const,
      fn: ({
        args,
        ctx,
        tx,
      }: {
        readonly args: unknown;
        readonly ctx: unknown;
        readonly tx: ZeroTransaction<ZeroSchema>;
      }) => {
        const state = Predicate.hasProperty(ctx, ClientStateSymbol)
          ? (ctx as ZeroContext)[ClientStateSymbol]
          : undefined;
        if (state === undefined) {
          return Promise.reject(new Error(`Mutator "${name}" must run on a Zero instance made with ZfxClient.make`));
        }
        const envelope = unwrapEnvelope(args);
        const pending = envelope?.meta === undefined ? undefined : state.pending.get(envelope.meta.id);

        return decode(envelope?.args).pipe(
          Effect.catchTag("SchemaError", (e) => Effect.fail(new ClientArgsParseError({ cause: Cause.fail(e) }))),
          Effect.flatMap(definition.mutator),
          Effect.tapCause((cause) =>
            Effect.sync(() => {
              // Only the optimistic run answers the caller; rebases re-run the same write later.
              if (pending !== undefined && tx.reason === "optimistic") pending.clientFailure = cause;
            }),
          ),
          Effect.withSpan("zero.mutator", {
            parent: pending?.span,
            attributes: { "zero.mutator": name, "zero.mutation.reason": tx.reason },
          }),
          Effect.provideService(transaction[ContextSymbol], tx),
          Effect.asVoid,
          // The services were captured by `ZfxClient.make`, which requires every mutator's services.
          (effect) => Effect.runPromiseWith(state.context)(effect as Effect.Effect<void, unknown>),
        );
      },
    },
  );
  return mutator;
};

type Overrides<T> = {
  readonly [K in Keys<T>]?: T[K] extends Mutator<infer Type, infer _Encoded, infer _E, infer _R>
    ? // biome-ignore lint/suspicious/noExplicitAny: any server implementation
      (args: Type) => Effect.Effect<unknown, any, any>
    : Overrides<T[K]>;
};

/** The server's view of a registry, made with {@link server}. */
export interface ServerMutators<TRegistry extends AnyRegistry, O> {
  readonly [ServerMutatorsTypeId]: typeof ServerMutatorsTypeId;
  readonly registry: TRegistry;
  readonly overrides: O;
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyServerMutators = ServerMutators<AnyRegistry, any>;

/**
 * The registry's mutators as the server runs them. `overrides` replaces some mutators' bodies, like Zero's
 * `defineMutators(base, overrides)`; every other mutator runs its shared body in a server transaction. An override
 * runs outside the transaction, so it can do work before and after `serverTransaction.execute`, e.g.
 * `registry.todo.create.mutator(args).pipe(serverTransaction.execute)`.
 */
export const server = <TRegistry extends AnyRegistry, const O extends Overrides<TRegistry>>(
  registry: TRegistry,
  overrides: O,
): ServerMutators<TRegistry, O> => ({ [ServerMutatorsTypeId]: ServerMutatorsTypeId, registry, overrides });

export const isServerMutators = (u: unknown): u is AnyServerMutators => Predicate.hasProperty(u, ServerMutatorsTypeId);

type ServerTreeRequirements<T, O, ClientTransactionId> = {
  [K in Keys<T>]: T[K] extends AnyMutator
    ? K extends keyof O
      ? O[K] extends (args: never) => Effect.Effect<infer _A, infer _E, infer R>
        ? R
        : never
      : Exclude<Requirements<T[K]>, ClientTransactionId>
    : ServerTreeRequirements<T[K], K extends keyof O ? O[K] : unknown, ClientTransactionId>;
}[Keys<T>];

/**
 * The services the server needs to run these mutators. Mutators that aren't overridden run in a server transaction,
 * which provides their client transaction.
 */
export type ServerRequirements<T extends AnyRegistry | AnyServerMutators> = T extends ServerMutators<
  infer TRegistry,
  infer O
>
  ? ServerTreeRequirements<TRegistry, O, Transaction<TRegistry>[typeof ContextSymbol]["Identifier"]>
  : T extends AnyRegistry
    ? ServerTreeRequirements<T, unknown, Transaction<T>[typeof ContextSymbol]["Identifier"]>
    : never;

/**
 * Finds a mutator by its path, with its server override if it has one.
 */
export const lookup = (
  mutators: AnyRegistry | AnyServerMutators,
  path: ReadonlyArray<string>,
):
  | {
      readonly mutator: AnyMutator;
      readonly override: ((args: unknown) => Effect.Effect<unknown, unknown, unknown>) | undefined;
    }
  | undefined => {
  const [registry, overrides] = isServerMutators(mutators)
    ? [mutators.registry, mutators.overrides]
    : [mutators, undefined];
  let node: unknown = registry;
  let overrideNode: unknown = overrides;
  for (const key of path) {
    if (key === "~" || !Predicate.hasProperty(node, key)) return undefined;
    node = node[key];
    overrideNode = Predicate.hasProperty(overrideNode, key) ? overrideNode[key] : undefined;
  }
  return isMutator(node)
    ? {
        mutator: node,
        override: Predicate.isFunction(overrideNode)
          ? (overrideNode as (args: unknown) => Effect.Effect<unknown, unknown, unknown>)
          : undefined,
      }
    : undefined;
};

export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) && !Schema.isSchemaError(error) ? error.message : "Internal error";
  }
}
