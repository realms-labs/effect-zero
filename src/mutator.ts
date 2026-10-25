import type {
  AnyMutatorRegistry,
  MutateRequest as ZeroMutateRequest,
  Schema as ZeroSchema,
  Transaction as ZeroTransaction,
} from "@rocicorp/zero";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Data from "effect/Data";
import * as Effect from "effect/Effect";
import type { NodeInspectSymbol } from "effect/Inspectable";
import * as Predicate from "effect/Predicate";
import * as Rec from "effect/Record";
import * as Schema from "effect/Schema";
import type * as Unify from "effect/Unify";
import type * as ClientTransaction from "./client-transaction.js";
import { ContextSymbol } from "./client-transaction.js";
import { prefixId } from "./internal/utils.js";

type _NodeInspectSymbol = NodeInspectSymbol;
type _Unify = Unify.typeSymbol | Unify.unifySymbol | Unify.ignoreSymbol;

export const RegistryTypeId = Symbol.for(prefixId("MutatorRegistry"));

/**
 * A mutator that isn't part of a registry yet, made with {@link make}. Like `defineMutator`, it gets its name
 * from its path in the {@link registry}.
 */
export interface Definition<Type, Encoded, E, R> {
  readonly payload: Schema.Codec<Type, Encoded, R>;
  readonly mutator: (args: Type) => Effect.Effect<unknown, E, R>;
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyDefinition = Definition<any, any, any, any>;

export type Definitions = { readonly [key: string]: AnyDefinition | Definitions };

/**
 * A registered mutator. Calling it encodes `args` and returns a request for `ZfxClient.mutate(zero, request)`, or
 * for Zero's own `zero.mutate(request)`. It is also a Zero `Mutator`, so the registry can be passed to Zero.
 */
export interface Mutator<Type, Encoded, E, R> extends Definition<Type, Encoded, E, R> {
  (args: Type): MutateRequest<Type>;
  readonly mutatorName: string;
  /** Runs the mutator for Zero, which calls it with the transaction and the `context` from `ZfxClient.make`. */
  readonly fn: (options: {
    readonly args: unknown;
    readonly ctx: unknown;
    readonly tx: ZeroTransaction<ZeroSchema>;
  }) => Promise<void>;
  readonly "~": "Mutator";
}

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyMutator = Mutator<any, any, any, any>;

/** A request to run a mutator, for `ZfxClient.mutate` or Zero's own `zero.mutate`. */
// biome-ignore lint/suspicious/noExplicitAny: compatible with any Zero instance's `zero.mutate`
export type MutateRequest<Type> = ZeroMutateRequest<any, any, any, any> & {
  // biome-ignore lint/suspicious/noExplicitAny: the encoded args, errors and services don't matter to the caller
  readonly mutator: Mutator<Type, any, any, any>;
};

type Tree<D> = {
  readonly [K in keyof D]: D[K] extends Definition<infer Type, infer Encoded, infer E, infer R>
    ? Mutator<Type, Encoded, E, R>
    : Tree<D[K]>;
};

/** A tree of registered mutators, made with {@link registry}. Pass it to `ZfxClient.make` and the server. */
export type Registry<D extends Definitions, TTransaction extends AnyClientTransaction> = Tree<D> & {
  readonly "~": AnyMutatorRegistry["~"];
  readonly [RegistryTypeId]: { readonly transaction: TTransaction };
};

// biome-ignore lint/suspicious/noExplicitAny: upper bound to allow everything
export type AnyRegistry = Registry<any, AnyClientTransaction>;

// biome-ignore lint/suspicious/noExplicitAny: any client transaction
type AnyClientTransaction = ClientTransaction.Context<any>;

type Keys<T> = Exclude<Extract<keyof T, string>, "~">;

/** The services a registry's mutators require. */
// biome-ignore lint/suspicious/noExplicitAny: matches any mutator
export type Requirements<T> = T extends Mutator<any, any, any, infer R>
  ? R
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
}): Definition<Type, Encoded, E, R | RD> => options;

/**
 * Registers mutators under their path, like Zero's `defineMutators`: `{ todo: { create } }` registers
 * `"todo.create"`. The result is also a Zero mutator registry, so it can be passed to `new Zero({ mutators })`.
 */
export const registry = <TTransaction extends AnyClientTransaction, const D extends Definitions>(
  transaction: TTransaction,
  definitions: D,
): Registry<D, TTransaction> => {
  const register = (definitions: Definitions, path: ReadonlyArray<string>): Record<string, unknown> =>
    Rec.map(definitions, (value, key) =>
      isDefinition(value) ? makeMutator(transaction, [...path, key].join("."), value) : register(value, [...path, key]),
    );

  // The registry's shape is only known from `D`.
  return Object.assign(register(definitions, []), {
    "~": "MutatorRegistry",
    [RegistryTypeId]: { transaction },
  }) as unknown as Registry<D, TTransaction>;
};

const isDefinition = (u: unknown): u is AnyDefinition =>
  Predicate.hasProperty(u, "payload") && Schema.isSchema(u.payload);

const makeMutator = <Type, Encoded, E, R>(
  transaction: AnyClientTransaction,
  name: string,
  definition: Definition<Type, Encoded, E, R>,
): Mutator<Type, Encoded, E, R> => {
  const run = (args: unknown, tx: ZeroTransaction<ZeroSchema>) =>
    Schema.decodeUnknownEffect(definition.payload)(args).pipe(
      Effect.catchTag("SchemaError", (error) => new ClientArgsParseError({ cause: Cause.fail(error) })),
      Effect.flatMap(definition.mutator),
      Effect.withSpan("zero.mutator", {
        attributes: {
          "zero.mutator": name,
          "zero.clientID": tx.clientID,
          "zero.mutationID": tx.mutationID,
          "zero.mutation.reason": tx.reason,
        },
      }),
      Effect.provideService(transaction[ContextSymbol], tx),
      Effect.asVoid,
    );

  const mutator: Mutator<Type, Encoded, E, R> = Object.assign(
    (args: Type) =>
      // A Zero `MutateRequest`, whose brand only exists at the type level.
      ({ mutator, args: Schema.encodeSync(definition.payload)(args), "~": "MutateRequest" }) as MutateRequest<Type>,
    {
      ...definition,
      mutatorName: name,
      "~": "Mutator" as const,
      fn: ({
        args,
        ctx,
        tx,
      }: {
        readonly args: unknown;
        readonly ctx: unknown;
        readonly tx: ZeroTransaction<ZeroSchema>;
      }) =>
        Context.isContext(ctx)
          ? // `ZfxClient.make` passes the services it captured, which cover every mutator's requirements.
            run(args, tx).pipe(Effect.runPromiseWith(ctx as Context.Context<R>))
          : Promise.reject(new MutatorContextNotFound({ mutator: name })),
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
  readonly registry: TRegistry;
  readonly overrides: O;
}

export type AnyServerMutators = ServerMutators<AnyRegistry, unknown>;

/**
 * The registry's mutators as the server runs them, for `ZfxServer.handleMutate`. `overrides` replaces some mutators'
 * bodies, like Zero's `defineMutators(base, overrides)`; every other mutator runs its shared body in a server
 * transaction, so pass `{}` if nothing differs. An override runs outside the transaction, so it can do work before
 * and after `serverTransaction.execute`, e.g. `registry.todo.create.mutator(args).pipe(serverTransaction.execute)`.
 */
export const server = <TRegistry extends AnyRegistry, const O extends Overrides<TRegistry>>(
  registry: TRegistry,
  overrides: O,
): ServerMutators<TRegistry, O> => ({ registry, overrides });

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
export type ServerRequirements<T extends AnyServerMutators> = T extends ServerMutators<infer TRegistry, infer O>
  ? ServerTreeRequirements<TRegistry, O, Transaction<TRegistry>[typeof ContextSymbol]["Identifier"]>
  : never;

export class ClientArgsParseError extends Data.TaggedError("ClientArgsParseError")<{
  readonly cause: Cause.Cause<unknown>;
}> {
  override get message() {
    const error = Cause.squash(this.cause);
    return Predicate.isError(error) && !Schema.isSchemaError(error) ? error.message : "Internal error";
  }
}

/** A mutator ran on a Zero instance that wasn't made with `ZfxClient.make`. */
export class MutatorContextNotFound extends Data.TaggedError("MutatorContextNotFound")<{
  readonly mutator: string;
}> {}
