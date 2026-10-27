# effect-zero

## Custom mutators

Mutators follow Zero's [mutation request model](https://zero.rocicorp.dev/docs/mutators): you define them once,
register them, and run a mutation by calling a mutator to get a request.

### Defining mutators

```ts
// mutators.ts
import * as ZfxClientTransaction from "effect-zero/client-transaction";
import * as ZfxMutator from "effect-zero/mutator";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { schema } from "./schema"; // your Zero schema (define it, or use something like drizzle-zero)

// The "client-side" transaction
export const clientTransaction = ZfxClientTransaction.make("ClientTransaction", schema);

// Each mutator has a payload codec and an Effect body, like `ZfxQuery.make`
const createTodo = ZfxMutator.make({
  payload: Schema.Struct({ id: Schema.String, title: Schema.String }),
  mutator: Effect.fn(function* ({ id, title }) {
    yield* clientTransaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
  }),
});

// Mutators are named after their path in the registry, like Zero's `defineMutators`: "todo.create", "todo.toggle"
export const mutators = ZfxMutator.registry(clientTransaction, {
  todo: {
    create: createTodo,
    toggle: ZfxMutator.make({
      payload: Schema.Struct({ id: Schema.String, done: Schema.Boolean }),
      mutator: Effect.fn(function* ({ id, done }) {
        yield* clientTransaction.use((tx) => tx.mutate.TodoTable.update({ id, done }));
      }),
    }),
    // A mutator without arguments
    clear: ZfxMutator.make({
      payload: Schema.Void,
      mutator: Effect.fn(function* () {
        // ...
      }),
    }),
  },
});
```

> [!NOTE]
> Calling a mutator encodes its args right away, so the payload must encode without services.
> Zero stores and sends `undefined` mutator args as `null`, so a mutator may receive `null` where `undefined` was
> passed. Use `Schema.Void` for mutators without arguments, and `Schema.NullOr(...)` (not `Schema.UndefinedOr(...)`)
> for optional arguments.

### Server setup

```ts
// server.ts
import { zeroPostgresJS } from "@rocicorp/zero/server/adapters/postgresjs";
import * as ZfxMutateServerTypes from "effect-zero/types/mutate-server";
import * as ZfxPushTypes from "effect-zero/types/push";
import * as ZfxServerTransaction from "effect-zero/server-transaction";
import * as ZfxMutator from "effect-zero/mutator";
import * as ZfxServer from "effect-zero/server";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import postgres from "postgres";
import { clientTransaction, mutators } from "./mutators";
import { schema } from "./schema";

// for more driver options, see: https://zero.rocicorp.dev/docs/zql-on-the-server#creating-a-database
const database = zeroPostgresJS(schema, postgres(process.env.ZERO_UPSTREAM_DB!));

// The "server-side" transaction. Passing the client transaction lets mutators' shared bodies run on the server.
const serverTransaction = ZfxServerTransaction.make("ServerTransaction", database, clientTransaction);

// On the server, every mutator runs its shared body in a server transaction, unless it's overridden here
// (pass `{}` if nothing is). An override runs outside the transaction, so it can do work before and after it.
export const serverMutators = ZfxMutator.server(mutators, {
  todo: {
    create: Effect.fn(function* (args) {
      // before the transaction
      yield* Effect.log("before the transaction");

      // the shared body, in the transaction
      yield* mutators.todo.create.mutator(args).pipe(serverTransaction.execute);

      // after the transaction
      yield* Effect.log("after the transaction");
    }),
  },
});

// handler for push endpoint
// Note: this is framework-agnostic so that is why Effect.runPromise is used below, however this is of course not needed
// if your server framework is effect-based (like Effect HTTP module)
export async function handleZeroPush(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const query = Schema.decodeSync(ZfxMutateServerTypes.MutateParams)({
    schema: url.searchParams.get("schema")!,
    appID: url.searchParams.get("appID")!,
  });
  const body = Schema.decodeSync(ZfxPushTypes.PushBody)(await req.json());

  const responseBody = await Effect.runPromise(
    ZfxServer.handleMutate({
      transaction: serverTransaction,
      mutators: serverMutators,
      query,
      body,
      // The authenticated user id (or `null`/`undefined` for logged-out clients).
      userId: undefined,
    }),
  );
  return new Response(JSON.stringify(responseBody), { status: 200, headers: { "content-type": "application/json" } });
}
```

### Client setup

```ts
// client.ts
import * as ZfxClient from "effect-zero/client";
import * as Effect from "effect/Effect";
import { mutators } from "./mutators";

// Helper to create a vanilla Zero client instance for querying and mutating
export const createZero = Effect.fn(function* (opts: { userID: string; auth?: string; server: string; mutateURL: string }) {
  // `ZfxClient.make` returns an Effect containing a Zero client instance. It captures the services the mutators need.
  // Note: this is a scoped effect, so it must be run within a scope (e.g., Effect.scoped)
  return yield* ZfxClient.make(mutators, {
    userID: opts.userID,
    auth: opts.auth,
    server: opts.server, // your zero-cache server URL
    mutateURL: opts.mutateURL, // your push endpoint URL
    kvStore: "idb", // or "mem" for in-memory
    // Zero's own logs go through Effect's logger; pass `logSink` to send them elsewhere
  });
});
```

### Running mutations

```ts
const program = Effect.gen(function* () {
  const zero = yield* createZero({ /* ... */ });

  const mutation = yield* ZfxClient.mutate(zero, mutators.todo.create({ id: "1", title: "Write docs" }));

  // The client and server outcomes settle independently; await either, both, or neither.
  yield* mutation.client; // applied locally; fails with `MutationError`
  yield* mutation.server; // applied by the server; fails with `MutationError`
});
```

`ZfxClient.mutate` sends the mutation with `zero.mutate(request)` as soon as it runs, with the same args Zero would
send. A rejected mutation (Zero offline, or a mutator failing) fails `client` or `server` with a `MutationError`
carrying Zero's error (`type: "zero" | "app"`, `message`, `details`) and its `origin` (`"client" | "server"`). As in
Zero, a mutator's error survives as its name in `details`.

Each mutation is traced:
- **`zero.mutation`** (with `zero.mutator`, `zero.clientID`, `zero.clientGroupID` and the connection state) spans the
  whole write, with **`zero.mutation.client`** and **`zero.mutation.server`** children for its two outcomes. They're
  recorded whether or not you await them.
- **`zero.mutator`** spans each run of the mutator's body on the client, with `zero.clientID`, Zero's `zero.mutationID`
  and `zero.mutation.reason` (`optimistic` or `rebase`).
- **`zero.mutation.handle`** spans each mutation on the server, with the same `zero.clientID` and `zero.mutationID`, so
  the client's runs and the server's handling of a write can be joined on them.

Zero's own `zero.mutate(request)` also works with these requests, but isn't traced and resolves Zero's result details
instead of failing.

## Synced queries

### Defining queries
```ts
// queries.ts

import { createBuilder } from "@rocicorp/zero";
import * as ZfxQuery from "effect-zero/query";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { schema } from "./schema"; // your schema

const builder = createBuilder(schema);

export const getTodoByIdQuery = ZfxQuery.make({
  name: "getTodoById",
  payload: Schema.Tuple(Schema.String),
  query: Effect.fn(function* (id) {
    return yield* Effect.succeed(builder.todos.where("id", id).one());
  }),
});

// Add all your queries here
export const queries = [
  getTodoByIdQuery,
];
```

### Server setup
```ts
// server.ts

import * as ZfxServer from "effect-zero/server";
import * as ZfxCustomQueriesTypes from "effect-zero/types/custom-queries";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { queries } from "./queries";
import { schema } from "./schema"; // your schema

// See `handleZeroPush` notes
export async function handleZeroQueryRequest(req: Request): Promise<Response> {
  const body = Schema.decodeSync(ZfxCustomQueriesTypes.TransformRequestMessage)(await req.json());
  const result = await Effect.runPromise(
    ZfxServer.handleQuery({
      queries,
      schema,
      body,
      // The authenticated user id (or `null`/`undefined` for logged-out clients).
      userId: undefined,
    }),
  );
  return new Response(JSON.stringify(result), { status: 200, headers: { "content-type": "application/json" } });
}
```

### Client setup

```ts
// client.ts

import * as Effect from 'effect/Effect';
import * as ZfxQuery from "effect-zero/query";
import { getTodoByIdQuery } from "./queries";
import { createZero } from "./client"; // see "Custom mutators" -> "Client setup"

const getTodoById = Effect.fn(function* (id: string) {
  // Create the query instance
  const query = yield* getTodoByIdQuery(id);

  // Pass the real user ID (or `null`/`undefined` for logged-out clients).
  const zero = yield* createZero({ userID: "user-123", server: "http://localhost:4848" });

  // `ZfxQuery.stream` creates an Effect's Stream from a query
  const stream = ZfxQuery.stream(zero, query);

  // `ZfxQuery.subscribable` creates an Effect's Subscribable from a query
  const sub = ZfxQuery.subscribable(zero, query);

  // You can also use the query with the Zero client as usual
  const view = yield* Effect.sync(() => zero.materialize(query));
});
```

Each `ZfxQuery.stream` subscription is traced with a `zero.query` span, which runs until the query's first sync.

### Usage with [effect-atom](https://github.com/tim-smart/effect-atom)

You might want to create atoms with query results. To do that, you can implement a `queryAtom` helper like this:

```ts
import { Atom } from "@effect-atom/atom";
import * as Effect from "effect/Effect";
import * as Subscribable from "effect/Subscribable";
import * as ZfxQuery from "effect-zero/query";

import type { schema } from "./schema"; // your schema
import { zeroAtom } from "./zero"; // you can create zeroAtom using `createZero` from the "Client setup" section as a reference

type Schema = typeof schema;

export const queryAtom = Atom.family(
  <T extends keyof Schema["tables"] & string, R>(query: ZfxQuery.Query<T, Schema, R>) => {
    return Atom.subscribable(
      Effect.fn(function* (get) {
        const zero = yield* get.result(zeroAtom);
        return ZfxQuery.subscribable(zero, query).pipe(Subscribable.map(({ value }) => value));
      }),
    );
  },
);
```

```ts
// Usage

import { Atom } from "@effect-atom/atom";

const todoAtom = Atom.fn(Effect.fn(function* (id: string, get: Atom.FnContext) {
  const query = yield* getTodoByIdQuery(id);
  return yield* get.result(queryAtom(query));
});
```

> **Note:** Queries created via `ZfxQuery.make` implement the [`Equal` trait](https://effect.website/docs/trait/equal/), so `Atom.family` would properly cache the results when using queries as arguments.

## Differences from the original implementation

In `effect-zero` the Zero transaction is an explicit step inside a mutator (via `serverTransaction.execute`/`.use`), so a mutator may run arbitrary Effect logic before and after it. (Upstream's `handleMutateRequest`/`transact` callback supports running code around the transaction too — this is not unique to `effect-zero` — but because an `effect-zero` mutator is a typed `Effect` with the transaction as a sub-step, it defines explicit rules for the edge cases this creates: the transaction may commit while surrounding code fails, or a mutator may run zero or multiple transactions.) Below are the rules `effect-zero` follows during mutation execution:

1. "One transaction and succeed" -> successful response from the push endpoint (normal flow).
2. "One transaction then fail" (code after the transaction produces an error) -> successful response, despite the mutation failing. This is essential to maintain integrity of Zero's internal state: the transaction has already succeeded (and altered the state of the database), thus the result from the push endpoint must coincide. Relatedly, the user must be careful with work performed after the transaction, it is considered "fire and forget".
3. "Two or more transactions" -> This is a sub-case of the "One transaction then fail (#2)" scenario; the first transaction will succeed (and thus we must return a successful response from the push endpoint), and the second one will fail. We must be careful of performing multiple transactions in the mutator for this reason.
4. "Zero transactions then succeed" -> error response, because all mutations must have a transaction.
5. "Zero transactions then fail" -> error response containing the first error encountered.
6. "Fail before transaction" -> same as #5.
