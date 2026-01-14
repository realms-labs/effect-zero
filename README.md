# effect-zero

## Custom mutators
### Defining mutators schema
```ts
// mutators.ts

import * as Mutators from "effect-zero/mutators";

// Define your mutators schema
// Both root-level and nested mutators are supported (up to 1 level of nesting)
export const mutatorSchema = Mutators.schema({
  // root-level mutator
  /*
  foo: Schema.Struct({
    message: Schema.String,
  }),
  // mutator without arguments
  /*
  bar: Schema.Void,
  */
  todo: {
    // nested mutator
    create: Schema.Struct({
      id: Schema.String,
      title: Schema.String,
    }),
    toggle: Schema.Struct({
      id: Schema.String,
      done: Schema.Boolean,
    }),
  },
});
```

### Server setup

```ts
// server.ts

import { zeroPostgresJS } from "@rocicorp/zero/server/adapters/postgresjs";
import { PushResponse, PushParams, PushBody } from "effect-zero/types/push";
import * as ServerTransaction from "effect-zero/server-transaction";
import * as Mutators from "effect-zero/mutators";
import * as Server from "effect-zero/server";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import postgres from "postgres";
import { clientTransaction, clientMutators } from "./client"; // see below
import { mutatorSchema } from "./mutators";

// zero schema (define or use something like drizzle-zero)
import { schema } from "./schema";

// setup connection
// for more driver options, see: https://zero.rocicorp.dev/docs/zql-on-the-server#creating-a-database
const database = zeroPostgresJS(
  schema,
  postgres(process.env.ZERO_UPSTREAM_DB!),
);

// The "server-side" transaction
const serverTransaction = ServerTransaction.make(
  "ServerTransaction",
  database,
  // passing a client transaction allows us to use the client mutators on the server side
  clientTransaction,
);

// define server mutators
export const serverMutators = Mutators.make(mutatorSchema, {
  todo: {
    create: Effect.fn(function* ({ id, title }) {
      // Note, we can run arbitrary logic before/after performing the zero transaction
      // this is a unique feature which is not supported by the default zero push processor implementation
      // shipped with the base `@rocicorp/zero` package
      
      // before the transaction
      yield* Effect.log("before the transaction");

      yield* Effect.gen(function* () {
        // during the transaction
        yield* Effect.log("during the transaction");
        yield* serverTransaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
        yield* serverTransaction.use((tx) => tx.mutate.TodoTable.update({ id, done: false }));
        // ...
      }).pipe(serverTransaction.execute);

      // after the transaction
      yield* Effect.log("after the transaction");
    }),
    toggle: Effect.fn(function* ({ id, done }) {
      // You can also reuse client mutators on the server
      yield* clientMutators.todo.toggle({ id, done }).pipe(serverTransaction.execute);
    }),
  },
});

// handler for push endpoint
// Note: this is framework-agnostic so that is why Effect.runPromise is used below, however this is of course not needed
// if your server framework is effect-based (like Effect HTTP module)
export async function handleZeroPush(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const urlParams = Schema.decodeSync(PushParams)({
    schema: url.searchParams.get("schema")!,
    appID: url.searchParams.get("appID")!,
  });
  const payload = Schema.decodeSync(PushBody)(await req.json());

  const result = await Effect.runPromise(Server.processPush(serverTransaction, serverMutators, urlParams, payload));
  const responseBody = Schema.encodeSync(PushResponse)(result);
  return new Response(JSON.stringify(responseBody), { status: 200, headers: { "content-type": "application/json" } });
}
```

### Client setup

```ts
// client.ts
import * as Client from "effect-zero/client";
import * as ClientTransaction from "effect-zero/client-transaction";
import * as Mutators from "effect-zero/mutators";
import * as Effect from "effect/Effect";
import { schema } from "./schema"; // your schema
import { mutatorSchema } from "./mutators"; // see above

// The "client-side" transaction
export const clientTransaction = ClientTransaction.make("ClientTransaction", schema);

export const clientMutators = Mutators.make(mutatorSchema, {
  todo: {
    create: Effect.fn(function* ({ id, title }) {
      yield* clientTransaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
    }),
    toggle: Effect.fn(function* ({ id, done }) {
      yield* clientTransaction.use((tx) => tx.mutate.TodoTable.update({ id, done }));
    }),
  },
});

// Helper to create a vanilla Zero client instance for querying and mutating
export const createZero = Effect.fn(function* (opts: { userID: string; auth?: string; server: string; mutateURL: string }) {
  // `Client.make` returns an Effect containing a Zero client instance
  // Note: this is a scoped effect, so it must be run within a scope (e.g., Effect.scoped)
  return yield* Client.make(clientTransaction, clientMutators, {
    userID: opts.userID,
    auth: opts.auth,
    server: opts.server, // your zero-cache server URL
    mutateURL: opts.mutateURL, // your push endpoint URL
    kvStore: "idb", // or "mem" for in-memory
    // "mutators" and "schema" are inferred from other arguments, so we don't need to pass them here
  });
});
```

## Synced queries

### Defining queries
```ts
// queries.ts

import { createBuilder } from "@rocicorp/zero";
import * as Query from "effect-zero/query";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { schema } from "./schema"; // your schema

const builder = createBuilder(schema);

export const getTodoByIdQuery = Query.make({
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

import * as Server from "effect-zero/server";
import { TransformRequestMessage } from "effect-zero/types/queries";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { queries } from "./queries";
import { schema } from "./schema"; // your schema

// See `handleZeroPush` notes
export async function handleZeroQueryRequest(req: Request): Promise<Response> {
  const payload = Schema.decodeSync(TransformRequestMessage)(await req.json());
  const result = await Effect.runPromise(Server.handleQueryRequest(queries, schema, payload));
  return new Response(JSON.stringify(result), { status: 200, headers: { "content-type": "application/json" } });
}
```

### Client setup

```ts
// client.ts

import * as Effect from 'effect/Effect';
import * as Query from "effect-zero/query";
import { getTodoByIdQuery } from "./queries";
import { createZero } from "./client"; // see "Custom mutators" -> "Client setup"

const getTodoById = Effect.fn(function* (id: string) {
  // Create the query instance
  const query = yield* getTodoByIdQuery(id);

  const zero = yield* createZero({ userID: "anon", server: "http://localhost:4848" });

  // `Query.stream` creates an Effect's Stream from a query
  const stream = Query.stream(zero, query);

  // `Query.subscribable` creates an Effect's Subscribable from a query
  const sub = Query.subscribable(zero, query);

  // You can also use the query with the Zero client as usual
  const view = yield* Effect.sync(() => zero.materialize(query));
});
```

### Usage with [effect-atom](https://github.com/tim-smart/effect-atom)

You might want to create atoms with query results. To do that, you can implement a `queryAtom` helper like this:

```ts
import { Atom } from "@effect-atom/atom";
import * as Effect from "effect/Effect";
import * as Subscribable from "effect/Subscribable";
import * as Query from "effect-zero/query";

import type { schema } from "./schema"; // your schema
import { zeroAtom } from "./zero"; // you can create zeroAtom using `createZero` from the "Client setup" section as a reference

type Schema = typeof schema;

export const queryAtom = Atom.family(
  <T extends keyof Schema["tables"] & string, R>(query: Query.Query<T, Schema, R>) => {
    return Atom.subscribable(
      Effect.fn(function* (get) {
        const zero = yield* get.result(zeroAtom);
        return Query.subscribable(zero, query).pipe(Subscribable.map(({ value }) => value));
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

> **Note:** Queries created via `Query.make` implement the [`Equal` trait](https://effect.website/docs/trait/equal/), so `Atom.family` would properly cache the results when using queries as arguments.

## Differences from the original implementation

One key difference is that `effect-zero` requires you to manually wrap your DB-related logic in a transaction inside a mutator code, whereas the original implementation automatically wraps the whole mutation in a transaction. This allows you to define some logic outside of transaction (either before or after), but it also creates some edge cases that are not possible in the original implementation, because now the transaction might succeed, but the code outside of it might fail. Below are the edge case rules that `effect-zero` follows during the mutation execution:

1. "One transaction and succeed" -> successful response from the push endpoint (normal flow).
2. "One transaction then fail" (code after the transaction produces an error) -> successful response, despite the mutation failing. This is essential to maintain integrity of Zero's internal state: the transaction has already succeeded (and altered the state of the database), thus the result from the push endpoint must coincide. Relatedly, the user must be careful with work performed after the transaction, it is considered "fire and forget".
3. "Two or more transactions" -> This is a sub-case of the "One transaction then fail (#2)" scenario; the first transaction will succeed (and thus we must return a successful response from the push endpoint), and the second one will fail. We must be careful of performing multiple transactions in the mutator for this reason.
4. "Zero transactions then succeed" -> error response, because all mutations must have a transaction.
5. "Zero transactions then fail" -> error response containing the first error encountered.
6. "Fail before transaction" -> same as #5.
