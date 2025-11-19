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
import * as Server from "effect-zero/server";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import postgres from "postgres";
import { clientTransaction } from "./client"; // see below
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
    create: ({ id, title }) => Effect.gen(function* () {
      // Note, we can run arbitrary logic before/after performing the zero transaction
      // this is a unique feature which is not supported by the default zero push processor implementation
      // shipped with the base `@rocicorp/zero` package
      
      // before the transaction
      yield* Effect.log("before the transaction");

      Effect.gen(function* () {
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
      yield* serverTransaction.use((tx) => tx.mutate.TodoTable.update({ id, done })).pipe(serverTransaction.execute);
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
import { Zero } from "@rocicorp/zero";
import * as ClientTransaction from "effect-zero/client-transaction";
import * as Effect from "effect/Effect";
import { schema } from "./schema"; // your schema
import { mutatorSchema } from "./mutators"; // see below

const clientTransaction = ClientTransaction.make("ClientTransaction", schema);

// The "client-side" transaction
export const clientTransaction = ClientTransaction.make("ClientTransaction", schema);

export const clientMutators = Mutators.make(mutatorSchema, {
  todo: {
    create: Effect.fn(function* ({ id, title }) {
      yield* zeroClient.Transaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
    }),
    toggle: Effect.fn(function* ({ id, done }) {
      yield* zeroClient.Transaction.use((tx) => tx.mutate.TodoTable.update({ id, done }));
    }),
  },
});

// Helper to create a vanilla Zero client instance for querying and mutating
export const createZero = Effect.fn(function* (opts: { userID: string; auth?: string; server: string }) {
  // `Client.make` returns an Effect containing a Zero client instance
  return yield* Client.make(clientTransaction, clientMutators, {
    userID: opts.userID,
    auth: opts.auth,
    server: opts.server, // your push/pull endpoint base URL
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
import { schema } from "./schema"; // your schema

const builder = createBuilder(schema);

export const getTodoByIdQuery = Query.make({
  name: "listTodos",
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

// See `handleZeroPush` notes
export async function handleZeroGetQueries(req: Request): Promise<Response> {
  const payload = Schema.decodeSync(TransformRequestMessage)(await req.json());
  const result = await Effect.runPromise(Server.handleGetQueries(queries, schema, payload));
  return new Response(JSON.stringify(result), { status: 200, headers: { "content-type": "application/json" } });
}
```

### Client setup

```ts
// client.ts

import * as Effect from 'effect/Effect';
import * as Query from "effect-zero/query";
import { getTodoByIdQuery } from "./queries";

const getTodoById = Effect.fn(function* (id: string) {
  // Create the query instance
  const query = yield* getTodoByIdQuery(id);

  // For `createZero` implementation, see "Custom mutators" -> "Client setup"
  const zero = yield* createZero({ ... });

  // `Query.subscribe` creates an Effect's Subscribable from a query
  yield* Query.subscribe(zero, query);

  // You can also use the query with the Zero client as usual
  const view = yield* Effect.sync(() => zero.materialize(query));
});
```

## Differences from the original implementation

One key difference is that `effect-zero` requires you to manually wrap your DB-related logic in a transaction inside a mutator code, whereas the original implementation automatically wraps the whole mutation in a transaction. This allows you to define some logic outside of transaction (either before or after), but it also creates some edge cases that are not possible in the original implementation, because now the transaction might succeed, but the code outside of it might fail. Below are the edge case rules that `effect-zero` follows during the mutation execution:

1. "One transaction and succeed" -> successful response from the push endpoint (normal flow).
2. "One transaction then fail" (code after the transaction produces an error) -> successful response, despite the mutation failing. This is essential to maintain integrity of Zero's internal state: the transaction has already succeeded (and altered the state of the database), thus the result from the push endpoint must coincide. Relatedly, the user must be careful with work performed after the transaction, it is considered "fire and forget".
3. "Two or more transactions" -> This is a sub-case of the "One transaction then fail (#2)" scenario; the first transaction will succeed (and thus we must return a successful response from the push endpoint), and the second one will fail. We must be careful of performing multiple transactions in the mutator for this reason.
4. "Zero transactions then succeed" -> error response, because all mutations must have a transaction.
5. "Zero transactions then fail" -> error response containing the first error encountered.
6. "Fail before transaction" -> same as #5.
