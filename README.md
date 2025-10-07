# effect-zero

## Server Setup

```ts
// server/zero.ts
import { PostgresJSConnection, ZQLDatabase } from "@rocicorp/zero/pg";
import { ZeroPushResponse, ZeroPushParams, ZeroPushBody } from "effect-zero/types";
import * as ZeroServer from "effect-zero/server";
import * as Schema from "effect/Schema";
import * as Effect from "effect/Effect";
import postgres from "postgres";
import { zeroClient } from '../client/zero'; // optional (see below)

// zero schema (define or use something like drizzle-zero)
import { schema } from "./schema";

// setup connection
const connection = new PostgresJSConnection(postgres(process.env.DATABASE_URL!));
const database = new ZQLDatabase(connection, schema);

// the "server-side" zero instance
export const zero = ZeroServer.makeServer({
  database,
  // this part is optional, but it allows us to use the client mutators on the server side
  clientTransaction: zeroClient.Transaction,
});

// define mutators (same as client, see below)
// in practice this can be a centralized file which is imported in both places
export type MutatorArgs = {
  todo: {
    create: { id: string; title: string };
    toggle: { id: string; done: boolean };
  };
};

// define mutators.
export const serverMutators = zero.mutators<MutatorArgs>()({
  todo: {
    create: Effect.fn(function* ({ id, title }) {
      // Note, we can run arbitrary logic before/after performing the zero transaction
      // this is a unique feature which is not supported by the default zero push processor implementation
      // shipped with the base `@rocicorp/zero` package
      
      // before the transaction
      yield Effect.log("before the transaction");

      Effect.gen(function* () {
        // during the transaction
        yield Effect.log("during the transaction");
        yield* zero.Transaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
        yield* zero.Transaction.use((tx) => tx.mutate.TodoTable.update({ id, done: false }));
        // ...
      }).pipe(zero.Transaction.execute);

      // after the transaction
      yield Effect.log("after the transaction");
    }),
    toggle: Effect.fn(function* ({ id, done }) {
      yield* zero.Transaction.use((tx) => tx.mutate.TodoTable.update({ id, done })).pipe(zero.Transaction.execute);
    }),
  },
});

// handler for push endpoint
// Note: this is framework-agnostic so that is why Effect.runPromise is used below, however this is of course not needed
// if your server framework is effect-based (like Effect HTTP module)

export async function handleZeroPush(req: Request): Promise<Response> {
  const url = new URL(req.url);
  const urlParams = Schema.decodeSync(ZeroPushParams)({
    schema: url.searchParams.get("schema")!,
    appID: url.searchParams.get("appID")!,
  });
  const payload = Schema.decodeSync(ZeroPushBody)(await req.json());

  const result = await Effect.runPromise(zero.processPush(serverMutators, urlParams, payload));
  const responseBody = Schema.encodeSync(ZeroPushResponse)(result);
  return new Response(JSON.stringify(responseBody), { status: 200, headers: { "content-type": "application/json" } });
}
```

## Client Setup

```ts
// client/zero.ts
import { Zero } from "@rocicorp/zero";
import * as ZeroClient from "effect-zero/client";
import * as Effect from "effect/Effect";
import { schema } from "./schema"; // your schema

// as mentioned above, this can be centralized between client and server
export type MutatorArgs = {
  todo: {
    create: { id: string; title: string };
    toggle: { id: string; done: boolean };
  };
};

// The "client-side" zero instance
export const zeroClient = ZeroClient.makeClient<typeof schema>();

export const clientMutators = zeroClient.mutators<MutatorArgs>()({
  todo: {
    create: Effect.fn(function* ({ id, title }) {
      yield* zeroClient.Transaction.use((tx) => tx.mutate.TodoTable.insert({ id, title, createdAt: Date.now() }));
    }),
    toggle: Effect.fn(function* ({ id, done }) {
      yield* zeroClient.Transaction.use((tx) => tx.mutate.TodoTable.update({ id, done }));
    }),
  },
});

export async function createZero(opts: { userID: string; auth?: string; server: string }) {
  const mutators = await Effect.runPromise(zeroClient.unwrapMutators(clientMutators));
  return new Zero({
    userID: opts.userID,
    auth: opts.auth,
    server: opts.server, // your push/pull endpoint base URL
    schema,
    kvStore: "idb", // or "mem" for in-memory
    mutators,
  });
}
```

## Query Atoms

```ts
// state/todos.ts
import { Atom } from "@effect-atom/atom";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { createZero, zero } from "../client/zero";

// build/load the zero instance
const zeroAtom = Atom.make(
  Effect.fn(function* (get) {
    const zero = yield* Effect.promise(() => createZero({ ... }));
    
    //add finalizer
    get.addFinalizer(() => {
      zero.close();
    });

    return zero;
  }),
);

// example query atom
export const userAtom = Atom.make(
  Effect.fn(function* (get) {
    const zero = yield* get.result(zeroAtom);
    const userId = yield* get.result(userIdAtom);
    if (Option.isNone(userId)) {
      return Option.none();
    }
    const query = zero.query.UserTable.where("id", "=", userId.value).one();
    return yield* get.result(zeroClient.queryAtom(query)).pipe(Effect.map(Option.fromNullable));
  }),
);
```

## Calling Mutations

With the `Zero` instance created using your unwrapped mutators, call them via `zero.mutate`:

```ts
export const deleteMessageAtom = Atom.fn(
  Effect.fn(function* (messageId: MessageId) {
    const zero = yield* Atom.getResult(zeroAtom);
    yield* Effect.promise(() => zero.mutate.MessageTable.delete({ id: messageId }));
  }),
);
```

## Differences from the original implementation

One key difference is that `effect-zero` requires you to manually wrap your DB-related logic in a transaction inside a mutator code, whereas the original implementation automatically wraps the whole mutation in a transaction. This allows you to define some logic outside of transaction (either before or after), but it also creates some edge cases that are not possible in the original implementation, because now the transaction might succeed, but the code outside of it might fail. Below are the edge case rules that `effect-zero` follows during the mutation execution:

1. "One transaction and succeed" -> successful response from the push endpoint (normal flow).
2. "One transaction then fail" (code after the transaction produces an error) -> successful response, despite the mutation failing. This is essential to maintain integrity of Zero's internal state: the transaction has already succeeded (and altered the state of the database), thus the result from the push endpoint must coincide. Relatedly, the user must be careful with work performed after the transaction, it is considered "fire and forget".
3. "Two or more transactions" -> This is a sub-case of the "One transaction then fail (#2)" scenario; the first transaction will succeed (and thus we must return a successful response from the push endpoint), and the second one will fail. We must be careful of performing multiple transactions in the mutator for this reason.
4. "Zero transactions then succeed" -> error response, because all mutations must have a transaction.
5. "Zero transactions then fail" -> error response containing the first error encountered.
6. "Fail before transaction" -> same as #5.
