/** biome-ignore-all lint/suspicious/noExplicitAny: allowed in tests */
/** biome-ignore-all lint/style/noNonNullAssertion: allowed in tests */
/** biome-ignore-all lint/suspicious/noConfusingVoidType: allowed in tests */

import dotenv from "dotenv";
dotenv.config({ path: "tests/.env", quiet: true });

import { PostgresJSConnection, ZQLDatabase } from "@rocicorp/zero/pg";
import { beforeAll, beforeEach, expect, expectTypeOf, test, vi } from "bun:test";
import postgres from "postgres";
import { schema, type Schema as ZeroSchema } from "./schema";
import * as ZeroServer from "../src/server";
import * as ZeroClient from "../src/client";
import {
  Chunk,
  Console,
  Duration,
  Effect,
  Layer,
  Option,
  pipe,
  Schema,
  type Scope,
  Stream,
  Subscribable,
} from "effect";
import { Zero } from "@rocicorp/zero";
import { FetchHttpClient, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { ZeroPushBody, ZeroPushParams, ZeroPushResponse } from "../src/types";
import { drizzle } from "drizzle-orm/postgres-js";
import { count } from "drizzle-orm";
import { messages } from "./drizzle.schema";
import { nanoid } from "nanoid";
import { Atom, Registry, Result } from "@effect-atom/atom";
import { createSessionStorage } from "bun-storage";
import type { Message } from "./schema.gen";

const [sessionStorage] = createSessionStorage();
globalThis.sessionStorage = sessionStorage;

const rawDb = postgres(process.env.ZERO_UPSTREAM_DB!, { onnotice: () => {} });
const ddb = drizzle(rawDb);
const connection = new PostgresJSConnection(rawDb);
const database = new ZQLDatabase(connection, schema);

type MutatorArgs = {
  messages: {
    create: Message;
    throwsError: void;
    throwsErrorInsideTransaction: void;
    throwsErrorAfterTransaction: void;
    yieldsError: void;
    yieldsErrorInsideTransaction: void;
    yieldsErrorAfterTransaction: void;
    noTransaction: void;
    doubleTransaction: void;
  };
};

const zeroClient = ZeroClient.makeClient<ZeroSchema>();

const zeroServer = ZeroServer.makeServer({
  database,
  clientTransaction: zeroClient.Transaction,
});

const createMessage = vi.fn(
  Effect.fn(function* (item: Message) {
    yield* zeroClient.Transaction.use(async (tx) => {
      await tx.mutate.messages.insert(item);
    });
  }),
);

const clientMutators = zeroClient.mutators<MutatorArgs>()({
  messages: {
    create: createMessage,
    throwsError: Effect.fn(function* () {}),
    throwsErrorInsideTransaction: Effect.fn(function* () {}),
    throwsErrorAfterTransaction: Effect.fn(function* () {}),
    yieldsError: Effect.fn(function* () {}),
    yieldsErrorInsideTransaction: Effect.fn(function* () {}),
    yieldsErrorAfterTransaction: Effect.fn(function* () {}),
    noTransaction: Effect.fn(function* () {}),
    doubleTransaction: Effect.fn(function* () {}),
    // @ts-expect-error
    nonExistingMutator: Effect.fn(function* () {}),
  },
});

const serverMutators = zeroServer.mutators<MutatorArgs>()({
  messages: {
    create: Effect.fn(function* (item: Message) {
      yield* createMessage(item).pipe(zeroServer.Transaction.execute);
    }),
    throwsError: Effect.fn(function* () {
      yield* Effect.void;
      throw new Error("error in throwsError");
    }),
    throwsErrorInsideTransaction: Effect.fn(function* () {
      yield* zeroServer.Transaction.use(() => {
        throw new Error("error in throwsErrorInsideTransaction");
      }).pipe(zeroServer.Transaction.execute);
    }),
    throwsErrorAfterTransaction: Effect.fn(function* () {
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      }).pipe(zeroServer.Transaction.execute);
      throw new Error("error in throwsErrorAfterTransaction");
    }),
    yieldsError: Effect.fn(function* () {
      yield* Effect.fail(new Error("error in yieldsError"));
    }),
    yieldsErrorInsideTransaction: Effect.fn(function* () {
      yield* Effect.fail(new Error("error in yieldsErrorInsideTransaction")).pipe(zeroServer.Transaction.execute);
    }),
    yieldsErrorAfterTransaction: Effect.fn(function* () {
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      }).pipe(zeroServer.Transaction.execute);
      yield* Effect.fail(new Error("error in yieldsErrorAfterTransaction"));
    }),
    noTransaction: Effect.fn(function* () {}),
    doubleTransaction: Effect.fn(function* () {
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      }).pipe(zeroServer.Transaction.execute);
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      }).pipe(zeroServer.Transaction.execute);
    }),
  },
});

const onError = vi.fn((...args) => console.error("onError:", ...args));

function initZero() {
  return new Zero({
    userID: "anon",
    server: "http://localhost:4848",
    schema,
    mutators: zeroClient.unwrapMutators(clientMutators).pipe(Effect.runSync),
    push: {
      url: "http://localhost:3000/push",
    },
    onError,
  });
}

let z: ReturnType<typeof initZero>;

function waitForLastItem<A, E, R>(stream: Stream.Stream<A, E, R>) {
  return pipe(
    stream,
    Stream.timeout(Duration.millis(100)),
    Stream.runLast,
    Effect.map(Option.getOrThrow),
    Effect.scoped,
  );
}

let responses = Chunk.empty<ZeroPushResponse>();

beforeAll(async () => {
  const router = HttpRouter.empty.pipe(
    HttpRouter.post(
      "/push",
      Effect.gen(function* () {
        const params = yield* HttpRouter.schemaParams(ZeroPushParams);
        const payload = yield* HttpServerRequest.schemaBodyJson(ZeroPushBody);
        const result = yield* zeroServer.processPush(serverMutators, params, payload);
        // yield* Effect.log("Push result:", result);
        const responseBody = yield* Schema.encode(ZeroPushResponse)(result);

        responses = Chunk.append(responses, responseBody);

        return (yield* HttpServerResponse.json(responseBody)).pipe(
          HttpServerResponse.setStatus(200),
          HttpServerResponse.setHeader("content-type", "application/json"),
        );
      }).pipe(
        Effect.catchAll((e) =>
          Effect.gen(function* () {
            yield* Console.error("Push processor error:", e);
            return HttpServerResponse.empty({ status: 500 });
          }),
        ),
      ),
    ),
    HttpRouter.get("/health", HttpServerResponse.text("OK")),
  );

  const server = Effect.gen(function* () {
    const serverStarted = yield* Effect.makeLatch();
    const app = router.pipe(
      HttpServer.serve(),
      Layer.provide(BunHttpServer.layer({ port: 3000 })),
      Layer.tap(() => serverStarted.open),
    );
    yield* Layer.launch(app).pipe(Effect.tap(Console.log("server closed")), Effect.forkDaemon);
    yield* serverStarted.await;
  }).pipe(Effect.provide(FetchHttpClient.layer));

  await Effect.runPromise(server);
});

beforeEach(async () => {
  onError.mockReset();

  responses = Chunk.empty<ZeroPushResponse>();

  if (z) {
    await z.close();
  }
  z = initZero();

  const c = await ddb
    .select({ count: count() })
    .from(messages)
    .then((r) => r[0]!.count);
  if (c > 0) {
    await rawDb`truncate table messages`;

    // Wait until view is synced after truncation
    const sub = zeroClient.querySub(z.query.messages);
    await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === "complete"),
      waitForLastItem,
      Effect.andThen((d) => Effect.fail(new Error("not empty")).pipe(Effect.when(() => d.data.length > 0))),
      Effect.scoped,
      Effect.runPromise,
    );
  }
});

test("server is running", async () => {
  const response = await fetch("http://localhost:3000/health");
  await expect(response.text()).resolves.toEqual("OK");
});

test("processPush has no extra requirements", () => {
  const effect = zeroServer.processPush(serverMutators, {} as any, {} as any);

  expectTypeOf<Effect.Effect.Context<typeof effect>>().toEqualTypeOf<never>();
});

test("rows are returned after data is inserted", async () => {
  const value1 = {
    id: nanoid(),
    body: "hello world",
  };
  await ddb.insert(messages).values(value1);

  const sub = zeroClient.querySub(z.query.messages);

  expectTypeOf<Effect.Effect.Context<typeof sub>>().toEqualTypeOf<Scope.Scope>();

  {
    const result = await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === "complete"),
      waitForLastItem,
      Effect.runPromise,
    );

    expect(result.data).toEqual([value1]);
  }

  const value2 = {
    id: nanoid(),
    body: "hello world 2",
  };
  await ddb.insert(messages).values(value2);

  {
    const result = await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === "complete"),
      waitForLastItem,
      Effect.runPromise,
    );

    expect(result.data).toBeArrayOfSize(2);
    expect(result.data).toContainAllValues([value1, value2]);
  }
});

test("atom is set to correct value", async () => {
  const atom = zeroClient.queryAtom(z.query.messages);

  {
    const result = atom.pipe(Atom.get, Effect.provide(Registry.layer), Effect.runSync, Result.getOrThrow);

    expect(result).toEqual([]);
  }

  const value1 = {
    id: nanoid(),
    body: "hello world",
  };
  await ddb.insert(messages).values(value1);

  {
    const result = await pipe(
      Atom.toStreamResult(atom),
      waitForLastItem,
      Effect.provide(Registry.layer),
      Effect.runPromise,
    );

    expect(result).toEqual([value1]);
  }

  const value2 = {
    id: nanoid(),
    body: "hello world 2",
  };
  await ddb.insert(messages).values(value2);

  {
    const result = await pipe(
      Atom.toStreamResult(atom),
      waitForLastItem,
      Effect.provide(Registry.layer),
      Effect.runPromise,
    );

    expect(result).toBeArrayOfSize(2);
    expect(result).toContainAllValues([value1, value2]);
  }
});

test("custom mutators work", async () => {
  const item: Message = { id: nanoid(), body: "Hello, world!" };
  await z.mutate.messages.create(item).server;

  expect(createMessage).toHaveBeenCalledWith(item);

  const result = await rawDb`select id, body from messages`;
  expect(result.slice()).toEqual([item]);
});

test("mutator that throws error should reject", async () => {
  expect(z.mutate.messages.throwsError().server).rejects.toEqual({
    error: "app",
    details: "error in throwsError",
  });
});

test("mutator that throws error inside transaction should reject", async () => {
  expect(z.mutate.messages.throwsErrorInsideTransaction().server).rejects.toEqual({
    error: "app",
    details: "error in throwsErrorInsideTransaction",
  });
});

test("mutator that throws error after transaction should resolve", async () => {
  expect(z.mutate.messages.throwsErrorAfterTransaction().server).resolves.toBeDefined();
});

test("mutator that yields error should reject", async () => {
  expect(z.mutate.messages.yieldsError().server).rejects.toEqual({
    error: "app",
    details: "error in yieldsError",
  });
});

test("mutator that yields error inside transaction should reject", async () => {
  expect(z.mutate.messages.yieldsErrorInsideTransaction().server).rejects.toEqual({
    error: "app",
    details: "error in yieldsErrorInsideTransaction",
  });
});

test("mutator that yields error after transaction should resolve", async () => {
  expect(z.mutate.messages.yieldsErrorAfterTransaction().server).resolves.toBeDefined();
});

test("mutator without transaction should reject", async () => {
  expect(z.mutate.messages.noTransaction().server).rejects.toEqual({
    error: "app",
    details: "No transaction detected in a mutation, a transaction is required.",
  });
});

test("mutator that invokes transaction more than once should reject", async () => {
  await expect(z.mutate.messages.doubleTransaction().server).resolves.toBeDefined();
});

test("out of order mutations should be rejected", async () => {
  await z.mutate.messages.create({ id: nanoid(), body: "hello world" }).server;

  // Simulate database corruption
  await rawDb`truncate table zero_0.clients`;

  /*
    In case of "out of order" error, the mutation promise is not rejected, but retried under the hood.
    Since it won't ever be resolved or rejected in our case, we just run the mutation and check that
    the last server response was "oooMutation" error.
  */
  z.mutate.messages.create({ id: nanoid(), body: "hello world" }).server.then();

  await Effect.sleep(Duration.millis(100)).pipe(Effect.runPromise);

  const lastResponse = pipe(responses, Chunk.last, Option.getOrThrow);
  expect(lastResponse).toHaveProperty(["mutations", 0, "result", "error"], "oooMutation");
});

test("non-existing mutator should reject", async () => {
  expect((z.mutate.messages as any).nonExistingMutator().server).rejects.toEqual({
    error: "app",
    details: "Internal error",
  });
});
