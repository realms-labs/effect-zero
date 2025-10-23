/** biome-ignore-all lint/suspicious/noExplicitAny: allowed in tests */
/** biome-ignore-all lint/style/noNonNullAssertion: allowed in tests */
/** biome-ignore-all lint/suspicious/noConfusingVoidType: allowed in tests */
// @effect-diagnostics importFromBarrel:off
// @effect-diagnostics missingReturnYieldStar:off

import dotenv from "dotenv";

dotenv.config({ path: "tests/.env", quiet: true });

import { afterEach, beforeAll, beforeEach, expect, expectTypeOf, test, vi } from "bun:test";
import { FetchHttpClient, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Atom, Registry, Result } from "@effect-atom/atom";
import { AtomRegistry } from "@effect-atom/atom/Registry";
import { createBuilder, syncedQuery, Zero } from "@rocicorp/zero";
import { PostgresJSConnection, ZQLDatabase } from "@rocicorp/zero/pg";
import { createSessionStorage } from "bun-storage";
import { count } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { Chunk, Console, Duration, Effect, Layer, Option, pipe, Schema, Scope, Stream, Subscribable } from "effect";
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as ZeroClient from "effect-zero/client";
import { MutatorSchema } from "effect-zero/mutators";
import * as ZeroQueries from "effect-zero/queries";
import * as ZeroServer from "effect-zero/server";
import { ZeroPushBody, ZeroPushParams, ZeroPushResponse } from "effect-zero/types/push";
import { ZeroTransformRequestMessage } from "effect-zero/types/queries";
import { prefixId } from "effect-zero/utils";
import { nanoid } from "nanoid";
import postgres from "postgres";
import { messages } from "./drizzle.schema";
import { schema, type Schema as ZeroSchema } from "./schema";
import type { Message } from "./schema.gen";

const [sessionStorage] = createSessionStorage();
globalThis.sessionStorage = sessionStorage;

const rawDb = postgres(process.env.ZERO_UPSTREAM_DB!, { onnotice: () => {} });
const ddb = drizzle(rawDb);
const connection = new PostgresJSConnection(rawDb);
const database = new ZQLDatabase(connection, schema);

const mutatorSchema = MutatorSchema.make({
  messages: {
    create: Schema.Struct({
      id: Schema.String,
      body: Schema.String,
    }),
  },
  optionalVoidArg: Schema.Void,
  transformArgs: Schema.Struct({
    foo: Schema.NumberFromString,
  }),
  throwsError: Schema.Void,
  throwsErrorInsideTransaction: Schema.Void,
  throwsErrorAfterTransaction: Schema.Void,
  clientThrowsError: Schema.Void,
  yieldsError: Schema.Void,
  yieldsErrorInsideTransaction: Schema.Void,
  yieldsErrorAfterTransaction: Schema.Void,
  noTransaction: Schema.Void,
  doubleTransaction: Schema.Void,
  concurrentTransactions: Schema.Void,
});

const zeroClient = ZeroClient.makeClient<ZeroSchema>();

const transformArgsClient = vi.fn(
  Effect.fn(function* (a) {
    yield* zeroClient.Transaction.use(async () => {});
    return a;
  }),
);

const transformArgsServer = vi.fn(
  Effect.fn(function* (a) {
    yield* zeroServer.Transaction.use(async () => {});
    return a;
  }),
);

const clientMutators = mutatorSchema.makeMutators({
  messages: {
    create: Effect.fn(function* (msg) {
      yield* zeroClient.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert(msg);
      });
    }),
  },
  /** biome-ignore lint/correctness/noUnusedFunctionParameters: required for test */
  optionalVoidArg: Effect.fn(function* (a) {}),
  transformArgs: Effect.fn(function* (a) {
    return yield* transformArgsClient(a);
  }),
  throwsError: Effect.fn(function* () {}),
  throwsErrorInsideTransaction: Effect.fn(function* () {}),
  throwsErrorAfterTransaction: Effect.fn(function* () {}),
  clientThrowsError: Effect.fn(function* () {
    yield* Effect.void;
    throw new Error("client error");
  }),
  yieldsError: Effect.fn(function* () {}),
  yieldsErrorInsideTransaction: Effect.fn(function* () {}),
  yieldsErrorAfterTransaction: Effect.fn(function* () {}),
  noTransaction: Effect.fn(function* () {}),
  doubleTransaction: Effect.fn(function* () {}),
  concurrentTransactions: Effect.fn(function* () {}),
});

const serverMutators = mutatorSchema.makeMutators({
  messages: {
    create: Effect.fn(function* (msg) {
      yield* clientMutators.messages.create(msg).pipe(zeroServer.Transaction.execute);
    }),
  },
  optionalVoidArg: Effect.fn(function* () {}),
  transformArgs: Effect.fn(function* (a) {
    return yield* transformArgsServer(a).pipe(zeroServer.Transaction.execute);
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
  clientThrowsError: Effect.fn(function* () {}),
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
  concurrentTransactions: Effect.fn(function* () {
    yield* Effect.all(
      [
        zeroServer.Transaction.use(async (tx) => {
          await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
        }).pipe(zeroServer.Transaction.execute),
        zeroServer.Transaction.use(async (tx) => {
          await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
        }).pipe(zeroServer.Transaction.execute),
      ],
      { concurrency: "unbounded" },
    );
  }),
});

const builder = createBuilder(schema);

const myMessages = ZeroQueries.makeQuery({
  name: "myMessages",
  payload: Schema.Tuple(Schema.String),
  query: Effect.fn(function* (id) {
    return yield* Effect.succeed(builder.messages.where("id", id));
  }),
});

const q1 = ZeroQueries.makeQuery({
  name: "q1",
  payload: Schema.Tuple(Schema.String),
  query: Effect.fn(function* () {
    return yield* Effect.succeed(builder.messages);
  }),
});

const queries = {
  myMessages,
  q1,
};

const zeroServer = ZeroServer.makeServer({
  database,
  clientTransaction: zeroClient.Transaction,
});

const onError = vi.fn((...args) => console.error("onError:", ...args));

const AtomRuntime: Atom.AtomRuntime<ZeroClient.ZeroClientProvider> = Atom.runtime(
  Layer.succeed(
    zeroClient.ZeroClientProvider,
    Effect.suspend(() => Atom.getResult(zeroAtom)),
  ),
);

const userIdAtom = AtomRuntime.atom(Effect.sync(() => "anon"));
const zeroAtom = AtomRuntime.atom(
  Effect.fn(function* (get) {
    yield* Console.log("creating zero");
    const userId = yield* get.result(userIdAtom);
    const z = new Zero({
      userID: userId,
      server: "http://localhost:4848",
      schema,
      mutators: yield* zeroClient.unwrapMutators(clientMutators),
      mutateURL: "http://localhost:3000/push",
      onError,
    });
    get.addFinalizer(() => {
      console.log("zeroAtom finalizer");
      // z.close();
    });

    const c = yield* Effect.promise(() =>
      ddb
        .select({ count: count() })
        .from(messages)
        .then((r) => r[0]!.count),
    );
    if (c > 0) {
      yield* Effect.promise(() => rawDb`truncate table messages`);

      // Wait until view is synced after truncation
      // const sub = yield* zeroClient.querySub(z.query.messages);

      // yield* pipe(
      //   sub.changes,
      //   Stream.filter((d) => d.status === "complete"),
      //   waitForLastItem,
      //   Effect.tap((d) => Effect.die(new Error("not empty")).pipe(Effect.when(() => d.data.length > 0))),
      //   Effect.scoped,
      // );
    }
    return z;
  }),
);

const waitForLastItem = Effect.fn("waitForLastItem")(function* <A, E, R>(stream: Stream.Stream<A, E, R>) {
  return yield* pipe(
    stream,
    Stream.timeout(Duration.millis(1000)),
    Stream.runLast,
    Effect.map(Option.getOrThrowWith(() => new Error("No items received from server"))),
    Effect.tap(Console.log("waitForLastItem done")),
  );
});

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
        Effect.catchAllCause(
          Effect.fn(function* (c) {
            yield* Effect.logError("Push processor error:", c);
            return HttpServerResponse.empty({ status: 500 });
          }),
        ),
      ),
    ),
    HttpRouter.post(
      "/get-queries",
      Effect.gen(function* () {
        const payload = yield* HttpServerRequest.schemaBodyJson(ZeroTransformRequestMessage);
        // yield* Console.log("get-queries payload:", payload);
        const response = yield* ZeroQueries.handleGetQueries(queries, schema, payload);
        return yield* HttpServerResponse.json(response);
      }).pipe(
        Effect.catchAllCause(
          Effect.fn(function* (c) {
            yield* Effect.logError("get-queries error:", c);
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
    yield* Layer.launch(app).pipe(Effect.forkDaemon);
    yield* serverStarted.await;
  }).pipe(Effect.provide(FetchHttpClient.layer));

  await Effect.runPromise(server);
});

beforeEach(async () => {
  onError.mockReset();

  responses = Chunk.empty<ZeroPushResponse>();
});

test("server is running", async () => {
  const response = await fetch("http://localhost:3000/health");
  await expect(response.text()).resolves.toEqual("OK");
});

test("processPush has no extra requirements", () => {
  const effect = zeroServer.processPush(serverMutators, {} as any, {} as any);

  expectTypeOf<Effect.Effect.Context<typeof effect>>().toEqualTypeOf<never>();
});

test("mutators should have correct argument types", () => {
  expectTypeOf<Parameters<typeof clientMutators.optionalVoidArg>>().toEqualTypeOf<[(void | undefined)?]>();
  expectTypeOf<Parameters<typeof clientMutators.throwsError>>().toEqualTypeOf<[]>();
  expectTypeOf<Parameters<typeof clientMutators.messages.create>>().toEqualTypeOf<
    [Schema.Schema.Type<typeof mutatorSchema.schema.messages.create>]
  >();
});

test("mutator requirements should propagate", () => {
  class DummyTag extends Effect.Service<DummyTag>()(prefixId("DummyTag"), {
    succeed: {},
  }) {}

  class DummyTag2 extends Effect.Service<DummyTag2>()(prefixId("DummyTag2"), {
    succeed: {},
  }) {}

  const mutatorSchema = MutatorSchema.make({
    dummy: Schema.Void,
    dummy2: Schema.Void,
  });
  const mutators = mutatorSchema.makeMutators({
    dummy: Effect.fn(function* () {
      yield* DummyTag;
    }),
    dummy2: Effect.fn(function* () {
      yield* DummyTag2;
    }),
  });
  const clientEffect = zeroClient.unwrapMutators(mutators);
  const serverEffect = zeroServer.processPush(mutators, {} as any, {} as any);

  expectTypeOf<Effect.Effect.Context<typeof clientEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
  expectTypeOf<Effect.Effect.Context<typeof serverEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
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
      Effect.scoped,
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
      Effect.scoped,
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

  const result = await rawDb`select id, body from messages`;
  expect(result.slice()).toEqual([item]);
});

test("schema validation is applied to mutator arguments", async () => {
  const mut = z.mutate.messages.create({} as any);
  await mut.client.catch((e) => {
    expect(e).toSatisfy(Predicate.isTagged("ZeroClientArgsParseError"));
  });
  await mut.server.catch((e) => {
    expect(e).toSatisfy(Predicate.isTagged("ZeroClientArgsParseError"));
  });
});

test("schema transformations are applied to mutator arguments", async () => {
  expectTypeOf<Parameters<typeof z.mutate.transformArgs>>().toEqualTypeOf<
    [Schema.Schema.Encoded<typeof mutatorSchema.schema.transformArgs>]
  >();
  expectTypeOf<Parameters<typeof clientMutators.transformArgs>>().toEqualTypeOf<
    [Schema.Schema.Type<typeof mutatorSchema.schema.transformArgs>]
  >();

  await z.mutate.transformArgs({ foo: "1" }).server;

  expect(transformArgsClient).toBeCalledWith({ foo: 1 });
  expect(transformArgsServer).toBeCalledWith({ foo: 1 });
});

test("mutator that throws error should reject", async () => {
  expect(z.mutate.throwsError().server).rejects.toEqual({
    error: "app",
    details: "error in throwsError",
  });
});

test("mutator that throws error inside transaction should reject", async () => {
  expect(z.mutate.throwsErrorInsideTransaction().server).rejects.toEqual({
    error: "app",
    details: "error in throwsErrorInsideTransaction",
  });
});

test("mutator that throws error after transaction should resolve", async () => {
  expect(z.mutate.throwsErrorAfterTransaction().server).resolves.toBeDefined();
});

test("client mutator that throws error should reject", async () => {
  const mut = z.mutate.clientThrowsError();

  await Promise.allSettled([mut.client, mut.server]);

  expect(mut.client).rejects.toThrowError("client error");
  expect(mut.server).rejects.toThrowError("client error");
});

test("mutator that yields error should reject", async () => {
  expect(z.mutate.yieldsError().server).rejects.toEqual({
    error: "app",
    details: "error in yieldsError",
  });
});

test("mutator that yields error inside transaction should reject", async () => {
  expect(z.mutate.yieldsErrorInsideTransaction().server).rejects.toEqual({
    error: "app",
    details: "error in yieldsErrorInsideTransaction",
  });
});

test("mutator that yields error after transaction should resolve", async () => {
  expect(z.mutate.yieldsErrorAfterTransaction().server).resolves.toBeDefined();
});

test("mutator without transaction should reject", async () => {
  expect(z.mutate.noTransaction().server).rejects.toEqual({
    error: "app",
    details: "No transaction detected in a mutation, a transaction is required.",
  });
});

test("mutator that invokes transaction more than once should resolve", async () => {
  await expect(z.mutate.doubleTransaction().server).resolves.toBeDefined();
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
  const mutatorSchema = MutatorSchema.make({
    nonExistingMutator: Schema.Void,
  });
  const clientMutators = mutatorSchema.makeMutators({
    nonExistingMutator: Effect.fn(function* () {}),
  });
  const z = new Zero({
    userID: "anon",
    server: "http://localhost:4848",
    schema,
    mutators: zeroClient.unwrapMutators(clientMutators).pipe(Effect.runSync),
    mutateURL: "http://localhost:3000/push",
    onError,
  });

  expect(z.mutate.nonExistingMutator().server).rejects.toEqual({
    error: "app",
    details: "Internal error",
  });
});

test("concurrent transactions should be resolved", async () => {
  await expect(z.mutate.concurrentTransactions().server).resolves.toBeDefined();
});

test.only("synced queries", async () => {
  const item: Message = { id: nanoid(), body: "Hello, world!" };

  const atom = AtomRuntime.atom(
    Effect.fn(function* (get) {
      yield* Console.log("creating query");
      const q = yield* myMessages(item.id);
      return yield* get.result(zeroClient.queryAtom(q));
    }),
  );

  await ddb.insert(messages).values(item);

  const result = await pipe(
    Atom.toStreamResult(atom),
    waitForLastItem,
    // Atom.getResult(atom),
    Effect.tap(Console.log("last atom value retrieved")),
    Effect.provide(Registry.layer),
    Effect.runPromise,
  );

  expect(result).toEqual([item]);
});

test("test", async () => {
  const builder = createBuilder(schema);

  await pipe(
    Effect.gen(function* () {
      const q2 = syncedQuery("q1", undefined, (a) => builder.messages);

      // const view = z.materialize(yield* q1("test"));
      // const view = (yield* q1("test")).materialize();

      // view.addListener((d) => {
      //   console.log("q1:", d);
      // });

      // const view2 = z.materialize(q2("test"));
      const view2 = q2("test").materialize();

      view2.addListener((d) => {
        console.log("q2:", d);
      });

      yield* Effect.promise(() => ddb.insert(messages).values({ id: "test", body: "hello world" }));

      yield* Effect.sleep(Duration.millis(1000));
    }),
    Effect.runPromise,
  );
});
