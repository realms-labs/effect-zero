/** biome-ignore-all lint/suspicious/noExplicitAny: allowed in tests */
/** biome-ignore-all lint/style/noNonNullAssertion: allowed in tests */
/** biome-ignore-all lint/suspicious/noConfusingVoidType: allowed in tests */
// @effect-diagnostics importFromBarrel:off
// @effect-diagnostics missingReturnYieldStar:off

import dotenv from "dotenv";

dotenv.config({ path: "tests/.env", quiet: true });

import { beforeAll, beforeEach, expect, expectTypeOf, test, vi } from "bun:test";
import { FetchHttpClient, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { Atom, Registry, Result } from "@effect-atom/atom";
import { Zero } from "@rocicorp/zero";
import { PostgresJSConnection, ZQLDatabase } from "@rocicorp/zero/pg";
import { createSessionStorage } from "bun-storage";
import { count } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
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
import * as Exit from "effect/Exit";
import * as Predicate from "effect/Predicate";
import * as ZeroClient from "effect-zero/client";
import { MutatorSchema } from "effect-zero/mutators";
import * as ZeroServer from "effect-zero/server";
import { ZeroPushBody, ZeroPushParams, ZeroPushResponse } from "effect-zero/types";
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

const zeroServer = ZeroServer.makeServer({
  database,
  clientTransaction: zeroClient.Transaction,
});

const onError = vi.fn((...args) => console.error("onError:", ...args));

function initZero() {
  return new Zero({
    userID: "anon",
    server: "http://localhost:4848",
    schema,
    mutators: zeroClient.unwrapMutators(clientMutators).pipe(Effect.runSync),
    mutateURL: "http://localhost:3000/push",
    onError,
  });
}

let z: ReturnType<typeof initZero>;

function waitForLastItem<A, E, R>(stream: Stream.Stream<A, E, R>) {
  return pipe(
    stream,
    Stream.timeout(Duration.millis(100)),
    Stream.runLast,
    Effect.map(Option.getOrThrowWith(() => new Error("No items received from server"))),
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
    yield* Layer.launch(app).pipe(Effect.forkDaemon);
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

test("dummy zero context is detected", async () => {
  const sub = zeroClient
    .querySub(z.query.messages)
    .pipe(Effect.provide(ZeroClient.DummyZeroContext.layer("dummy-zero")));
  const result = await sub.pipe(waitForLastItem, Effect.runPromiseExit);
  Exit.match(result, {
    onSuccess: () => {
      throw new Error("expected error");
    },
    onFailure: (e) => {
      // @ts-ignore
      expect(e.error).toSatisfy(Predicate.isTagged("ZeroClientArgsParseError"));
    },
  });
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
