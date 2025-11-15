/** biome-ignore-all lint/suspicious/noExplicitAny: allowed in tests */
/** biome-ignore-all lint/style/noNonNullAssertion: allowed in tests */
/** biome-ignore-all lint/suspicious/noConfusingVoidType: allowed in tests */
// @effect-diagnostics importFromBarrel:off
// @effect-diagnostics missingReturnYieldStar:off

import dotenv from "dotenv";

dotenv.config({ path: "tests/.env", quiet: true });
dotenv.config({ quiet: true });

import { createServer } from "node:http";
import { beforeEach } from "node:test";
import { FetchHttpClient, HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { NodeHttpServer } from "@effect/platform-node";
import { beforeAll, expect, expectTypeOf, it, test, vi } from "@effect/vitest";
import { zeroDrizzle } from "@rocicorp/zero/server/adapters/drizzle";
import { count } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { Chunk, Console, Duration, Effect, Layer, Option, pipe, Schema, type Scope, Stream } from "effect";
import * as Predicate from "effect/Predicate";
import * as Client from "effect-zero/client";
import * as ClientTransaction from "effect-zero/client-transaction";
import * as Mutators from "effect-zero/mutators";
import * as Query from "effect-zero/query";
import * as Server from "effect-zero/server";
import * as ServerTransaction from "effect-zero/server-transaction";
import { PushBody, PushParams, PushResponse } from "effect-zero/types";
import { prefixId } from "effect-zero/utils";
import { nanoid } from "nanoid";
import postgres from "postgres";
import { WebSocket } from "undici";
import { messages } from "./drizzle.schema";
import { schema } from "./schema";
import type { Message } from "./schema.gen";

globalThis.WebSocket = WebSocket as typeof globalThis.WebSocket;

const rawDb = postgres(process.env.ZERO_UPSTREAM_DB!, { onnotice: () => {} });
const ddb = drizzle(rawDb);
const database = zeroDrizzle(schema, ddb);

const mutatorSchema = Mutators.schema({
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

const clientTransaction = ClientTransaction.make("ClientTransaction", schema);
const serverTransaction = ServerTransaction.make("ServerTransaction", database, clientTransaction);

const transformArgsClient = vi.fn(
  Effect.fn(function* (a) {
    yield* clientTransaction.use(async () => {});
    return a;
  }),
);

const transformArgsServer = vi.fn(
  Effect.fn(function* (a) {
    yield* serverTransaction.use(async () => {});
    return a;
  }),
);

const clientMutators = Mutators.make(mutatorSchema, {
  messages: {
    create: Effect.fn(function* (msg) {
      yield* clientTransaction.use(async (tx) => {
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

const serverMutators = Mutators.make(mutatorSchema, {
  messages: {
    create: Effect.fn(function* (msg) {
      yield* clientMutators.messages.create(msg).pipe(serverTransaction.execute);
    }),
  },
  optionalVoidArg: Effect.fn(function* () {}),
  transformArgs: Effect.fn(function* (a) {
    return yield* transformArgsServer(a).pipe(serverTransaction.execute);
  }),
  throwsError: Effect.fn(function* () {
    yield* Effect.void;
    throw new Error("error in throwsError");
  }),
  throwsErrorInsideTransaction: Effect.fn(function* () {
    yield* serverTransaction
      .use(() => {
        throw new Error("error in throwsErrorInsideTransaction");
      })
      .pipe(serverTransaction.execute);
  }),
  throwsErrorAfterTransaction: Effect.fn(function* () {
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
    throw new Error("error in throwsErrorAfterTransaction");
  }),
  clientThrowsError: Effect.fn(function* () {}),
  yieldsError: Effect.fn(function* () {
    yield* Effect.fail(new Error("error in yieldsError"));
  }),
  yieldsErrorInsideTransaction: Effect.fn(function* () {
    yield* Effect.fail(new Error("error in yieldsErrorInsideTransaction")).pipe(serverTransaction.execute);
  }),
  yieldsErrorAfterTransaction: Effect.fn(function* () {
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
    yield* Effect.fail(new Error("error in yieldsErrorAfterTransaction"));
  }),
  noTransaction: Effect.fn(function* () {}),
  doubleTransaction: Effect.fn(function* () {
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
  }),
  concurrentTransactions: Effect.fn(function* () {
    yield* Effect.all(
      [
        serverTransaction
          .use(async (tx) => {
            await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
          })
          .pipe(serverTransaction.execute),
        serverTransaction
          .use(async (tx) => {
            await tx.mutate.messages.insert({ id: nanoid(), body: "hello world" });
          })
          .pipe(serverTransaction.execute),
      ],
      { concurrency: "unbounded" },
    );
  }),
});

const onError = vi.fn((...args) => console.error("onError:", ...args));

const waitForLastItem = Effect.fn("waitForLastItem")(<A, E, R>(stream: Stream.Stream<A, E, R>) => {
  return pipe(
    stream,
    Stream.timeout(Duration.millis(100)),
    Stream.runLast,
    Effect.flatten,
    Effect.catchTag("NoSuchElementException", () => Effect.fail(new Error("No items received from server"))),
  );
});

let responses = Chunk.empty<PushResponse>();

beforeAll(async () => {
  const router = HttpRouter.empty.pipe(
    HttpRouter.post(
      "/push",
      Effect.gen(function* () {
        const params = yield* HttpRouter.schemaParams(PushParams);
        const payload = yield* HttpServerRequest.schemaBodyJson(PushBody);
        const result = yield* Server.processPush(serverTransaction, serverMutators, params, payload);
        // yield* Effect.log("Push result:", result);
        const responseBody = yield* Schema.encode(PushResponse)(result);

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
      Layer.provide(NodeHttpServer.layer(() => createServer(), { port: 3000 })),
      Layer.tap(() => serverStarted.open),
    );
    yield* Layer.launch(app).pipe(Effect.forkDaemon);
    yield* serverStarted.await;
  }).pipe(Effect.provide(FetchHttpClient.layer));

  await Effect.runPromise(server);
});

beforeEach(() => {
  onError.mockReset();

  responses = Chunk.empty<PushResponse>();
});

const initZero = Effect.gen(function* () {
  const z = yield* Client.make(clientTransaction, clientMutators, {
    userID: "anon",
    server: "http://localhost:4848",
    mutateURL: "http://localhost:3000/push",
    onError,
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
    const sub = yield* Query.subscribe(z, z.query.messages);
    yield* pipe(
      sub.changes,
      Stream.filter((d) => d.status === "complete"),
      waitForLastItem,
      Effect.andThen((d) => Effect.fail(new Error("not empty")).pipe(Effect.when(() => d.data.length > 0))),
    );
  }

  return z;
});

// const queryAtom = Atom.family(
//   <T extends keyof (typeof schema)["tables"] & string, R>(query: ZeroQuery<typeof schema, T, R>) =>
//     Atom.make((get) =>
//       Effect.gen(function* () {
//         const z = yield* get.result(zeroAtom);
//         const sub = yield* Query.subscribe(z, query);
//         return sub.changes;
//       }).pipe((e) =>
//         Stream.unwrap(
//           e as Effect.Effect<
//             Effect.Effect.Success<typeof e>,
//             Effect.Effect.Error<typeof e>,
//             // Pretend this effect doesn't have a scope requirement to make the type inference work as expected
//             // TODO: ask Effect team to fix this
//             Exclude<Effect.Effect.Context<typeof e>, Scope.Scope>
//           >,
//         ),
//       ),
//     ),
// );

test("server is running", async () => {
  const response = await fetch("http://localhost:3000/health");
  await expect(response.text()).resolves.toEqual("OK");
});

test("processPush has no extra requirements", () => {
  const effect = Server.processPush(serverTransaction, serverMutators, {} as any, {} as any);

  expectTypeOf<Effect.Effect.Context<typeof effect>>().toEqualTypeOf<never>();
});

test("mutators should have correct argument types", () => {
  expectTypeOf<Parameters<typeof clientMutators.optionalVoidArg>>().toEqualTypeOf<[(void | undefined)?]>();
  expectTypeOf<Parameters<typeof clientMutators.throwsError>>().toEqualTypeOf<[]>();
  expectTypeOf<Parameters<typeof clientMutators.messages.create>>().toEqualTypeOf<
    [Schema.Schema.Type<typeof mutatorSchema.messages.create>]
  >();
});

test("mutator requirements should propagate", () => {
  class DummyTag extends Effect.Service<DummyTag>()(prefixId("DummyTag"), {
    succeed: {},
  }) {}

  class DummyTag2 extends Effect.Service<DummyTag2>()(prefixId("DummyTag2"), {
    succeed: {},
  }) {}

  const clientTransaction = ClientTransaction.make("DummyClientTransaction", schema);
  const serverTransaction = ServerTransaction.make("DummyServerTransaction", database, clientTransaction);
  const mutatorSchema = Mutators.schema({
    dummy: Schema.Void,
    dummy2: Schema.Void,
  });
  const mutators = Mutators.make(mutatorSchema, {
    dummy: Effect.fn(function* () {
      yield* DummyTag;
    }),
    dummy2: Effect.fn(function* () {
      yield* DummyTag2;
    }),
  });
  const serverEffect = Server.processPush(serverTransaction, mutators, {} as any, {} as any);

  // expectTypeOf<Effect.Effect.Context<typeof clientEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
  expectTypeOf<Effect.Effect.Context<typeof serverEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
});

it.scopedLive(
  "rows are returned after data is inserted",
  Effect.fn(function* () {
    const z = yield* initZero;

    const value1 = {
      id: nanoid(),
      body: "hello world",
    };
    yield* Effect.promise(() => ddb.insert(messages).values(value1));

    const subEffect = Query.subscribe(z, z.query.messages);
    expectTypeOf<Effect.Effect.Context<typeof subEffect>>().toEqualTypeOf<Scope.Scope>();
    const sub = yield* subEffect;

    {
      const result = yield* pipe(
        sub.changes,
        Stream.filter((d) => d.status === "complete"),
        waitForLastItem,
      );

      expect(result.data).toEqual([value1]);
    }

    const value2 = {
      id: nanoid(),
      body: "hello world 2",
    };
    yield* Effect.promise(() => ddb.insert(messages).values(value2));

    {
      const result = yield* pipe(
        sub.changes,
        Stream.filter((d) => d.status === "complete"),
        waitForLastItem,
      );

      expect(result.data).toHaveLength(2);
      expect(result.data).toEqual(expect.arrayContaining([value1, value2]));
    }
  }),
);

it.scopedLive(
  "custom mutators work",
  Effect.fn(function* () {
    const z = yield* initZero;

    const item: Message = { id: nanoid(), body: "Hello, world!" };
    yield* Effect.promise(() => z.mutate.messages.create(item).server);

    const result = yield* Effect.promise(() => rawDb`select id, body from messages`);
    expect(result.slice()).toEqual([item]);
  }),
);

it.scopedLive(
  "schema validation is applied to mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.tryPromise({
      try: async () => {
        const mut = z.mutate.messages.create({} as any)
        await mut.client.catch(Effect.fail)
        await mut.server.catch(Effect.fail)
        // expect.fail("should be unreachable");
      },
      catch: (e) => expect(e).toSatisfy(Predicate.isTagged("ClientArgsParseError")),
    });

    // expect(() => z.mutate.messages.create({} as any)).toThrowError(ClientArgsParseError);
    // yield* Effect.promise(() =>
    //   mut.client
    //     .catch((e) => {
    //       expect(e).toSatisfy(Predicate.isTagged("ClientArgsParseError"));
    //     })
    //     .then(() => expect.fail("should be unreachable")),
    // );
    // yield* Effect.promise(() =>
    //   mut.server
    //     .catch((e) => {
    //       expect(e).toSatisfy(Predicate.isTagged("ClientArgsParseError"));
    //     })
    //     .then(() => expect.fail("should be unreachable")),
    // );
  }),
);

it.scopedLive(
  "schema transformations are applied to mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    expectTypeOf<Parameters<typeof z.mutate.transformArgs>>().toEqualTypeOf<
      [Schema.Schema.Encoded<typeof mutatorSchema.transformArgs>]
    >();
    expectTypeOf<Parameters<typeof clientMutators.transformArgs>>().toEqualTypeOf<
      [Schema.Schema.Type<typeof mutatorSchema.transformArgs>]
    >();

    yield* Effect.promise(() => z.mutate.transformArgs({ foo: "1" }).server);

    expect(transformArgsClient).toBeCalledWith({ foo: 1 });
    expect(transformArgsServer).toBeCalledWith({ foo: 1 });
  }),
);

it.scopedLive(
  "mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() =>
      expect(z.mutate.throwsError().server).rejects.toEqual({
        error: "app",
        details: "error in throwsError",
      }),
    );
  }),
);

it.scopedLive(
  "mutator that throws error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.tryPromise({
      try: async () => {
        const mut = z.mutate.throwsErrorInsideTransaction()
        await mut.client.catch(Effect.fail)
        await mut.server.catch(Effect.fail)
      },
      catch: (e) => expect(e).toEqual({
        error: "app",
        details: "error in throwsErrorInsideTransaction",
      }),
    });
  }),
);

it.scopedLive(
  "mutator that throws error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.throwsErrorAfterTransaction().server).resolves.toBeDefined());
  }),
);

it.scopedLive(
  "client mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const mut = z.mutate.clientThrowsError();

    yield* Effect.promise(() => expect(mut.client).rejects.toThrowError("client error"));
    yield* Effect.promise(() => expect(mut.server).rejects.toThrowError("client error"));
  }),
);

it.scopedLive(
  "mutator that yields error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() =>
      expect(z.mutate.yieldsError().server).rejects.toEqual({
        error: "app",
        details: "error in yieldsError",
      }),
    );
  }),
);

it.scopedLive(
  "mutator that yields error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() =>
      expect(z.mutate.yieldsErrorInsideTransaction().server).rejects.toEqual({
        error: "app",
        details: "error in yieldsErrorInsideTransaction",
      }),
    );
  }),
);

it.scopedLive(
  "mutator that yields error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.yieldsErrorAfterTransaction().server).resolves.toBeDefined());
  }),
);

it.scopedLive(
  "mutator without transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.tryPromise({
      try: async () => {
        const mut = z.mutate.noTransaction()
        await mut.client.catch(Effect.fail)
        await mut.server.catch(Effect.fail)
      },
      catch: (e) => expect(e).toEqual({
        error: "app",
        details: "No transaction detected in a mutation, a transaction is required.",
      }),
    });
  }),
);

it.scopedLive(
  "mutator that invokes transaction more than once should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.doubleTransaction().server).resolves.toBeDefined());
  }),
);

it.scopedLive(
  "out of order mutations should be rejected",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => z.mutate.messages.create({ id: nanoid(), body: "hello world" }).server);

    // Simulate database corruption
    yield* Effect.promise(() => rawDb`truncate table zero_0.clients`);

    /*
    In case of "out of order" error, the mutation promise is not rejected, but retried under the hood.
    Since it won't ever be resolved or rejected in our case, we just run the mutation and check that
    the last server response was "oooMutation" error.
  */
    z.mutate.messages.create({ id: nanoid(), body: "hello world" }).server.then();

    yield* Effect.sleep(Duration.millis(100));

    const lastResponse = pipe(responses, Chunk.last, Option.getOrThrow);
    expect(lastResponse).toHaveProperty(["mutations", 0, "result", "error"], "oooMutation");
  }),
);

it.scopedLive(
  "non-existing mutator should reject",
  Effect.fn(function* () {
    const mutatorSchema = Mutators.schema({
      nonExistingMutator: Schema.Void,
    });
    const clientMutators = Mutators.make(mutatorSchema, {
      nonExistingMutator: Effect.fn(function* () {}),
    });
    const z = yield* Client.make(clientTransaction, clientMutators, {
      userID: "anon",
      server: "http://localhost:4848",
      mutateURL: "http://localhost:3000/push",
      onError,
    });

    yield* Effect.promise(() =>
      expect(z.mutate.nonExistingMutator().server).rejects.toEqual({
        error: "app",
        details: "Internal error",
      }),
    );
  }),
);

it.scopedLive(
  "concurrent transactions should be resolved",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.concurrentTransactions().server).resolves.toBeDefined());
  }),
);
