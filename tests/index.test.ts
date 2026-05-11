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
import { Atom, Registry } from "@effect-atom/atom";
import { createBuilder } from "@rocicorp/zero";
import { zeroDrizzle } from "@rocicorp/zero/server/adapters/drizzle";
import { count, eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { Chunk, Console, Context, Duration, Effect, Latch, Layer, Option, pipe, Schema, Stream } from "effect";
import * as Predicate from "effect/Predicate";
import * as SchemaGetter from "effect/SchemaGetter";
import * as ZfxClient from "effect-zero/client";
import * as ZfxClientTransaction from "effect-zero/client-transaction";
import * as ZfxMutators from "effect-zero/mutators";
import * as ZfxQuery from "effect-zero/query";
import * as ZfxResult from "effect-zero/result";
import * as ZfxServer from "effect-zero/server";
import * as ZfxServerTransaction from "effect-zero/server-transaction";
import { PushBody, PushParams, PushResponse } from "effect-zero/types/push";
import { TransformRequestMessage } from "effect-zero/types/queries";
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

const mutatorSchema = ZfxMutators.schema({
  messages: {
    create: Schema.Struct({
      id: Schema.String,
      body: Schema.String,
    }),
    // Nested mutator using Effect.fn with struct args
    updateMessage: Schema.Struct({
      id: Schema.String,
      body: Schema.String,
    }),
    // Nested mutator using Effect.fn with void args
    deleteAll: Schema.Void,
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

const clientTransaction = ZfxClientTransaction.make("ClientTransaction", schema);
const serverTransaction = ZfxServerTransaction.make("ServerTransaction", database, clientTransaction);

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

const clientMutators = ZfxMutators.make(mutatorSchema, {
  messages: {
    create: (msg) =>
      clientTransaction.use(async (tx) => {
        await tx.mutate.messages!.insert(msg);
      }),
    updateMessage: Effect.fn(function* (msg: { id: string; body: string }) {
      yield* clientTransaction.use(async (tx) => {
        await tx.mutate.messages!.update(msg);
      });
    }),
    deleteAll: Effect.fn(function* () {
      yield* clientTransaction.use(async (_tx) => {});
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

const serverMutators = ZfxMutators.make(mutatorSchema, {
  messages: {
    create: (msg) => clientMutators.messages.create(msg).pipe(serverTransaction.execute),
    updateMessage: Effect.fn(function* (msg: { id: string; body: string }) {
      yield* clientMutators.messages.updateMessage(msg).pipe(serverTransaction.execute);
    }),
    deleteAll: Effect.fn(function* () {
      yield* clientMutators.messages.deleteAll().pipe(serverTransaction.execute);
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
        await tx.dbTransaction.wrappedTransaction.insert(messages).values({ id: nanoid(), body: "hello world" });
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
        await tx.mutate.messages!.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
    yield* Effect.fail(new Error("error in yieldsErrorAfterTransaction"));
  }),
  noTransaction: Effect.fn(function* () {}),
  doubleTransaction: Effect.fn(function* () {
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages!.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
    yield* serverTransaction
      .use(async (tx) => {
        await tx.mutate.messages!.insert({ id: nanoid(), body: "hello world" });
      })
      .pipe(serverTransaction.execute);
  }),
  concurrentTransactions: Effect.fn(function* () {
    yield* Effect.all(
      [
        serverTransaction
          .use(async (tx) => {
            await tx.mutate.messages!.insert({ id: nanoid(), body: "hello world" });
          })
          .pipe(serverTransaction.execute),
        serverTransaction
          .use(async (tx) => {
            await tx.mutate.messages!.insert({ id: nanoid(), body: "hello world" });
          })
          .pipe(serverTransaction.execute),
      ],
      { concurrency: "unbounded" },
    );
  }),
});

const builder = createBuilder(schema);

const messageByIdQuery = ZfxQuery.make({
  name: "messageById",
  payload: Schema.String,
  query: Effect.fn(function* (id) {
    return yield* Effect.succeed(builder.messages!.where("id", id).one());
  }),
});

const messagesQuery = ZfxQuery.make({
  name: "messages",
  payload: Schema.Void,
  query: Effect.fn(function* () {
    // TODO: figure out why base queries (without `.limit()`) don't receive `completed` events
    return yield* Effect.succeed(builder.messages!.limit(100));
  }),
});

const transformArgsQuerySpy = vi.fn();

const DateFromNumber = Schema.Number.pipe(
  Schema.decodeTo(Schema.Date, {
    decode: SchemaGetter.transform((n: number) => new Date(n)),
    encode: SchemaGetter.transform((d: Date) => d.getTime()),
  }),
);

const transformArgsQuery = ZfxQuery.make({
  name: "transformArgs",
  payload: DateFromNumber,
  query: Effect.fn(function* (a) {
    expectTypeOf<typeof a>().toEqualTypeOf<Date>();
    transformArgsQuerySpy(a);
    return yield* Effect.succeed(builder.messages!);
  }),
});

const queries = [messageByIdQuery, messagesQuery, transformArgsQuery] satisfies ZfxQuery.MakeQueryResult[];

const waitForLastItem = Effect.fn("waitForLastItem")(
  <A, E, R>(stream: Stream.Stream<A, E, R>, timeout?: Duration.Duration) => {
    return pipe(
      stream,
      Stream.timeout(timeout ?? Duration.millis(200)),
      Stream.runLast,
      Effect.flatMap(Effect.fromOption),
      Effect.catchTag("NoSuchElementError", () => Effect.fail(new Error("No items received from server"))),
    );
  },
);

const initZero = Effect.gen(function* () {
  const z = yield* ZfxClient.make(clientTransaction, clientMutators, {
    userID: "anon",
    server: "http://localhost:4848",
    mutateURL: "http://localhost:3000/push",
    queryURL: "http://localhost:3000/query",
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
    const q = yield* messagesQuery();
    const stream = ZfxQuery.stream(z, q);
    yield* pipe(
      stream,
      Stream.filter((d) => Predicate.isTagged(d, "Complete")),
      waitForLastItem,
      Effect.andThen((d) =>
        Effect.fail(new Error("not empty")).pipe(Effect.when(Effect.sync(() => d.value.length > 0))),
      ),
    );
  }

  return z;
});

let responses = Chunk.empty<PushResponse>();

beforeAll(async () => {
  const router = HttpRouter.empty.pipe(
    HttpRouter.post(
      "/push",
      Effect.gen(function* () {
        const params = yield* HttpRouter.schemaParams(PushParams);
        const payload = yield* HttpServerRequest.schemaBodyJson(PushBody);
        const result = yield* ZfxServer.processPush(serverTransaction, serverMutators, params, payload);
        const responseBody = yield* Schema.encodeEffect(PushResponse)(result);

        responses = Chunk.append(responses, responseBody);

        return (yield* HttpServerResponse.json(responseBody)).pipe(
          HttpServerResponse.setStatus(200),
          HttpServerResponse.setHeader("content-type", "application/json"),
        );
      }).pipe(
        Effect.catch((e) =>
          Effect.gen(function* () {
            yield* Console.error("Push processor error:", e);
            return HttpServerResponse.empty({ status: 500 });
          }),
        ),
      ),
    ),
    HttpRouter.post(
      "/query",
      Effect.gen(function* () {
        const payload = yield* HttpServerRequest.schemaBodyJson(TransformRequestMessage);
        const response = yield* ZfxServer.handleQuery(queries, schema, payload);
        return yield* HttpServerResponse.json(response);
      }).pipe(
        Effect.catchCause(
          Effect.fn(function* (c) {
            yield* Effect.logError("query error:", c);
            return HttpServerResponse.empty({ status: 500 });
          }),
        ),
      ),
    ),
    HttpRouter.get("/health", HttpServerResponse.text("OK")),
  );

  const server = Effect.gen(function* () {
    const serverStarted = yield* Latch.make();
    const app = router.pipe(
      HttpServer.serve(),
      Layer.provide(NodeHttpServer.layer(() => createServer(), { port: 3000 })),
      Layer.tap(() => serverStarted.open),
    );
    yield* Layer.launch(app).pipe(Effect.forkDetach);
    yield* serverStarted.await;
  }).pipe(Effect.provide(FetchHttpClient.layer));

  await Effect.runPromise(server);
});

beforeEach(() => {
  responses = Chunk.empty<PushResponse>();
});

test("server is running", async () => {
  const response = await fetch("http://localhost:3000/health");
  await expect(response.text()).resolves.toEqual("OK");
});

test("processPush has no extra requirements", () => {
  const effect = ZfxServer.processPush(serverTransaction, serverMutators, {} as any, {} as any);

  expectTypeOf<Effect.Services<typeof effect>>().toEqualTypeOf<never>();
});

test("mutators should have correct argument types", () => {
  expectTypeOf<Parameters<typeof clientMutators.optionalVoidArg>>().toEqualTypeOf<[(void | undefined)?]>();
  expectTypeOf<Parameters<typeof clientMutators.throwsError>>().toEqualTypeOf<[]>();
  expectTypeOf<Parameters<typeof clientMutators.messages.create>>().toEqualTypeOf<
    [Schema.Schema.Type<typeof mutatorSchema.messages.create>]
  >();
});

test("nested mutators using Effect.fn should have correct argument types", () => {
  // Nested mutator with struct args using Effect.fn
  expectTypeOf<Parameters<typeof clientMutators.messages.updateMessage>>().toEqualTypeOf<
    [{ id: string; body: string }]
  >();

  // Nested mutator with void args using Effect.fn
  expectTypeOf<Parameters<typeof clientMutators.messages.deleteAll>>().toEqualTypeOf<[]>();

  // Server mutators should also have correct types
  expectTypeOf<Parameters<typeof serverMutators.messages.updateMessage>>().toEqualTypeOf<
    [{ id: string; body: string }]
  >();
  expectTypeOf<Parameters<typeof serverMutators.messages.deleteAll>>().toEqualTypeOf<[]>();
});

test("mutator requirements should propagate", () => {
  class DummyTag extends Context.Service<DummyTag, {}>()("effect-zero/DummyTag") {}

  class DummyTag2 extends Context.Service<DummyTag2, {}>()("effect-zero/DummyTag2") {}

  const clientTransaction = ZfxClientTransaction.make("DummyClientTransaction", schema);
  const serverTransaction = ZfxServerTransaction.make("DummyServerTransaction", database, clientTransaction);
  const mutatorSchema = ZfxMutators.schema({
    dummy: Schema.Void,
    dummy2: Schema.Void,
  });
  const mutators = ZfxMutators.make(mutatorSchema, {
    dummy: Effect.fn(function* () {
      yield* DummyTag;
    }),
    dummy2: Effect.fn(function* () {
      yield* DummyTag2;
    }),
  });
  const serverEffect = ZfxServer.processPush(serverTransaction, mutators, {} as any, {} as any);

  expectTypeOf<Effect.Services<typeof serverEffect>>().toEqualTypeOf<DummyTag | DummyTag2>();
});

it.live(
  "rows are returned after data is inserted",
  Effect.fn(function* () {
    const z = yield* initZero;

    const value1 = {
      id: nanoid(),
      body: "hello world",
    };
    yield* Effect.promise(() => ddb.insert(messages).values(value1));

    const q = yield* messagesQuery();
    const stream = ZfxQuery.stream(z, q);

    {
      const result = yield* pipe(
        stream,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        waitForLastItem,
      );

      expect(result.value).toEqual([value1]);
    }

    const value2 = {
      id: nanoid(),
      body: "hello world 2",
    };
    yield* Effect.promise(() => ddb.insert(messages).values(value2));

    {
      const result = yield* pipe(
        stream,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        waitForLastItem,
      );

      expect(result.value).toHaveLength(2);
      expect(result.value).toEqual(expect.arrayContaining([value1, value2]));
    }
  }),
);

it.live(
  "custom mutators work",
  Effect.fn(function* () {
    const z = yield* initZero;

    const item: Message = { id: nanoid(), body: "Hello, world!" };
    const mut = z.mutate.messages.create(item);

    const clientResult = yield* Effect.promise(() => mut.client);
    expect(clientResult).toEqual({ type: "success" });

    const serverResult = yield* Effect.promise(() => mut.server);
    expect(serverResult).toEqual({ type: "success" });

    const result = yield* Effect.promise(() => ddb.select({ id: messages.id, body: messages.body }).from(messages));
    expect(result).toEqual([item]);
  }),
);

it.live(
  "nested mutators using Effect.fn work",
  Effect.fn(function* () {
    const z = yield* initZero;

    // First create a message
    const item: Message = { id: nanoid(), body: "Original body" };
    yield* Effect.promise(() => z.mutate.messages.create(item).server);

    // Update it using the nested Effect.fn mutator
    const updatedItem: Message = { id: item.id, body: "Updated body" };
    const updateMut = z.mutate.messages.updateMessage(updatedItem);

    const updateClientResult = yield* Effect.promise(() => updateMut.client);
    expect(updateClientResult).toEqual({ type: "success" });

    const updateServerResult = yield* Effect.promise(() => updateMut.server);
    expect(updateServerResult).toEqual({ type: "success" });

    // Verify the update
    const result = yield* Effect.promise(() =>
      ddb.select({ id: messages.id, body: messages.body }).from(messages).where(eq(messages.id, item.id)),
    );
    expect(result).toEqual([updatedItem]);
  }),
);

it.live(
  "schema validation is applied to mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.tryPromise({
      try: async () => {
        const mut = z.mutate.messages.create({} as any);
        await mut.client.catch(Effect.fail);
        await mut.server.catch(Effect.fail);
      },
      catch: (e) => {
        expect(e).toSatisfy(Predicate.isTagged("ClientArgsParseError"));
      },
    });
  }),
);

it.live(
  "schema transformations are applied to mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    expectTypeOf<Parameters<typeof z.mutate.transformArgs>>().toEqualTypeOf<
      [Schema.Codec.Encoded<typeof mutatorSchema.transformArgs>]
    >();
    expectTypeOf<Parameters<typeof clientMutators.transformArgs>>().toEqualTypeOf<
      [Schema.Schema.Type<typeof mutatorSchema.transformArgs>]
    >();

    yield* Effect.promise(() => z.mutate.transformArgs({ foo: "1" }).server);

    expect(transformArgsClient).toBeCalledWith({ foo: 1 });
    expect(transformArgsServer).toBeCalledWith({ foo: 1 });
  }),
);

it.live(
  "mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate.throwsError().server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        details: "error in throwsError",
      }),
    });
  }),
);

it.live(
  "mutator that throws error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.tryPromise({
      try: async () => {
        const mut = z.mutate.throwsErrorInsideTransaction();
        await mut.server.catch(Effect.fail);
      },
      catch: (e) =>
        expect(e).toEqual({
          error: "app",
          details: "error in throwsErrorInsideTransaction",
        }),
    });
  }),
);

it.live(
  "mutator that throws error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.throwsErrorAfterTransaction().server).resolves.toBeDefined());
  }),
);

it.live(
  "client mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const mut = z.mutate.clientThrowsError();

    const clientResult = yield* Effect.promise(() => mut.client);
    expect(clientResult).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "client error",
      }),
    });
    const serverResult = yield* Effect.promise(() => mut.server);
    expect(serverResult).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "client error",
      }),
    });
  }),
);

it.live(
  "mutator that yields error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate.yieldsError().server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        details: "error in yieldsError",
      }),
    });
  }),
);

it.live(
  "mutator that yields error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate.yieldsErrorInsideTransaction().server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        details: "error in yieldsErrorInsideTransaction",
      }),
    });
  }),
);

it.live(
  "mutator that yields error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.yieldsErrorAfterTransaction().server).resolves.toBeDefined());
  }),
);

it.live(
  "mutator without transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate.noTransaction().server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        details: "No transaction detected in a mutation, a transaction is required.",
      }),
    });
  }),
);

it.live(
  "mutator that invokes transaction more than once should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.doubleTransaction().server).resolves.toBeDefined());
  }),
);

it.live(
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

it.live(
  "non-existing mutator should reject",
  Effect.fn(function* () {
    const mutatorSchema = ZfxMutators.schema({
      nonExistingMutator: Schema.Void,
    });
    const clientMutators = ZfxMutators.make(mutatorSchema, {
      nonExistingMutator: Effect.fn(function* () {}),
    });
    const z = yield* ZfxClient.make(clientTransaction, clientMutators, {
      userID: "anon",
      server: "http://localhost:4848",
      mutateURL: "http://localhost:3000/push",
    });

    const result = yield* Effect.promise(() => z.mutate.nonExistingMutator().server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        details: "Internal error",
      }),
    });
  }),
);

it.live(
  "concurrent transactions should be resolved",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate.concurrentTransactions().server).resolves.toBeDefined());
  }),
);

it.live(
  "synced queries should work",
  Effect.fn(function* () {
    const z = yield* initZero;

    const value: Message = { id: nanoid(), body: "Hello, world!" };
    yield* Effect.promise(() => ddb.insert(messages).values(value));

    const q = yield* messageByIdQuery(value.id);
    const stream = ZfxQuery.stream(z, q);
    const q2 = yield* messagesQuery();
    const stream2 = ZfxQuery.stream(z, q2);

    {
      const result = yield* pipe(
        stream,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        (s) => waitForLastItem(s, Duration.millis(500)),
      );

      expect(result.value).toEqual(value);
    }

    {
      const result = yield* pipe(
        stream2,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        (s) => waitForLastItem(s, Duration.millis(500)),
      );

      expect(result.value).toEqual([value]);
    }

    {
      const value2: Message = { id: nanoid(), body: "Hello, world!" };
      yield* Effect.promise(() => ddb.insert(messages).values(value2));

      const result = yield* pipe(
        stream2,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        (s) => waitForLastItem(s, Duration.millis(500)),
      );

      expect(result.value).toEqual(expect.arrayContaining([value, value2]));
    }
  }),
  10000,
);

it.live(
  "transformArgsQuery should accept and passthrough encoded non-JSON args",
  Effect.fn(function* () {
    expectTypeOf<Parameters<typeof transformArgsQuery>>().toEqualTypeOf<[Date]>();

    const now = new Date();
    const query = yield* transformArgsQuery(now);
    const zero = yield* initZero;
    yield* Effect.promise(() => zero.run(query));

    expect(transformArgsQuerySpy).toHaveBeenCalledWith(now);
  }),
);

it.live(
  "Query.subscribable should work for singular queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const value: Message = { id: nanoid(), body: "Hello, world!" };
    yield* Effect.promise(() => ddb.insert(messages).values(value));

    const q = yield* messageByIdQuery(value.id);
    const sub = ZfxQuery.subscribable(z, q);

    expect(yield* sub.get).toEqual({ _tag: "Partial", value: undefined });

    {
      const result = yield* pipe(
        sub.changes,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        waitForLastItem,
      );

      expect(result.value).toEqual(value);
    }
  }),
);

it.live(
  "Query.subscribable.get should always return the latest value for singular queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const value: Message = { id: nanoid(), body: "Hello, world!" };
    yield* Effect.promise(() => ddb.insert(messages).values(value));

    const q = yield* messageByIdQuery(value.id);
    const sub = ZfxQuery.subscribable(z, q);

    expect(yield* sub.get).toEqual(expect.objectContaining({ _tag: "Partial", value: undefined }));

    yield* Effect.sleep(Duration.millis(500));

    expect(yield* sub.get).toEqual({ _tag: "Partial", value: expect.objectContaining(value) });
  }),
);

it.live(
  "Query.subscribable.get should always return the latest value for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const values: Message[] = [
      { id: nanoid(), body: "Hello, world!" },
      { id: nanoid(), body: "Hello, world!" },
    ];
    yield* Effect.promise(() => ddb.insert(messages).values(values));

    const q = yield* messagesQuery();
    const sub = ZfxQuery.subscribable(z, q);

    expect(yield* sub.get).toEqual(expect.objectContaining({ _tag: "Partial", value: [] }));

    yield* Effect.sleep(Duration.millis(500));

    expect(yield* sub.get).toEqual({
      _tag: "Partial",
      value: expect.arrayContaining(values.map((v) => expect.objectContaining(v))),
    });
  }),
);

it.live(
  "Query.subscribable should work for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    Atom.subscriptionRef;

    const values: Message[] = [
      { id: nanoid(), body: "Hello, world!" },
      { id: nanoid(), body: "Hello, world!" },
    ];
    yield* Effect.promise(() => ddb.insert(messages).values(values));

    const q = yield* messagesQuery();
    const sub = ZfxQuery.subscribable(z, q);

    expect(yield* sub.get).toEqual({ _tag: "Partial", value: [] });

    {
      const result = yield* pipe(
        sub.changes,
        Stream.filter((d) => Predicate.isTagged(d, "Complete")),
        waitForLastItem,
      );

      expect(result.value).toEqual(expect.arrayContaining(values));
    }
  }),
);

it.live(
  "Result.acceptPartialMode with 'acceptCached' should work for singular queries",
  Effect.fn(function* () {
    const value: Message = { id: nanoid(), body: "hello world" };

    const z = yield* initZero;

    const q = yield* messageByIdQuery(value.id);
    const sub = ZfxQuery.subscribable(z, q);
    const atom = Atom.subscribable(Effect.succeed(sub)).pipe(
      Atom.map(ZfxResult.make),
      Atom.map(ZfxResult.acceptPartialMode("acceptCached")),
    );

    yield* Effect.promise(() => ddb.insert(messages).values(value));
    yield* Effect.sleep(Duration.millis(200));

    const stream = atom.pipe(
      Atom.toStream,
      Stream.filter((v) => Predicate.isTagged(v, "Success")),
      Stream.timeout(Duration.millis(200)),
    );

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes[changes.length - 1]).toEqual(expect.objectContaining({ _tag: "Success", value }));
    }

    const newBody = "hello world 2";
    const newValue: Message = { id: value.id, body: newBody };
    yield* Effect.promise(() => ddb.update(messages).set({ body: newBody }).where(eq(messages.id, value.id)));

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([
        expect.objectContaining({ _tag: "Success", value }),
        expect.objectContaining({ _tag: "Success", value: newValue }),
      ]);
    }
  }, Effect.provide(Registry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'acceptCached' should work for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const q = yield* messagesQuery();
    const sub = ZfxQuery.subscribable(z, q);
    const atom = Atom.subscribable(Effect.succeed(sub)).pipe(
      Atom.map(ZfxResult.make),
      Atom.map(ZfxResult.acceptPartialMode("acceptCached")),
    );

    const values: Message[] = [
      { id: nanoid(), body: "hello world" },
      { id: nanoid(), body: "hello world 2" },
    ];
    yield* Effect.promise(() => ddb.insert(messages).values(values));
    yield* Effect.sleep(Duration.millis(200));

    const stream = atom.pipe(
      Atom.toStream,
      Stream.filter((v) => Predicate.isTagged(v, "Success")),
      Stream.timeout(Duration.millis(200)),
    );
    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes[changes.length - 1]).toEqual(
        expect.objectContaining({ _tag: "Success", value: expect.arrayContaining(values) }),
      );
    }

    const newValue: Message = { id: nanoid(), body: "hello world 3" };
    yield* Effect.promise(() => ddb.insert(messages).values(newValue));

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([
        expect.objectContaining({ _tag: "Success", value: expect.arrayContaining(values) }),
        expect.objectContaining({ _tag: "Success", value: expect.arrayContaining([...values, newValue]) }),
      ]);
    }
  }, Effect.provide(Registry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'waitForServer' should work for singular queries",
  Effect.fn(function* () {
    const value: Message = { id: nanoid(), body: "hello world" };

    const z = yield* initZero;

    const q = yield* messageByIdQuery(value.id);
    const sub = ZfxQuery.subscribable(z, q);
    const atom = Atom.subscribable(Effect.succeed(sub)).pipe(
      Atom.map(ZfxResult.make),
      Atom.map(ZfxResult.acceptPartialMode("waitForServer")),
    );

    yield* Effect.promise(() => ddb.insert(messages).values(value));
    yield* Effect.sleep(Duration.millis(200));

    const stream = atom.pipe(
      Atom.toStream,
      Stream.filter((v) => Predicate.isTagged(v, "Success")),
      Stream.timeout(Duration.millis(200)),
    );

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([expect.objectContaining({ _tag: "Success", value })]);
    }

    const newBody = "hello world 2";
    const newValue: Message = { id: value.id, body: newBody };
    yield* Effect.promise(() => ddb.update(messages).set({ body: newBody }).where(eq(messages.id, value.id)));
    yield* Effect.sleep(Duration.millis(200));

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([expect.objectContaining({ _tag: "Success", value: newValue })]);
    }
  }, Effect.provide(Registry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'waitForServer' should work for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const q = yield* messagesQuery();
    const sub = ZfxQuery.subscribable(z, q);
    const atom = Atom.subscribable(Effect.succeed(sub)).pipe(
      Atom.map(ZfxResult.make),
      Atom.map(ZfxResult.acceptPartialMode("waitForServer")),
    );

    const values: Message[] = [
      { id: nanoid(), body: "hello world" },
      { id: nanoid(), body: "hello world 2" },
    ];
    yield* Effect.promise(() => ddb.insert(messages).values(values));
    yield* Effect.sleep(Duration.millis(200));

    const stream = atom.pipe(
      Atom.toStream,
      Stream.filter((v) => Predicate.isTagged(v, "Success")),
      Stream.timeout(Duration.millis(200)),
    );
    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([expect.objectContaining({ _tag: "Success", value: expect.arrayContaining(values) })]);
    }

    const newValue: Message = { id: nanoid(), body: "hello world 3" };
    yield* Effect.promise(() => ddb.insert(messages).values(newValue));
    yield* Effect.sleep(Duration.millis(200));

    {
      const changes = yield* stream.pipe(Stream.runCollect);

      expect(changes).toEqual([
        expect.objectContaining({ _tag: "Success", value: expect.arrayContaining([...values, newValue]) }),
      ]);
    }
  }, Effect.provide(Registry.layer)),
);
