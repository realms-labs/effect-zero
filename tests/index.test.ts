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
import { NodeHttpServer } from "@effect/platform-node";
import { beforeAll, expect, expectTypeOf, it, test, vi } from "@effect/vitest";
import { createBuilder } from "@rocicorp/zero";
import { zeroDrizzle } from "@rocicorp/zero/server/adapters/drizzle";
import { count, eq } from "drizzle-orm";
import { drizzle } from "drizzle-orm/postgres-js";
import { Chunk, Console, Duration, Effect, Latch, Layer, Logger, Option, pipe, Schema, Stream } from "effect";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as Predicate from "effect/Predicate";
import * as Atom from "effect/reactivity/Atom";
import * as AtomRegistry from "effect/reactivity/AtomRegistry";
import * as SchemaGetter from "effect/SchemaGetter";
import * as ZfxClient from "effect-zero/client";
import * as ZfxClientTransaction from "effect-zero/client-transaction";
import * as ZfxMutator from "effect-zero/mutator";
import * as ZfxQuery from "effect-zero/query";
import * as ZfxResult from "effect-zero/result";
import * as ZfxServer from "effect-zero/server";
import * as ZfxServerTransaction from "effect-zero/server-transaction";
import { TransformRequestMessage } from "effect-zero/types/custom-queries";
import { MutateParams, type MutateResponse } from "effect-zero/types/mutate-server";
import { PushBody } from "effect-zero/types/push";
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

const noop = ZfxMutator.make({ payload: Schema.Void, mutator: Effect.fn(function* () {}) });

const mutators = ZfxMutator.registry(clientTransaction, {
  messages: {
    create: ZfxMutator.make({
      payload: Schema.Struct({ id: Schema.String, body: Schema.String }),
      mutator: (msg) =>
        clientTransaction.use(async (tx) => {
          await tx.mutate.messages.insert(msg);
        }),
    }),
    updateMessage: ZfxMutator.make({
      payload: Schema.Struct({ id: Schema.String, body: Schema.String }),
      mutator: Effect.fn(function* (msg) {
        yield* clientTransaction.use(async (tx) => {
          await tx.mutate.messages.update(msg);
        });
      }),
    }),
    deleteAll: ZfxMutator.make({
      payload: Schema.Void,
      mutator: Effect.fn(function* () {
        yield* clientTransaction.use(async (_tx) => {});
      }),
    }),
  },
  optionalVoidArg: noop,
  nullableArg: ZfxMutator.make({
    payload: Schema.NullOr(Schema.Struct({ id: Schema.String, body: Schema.String })),
    mutator: Effect.fn(function* (msg) {
      yield* clientTransaction.use(async (tx) => {
        if (msg !== null) await tx.mutate.messages.insert(msg);
      });
    }),
  }),
  transformArgs: ZfxMutator.make({
    payload: Schema.Struct({ foo: Schema.NumberFromString }),
    mutator: Effect.fn(function* (a) {
      return yield* transformArgsClient(a);
    }),
  }),
  throwsError: noop,
  throwsErrorInsideTransaction: noop,
  throwsErrorAfterTransaction: noop,
  clientThrowsError: ZfxMutator.make({
    payload: Schema.Void,
    mutator: Effect.fn(function* () {
      yield* Effect.void;
      throw new Error("client error");
    }),
  }),
  yieldsError: noop,
  yieldsErrorInsideTransaction: noop,
  yieldsErrorAfterTransaction: noop,
  noTransaction: noop,
  doubleTransaction: noop,
  concurrentTransactions: noop,
  rawInsertDuplicate: ZfxMutator.make({
    payload: Schema.String,
    mutator: Effect.fn(function* (_id) {
      yield* clientTransaction.use(async () => {});
    }),
  }),
});

// Server-only behaviour. Mutators that aren't overridden run their shared body in a server transaction.
const serverMutators = ZfxMutator.server(mutators, {
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
  // No transaction at all, which the server rejects.
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
  // A plain Drizzle insert (no `ON CONFLICT`): since Zero 1.9, CRUD `insert` ignores primary-key collisions.
  rawInsertDuplicate: Effect.fn(function* (id) {
    yield* serverTransaction
      .use(async (tx) => {
        await tx.dbTransaction.wrappedTransaction.insert(messages).values({ id, body: "dup" });
      })
      .pipe(serverTransaction.execute);
  }),
});

const builder = createBuilder(schema);

const messageByIdQuery = ZfxQuery.make({
  name: "messageById",
  payload: Schema.String,
  query: Effect.fn(function* (id) {
    return yield* Effect.succeed(builder.messages.where("id", id).one());
  }),
});

const messagesQuery = ZfxQuery.make({
  name: "messages",
  payload: Schema.Void,
  query: Effect.fn(function* () {
    // TODO: figure out why base queries (without `.limit()`) don't receive `completed` events
    return yield* Effect.succeed(builder.messages.limit(100));
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
    return yield* Effect.succeed(builder.messages);
  }),
});

const queries = [messageByIdQuery, messagesQuery, transformArgsQuery] satisfies ZfxQuery.MakeQueryResult[];

// Waits for a complete result that matches, rather than the last result within a fixed window: zero-cache can make
// a client reconnect (e.g. after a test truncates a table), which delays its results.
const waitForComplete = <A extends { readonly _tag: string }, E, R>(
  stream: Stream.Stream<A, E, R>,
  predicate: (result: Extract<A, { readonly _tag: "Complete" }>) => boolean,
) =>
  stream.pipe(
    Stream.filter((result): result is Extract<A, { readonly _tag: "Complete" }> => result._tag === "Complete"),
    Stream.filter(predicate),
    Stream.runHead,
    Effect.flatMap(Effect.fromOption),
    Effect.timeout(Duration.seconds(10)),
  );

const initZero = Effect.gen(function* () {
  const z = yield* ZfxClient.make(mutators, {
    userID: null,
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

    // Wait until the view has synced the truncation. zero-cache can make a new client reconnect first, which
    // delays its first sync past a fixed window.
    const q = yield* messagesQuery();
    yield* ZfxQuery.stream(z, q).pipe(
      Stream.filter((d) => Predicate.isTagged(d, "Complete") && d.value.length === 0),
      Stream.runHead,
      Effect.timeout(Duration.seconds(10)),
    );
  }

  return z;
});

let responses = Chunk.empty<MutateResponse>();

beforeAll(async () => {
  const pushRoute = HttpRouter.add(
    "POST",
    "/push",
    Effect.gen(function* () {
      const params = yield* HttpRouter.schemaParams(MutateParams);
      const payload = yield* HttpServerRequest.schemaBodyJson(PushBody);
      // `handleMutate` returns the upstream push response already in wire format, so no encoding step.
      // No auth in this test harness, so the client is logged out (`userId: undefined`).
      const result = yield* ZfxServer.handleMutate({
        transaction: serverTransaction,
        mutators: serverMutators,
        query: params,
        body: payload,
        userId: undefined,
      });

      responses = Chunk.append(responses, result);

      return (yield* HttpServerResponse.json(result)).pipe(
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
  );

  const queryRoute = HttpRouter.add(
    "POST",
    "/query",
    Effect.gen(function* () {
      const payload = yield* HttpServerRequest.schemaBodyJson(TransformRequestMessage);
      const response = yield* ZfxServer.handleQuery({ queries, schema, body: payload, userId: undefined });
      return yield* HttpServerResponse.json(response);
    }).pipe(
      Effect.catchCause(
        Effect.fn(function* (c) {
          yield* Effect.logError("query error:", c);
          return HttpServerResponse.empty({ status: 500 });
        }),
      ),
    ),
  );

  const healthRoute = HttpRouter.add("GET", "/health", HttpServerResponse.text("OK"));

  const routes = Layer.mergeAll(pushRoute, queryRoute, healthRoute);

  const server = Effect.gen(function* () {
    const serverStarted = yield* Latch.make();
    const app = routes.pipe(
      HttpRouter.serve,
      Layer.provide(NodeHttpServer.layer(() => createServer(), { port: 3000 })),
      Layer.tap(() => serverStarted.open),
    );
    yield* Layer.launch(app).pipe(Effect.forkDetach);
    yield* serverStarted.await;
  }).pipe(Effect.provide(FetchHttpClient.layer));

  await Effect.runPromise(server);
});

beforeEach(() => {
  responses = Chunk.empty<MutateResponse>();
});

test("server is running", async () => {
  const response = await fetch("http://localhost:3000/health");
  await expect(response.text()).resolves.toEqual("OK");
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
      const result = yield* waitForComplete(stream, (result) => result.value.length === 1);
      expect(result.value).toEqual([value1]);
    }

    const value2 = {
      id: nanoid(),
      body: "hello world 2",
    };
    yield* Effect.promise(() => ddb.insert(messages).values(value2));

    {
      const result = yield* waitForComplete(stream, (result) => result.value.length === 2);
      expect(result.value).toEqual(expect.arrayContaining([value1, value2]));
    }
  }),
);

it.live(
  "mutators create and update rows",
  Effect.fn(function* () {
    const z = yield* initZero;

    const item: Message = { id: nanoid(), body: "Original body" };
    const create = z.mutate(mutators.messages.create(item));
    expect(yield* Effect.promise(() => create.client)).toEqual({ type: "success" });
    expect(yield* Effect.promise(() => create.server)).toEqual({ type: "success" });

    const updatedItem: Message = { id: item.id, body: "Updated body" };
    expect(yield* Effect.promise(() => z.mutate(mutators.messages.updateMessage(updatedItem)).server)).toEqual({
      type: "success",
    });

    const result = yield* Effect.promise(() => ddb.select({ id: messages.id, body: messages.body }).from(messages));
    expect(result).toEqual([updatedItem]);
  }),
);

it.live(
  "schema validation rejects invalid mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    // Calling a mutator encodes its args, so invalid args throw right away.
    expect(() => mutators.messages.create({} as any)).toThrow();

    // Args that reach the client mutator malformed (here, a hand-made request) fail its decoding. That surfaces
    // as a `ClientArgsParseError` app error, whose `message` getter masks the schema detail.
    const request = mutators.messages.create({ id: nanoid(), body: "valid" });
    const result = yield* Effect.promise(() => z.mutate({ ...request, args: {} }).client);
    expect(result).toMatchObject({ type: "error", error: { type: "app", message: "Internal error" } });
    expect(JSON.stringify(result)).not.toContain("Parsed message");
  }),
);

it.live(
  "schema transformations are applied to mutator arguments",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => z.mutate(mutators.transformArgs({ foo: 1 })).server);

    expect(transformArgsClient).toBeCalledWith({ foo: 1 });
    expect(transformArgsServer).toBeCalledWith({ foo: 1 });
  }),
);

it.live(
  "mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate(mutators.throwsError()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "error in throwsError",
      }),
    });
  }),
);

it.live(
  "mutator that throws error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate(mutators.throwsErrorInsideTransaction()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "error in throwsErrorInsideTransaction",
      }),
    });
  }),
);

it.live(
  "mutator that throws error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate(mutators.throwsErrorAfterTransaction()).server).resolves.toBeDefined());
  }),
);

it.live(
  "client mutator that throws error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const mut = z.mutate(mutators.clientThrowsError());

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
  "Zero's own logs go through Effect's logger",
  Effect.fn(function* () {
    const logged: Array<{ readonly message: unknown; readonly logLevel: string }> = [];
    const z = yield* ZfxClient.make(mutators, {
      userID: null,
      server: "http://localhost:4848",
      mutateURL: "http://localhost:3000/push",
    }).pipe(Effect.provide(Logger.layer([Logger.make(({ message, logLevel }) => logged.push({ message, logLevel }))])));

    // Zero logs a client mutator's failure as an error.
    yield* Effect.promise(() => z.mutate(mutators.clientThrowsError()).client);
    yield* Effect.sleep(Duration.millis(50));

    expect(logged).toContainEqual({
      message: expect.arrayContaining([expect.stringContaining('Mutator "clientThrowsError" app error on client')]),
      logLevel: "Error",
    });
  }),
);

it.live(
  "a logSink option overrides the Effect logger",
  Effect.fn(function* () {
    const effectLogs: unknown[] = [];
    const sinkLogs: unknown[] = [];
    const z = yield* ZfxClient.make(mutators, {
      userID: null,
      server: "http://localhost:4848",
      mutateURL: "http://localhost:3000/push",
      logSink: { log: (_level, _context, ...args) => sinkLogs.push(args) },
    }).pipe(Effect.provide(Logger.layer([Logger.make(({ message }) => effectLogs.push(message))])));

    yield* Effect.promise(() => z.mutate(mutators.clientThrowsError()).client);
    yield* Effect.sleep(Duration.millis(50));

    expect(sinkLogs).toContainEqual(
      expect.arrayContaining([expect.stringContaining('Mutator "clientThrowsError" app error on client')]),
    );
    expect(effectLogs).not.toContainEqual(
      expect.arrayContaining([expect.stringContaining('Mutator "clientThrowsError" app error on client')]),
    );
  }),
);

it.live(
  "mutator that yields error should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate(mutators.yieldsError()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "error in yieldsError",
      }),
    });
  }),
);

it.live(
  "mutator that yields error inside transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate(mutators.yieldsErrorInsideTransaction()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "error in yieldsErrorInsideTransaction",
      }),
    });
  }),
);

it.live(
  "mutator that yields error after transaction should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate(mutators.yieldsErrorAfterTransaction()).server).resolves.toBeDefined());
  }),
);

it.live(
  "mutator without transaction should reject",
  Effect.fn(function* () {
    const z = yield* initZero;

    const result = yield* Effect.promise(() => z.mutate(mutators.noTransaction()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "No transaction detected in a mutation, a transaction is required.",
      }),
    });
  }),
);

it.live(
  "mutator that invokes transaction more than once should resolve",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate(mutators.doubleTransaction()).server).resolves.toBeDefined());
  }),
);

it.live(
  "out of order mutations should be rejected",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => z.mutate(mutators.messages.create({ id: nanoid(), body: "hello world" })).server);

    // Simulate database corruption
    yield* Effect.promise(() => rawDb`truncate table zero_0.clients`);

    /*
    In case of "out of order" error, the mutation promise is not rejected, but retried under the hood.
    Since it won't ever be resolved or rejected in our case, we just run the mutation and check that
    the last server response was a top-level "PushFailed" error with reason "oooMutation".
    (Upstream Zero reworked out-of-order handling into a top-level push error that stops the batch,
    rather than a per-mutation `oooMutation` result.)
  */
    z.mutate(mutators.messages.create({ id: nanoid(), body: "hello world" })).server.then();

    yield* Effect.sleep(Duration.millis(100));

    const lastResponse = pipe(responses, Chunk.last, Option.getOrThrow);
    expect(lastResponse).toMatchObject({ kind: "PushFailed", origin: "server", reason: "oooMutation" });
  }),
);

it.live(
  "non-existing mutator should reject",
  Effect.fn(function* () {
    // A client registry with a mutator the server doesn't have.
    const mutators = ZfxMutator.registry(clientTransaction, {
      nonExistingMutator: ZfxMutator.make({ payload: Schema.Void, mutator: Effect.fn(function* () {}) }),
    });
    const z = yield* ZfxClient.make(mutators, {
      userID: null,
      server: "http://localhost:4848",
      mutateURL: "http://localhost:3000/push",
    });

    const result = yield* Effect.promise(() => z.mutate(mutators.nonExistingMutator()).server);
    expect(result).toEqual({
      type: "error",
      error: expect.objectContaining({
        type: "app",
        message: "Mutator not found: nonExistingMutator",
      }),
    });
  }),
);

it.live(
  "nullable mutator arguments accept null",
  Effect.fn(function* () {
    const z = yield* initZero;

    // `null` must reach a `Schema.NullOr` decoder as-is on both the client and the server.
    const nullResult = z.mutate(mutators.nullableArg(null));
    yield* Effect.promise(() => expect(nullResult.client).resolves.toEqual({ type: "success" }));
    yield* Effect.promise(() => expect(nullResult.server).resolves.toEqual({ type: "success" }));

    const id = nanoid();
    const result = z.mutate(mutators.nullableArg({ id, body: "hello" }));
    yield* Effect.promise(() => expect(result.server).resolves.toEqual({ type: "success" }));
    const rows = yield* Effect.promise(() => rawDb`select body from messages where id = ${id}`);
    expect(rows).toEqual([{ body: "hello" }]);
  }),
);

it.live(
  "concurrent transactions should be resolved",
  Effect.fn(function* () {
    const z = yield* initZero;

    yield* Effect.promise(() => expect(z.mutate(mutators.concurrentTransactions()).server).resolves.toBeDefined());
  }),
);

it.live(
  "unsupported push version returns a top-level PushFailed",
  Effect.fn(function* () {
    // The push-version check runs before any DB/client logic, so dummy params + an empty batch suffice.
    const params: MutateParams = { schema: "public", appID: "zero" };
    const body: PushBody = {
      clientGroupID: nanoid(),
      mutations: [],
      pushVersion: 2,
      timestamp: Date.now(),
      requestID: nanoid(),
    };

    const result = yield* ZfxServer.handleMutate({
      transaction: serverTransaction,
      mutators: serverMutators,
      query: params,
      body,
      userId: undefined,
    });
    expect(result).toMatchObject({ kind: "PushFailed", origin: "server", reason: "unsupportedPushVersion" });
  }),
);

it.live(
  "already-processed mutations return an alreadyProcessed result",
  Effect.fn(function* () {
    const z = yield* initZero;

    // Establish the client's last-mutation-id row with a first successful mutation.
    yield* Effect.promise(() => z.mutate(mutators.messages.create({ id: nanoid(), body: "first" })).server);

    // Artificially advance the stored last-mutation-id so the next mutation looks already-processed.
    // NOTE: assumes the column is `zero_0.clients."lastMutationID"` — adjust to the live schema if needed.
    yield* Effect.promise(() => rawDb`update zero_0.clients set "lastMutationID" = 9999`);

    // Like the OOO case, the mutation is retried under the hood and never resolves, so inspect the last response.
    z.mutate(mutators.messages.create({ id: nanoid(), body: "second" })).server.then();
    yield* Effect.sleep(Duration.millis(100));

    const lastResponse = pipe(responses, Chunk.last, Option.getOrThrow);
    expect(lastResponse).toMatchObject({ mutations: [{ result: { error: "alreadyProcessed" } }] });
  }),
);

it.live(
  "a database error surfaces as a per-mutation app error (not a top-level PushFailed)",
  Effect.fn(function* () {
    const z = yield* initZero;

    // Seed a row directly so the server-side raw insert collides on the primary key. The client mutator
    // doesn't write, so it succeeds and the mutation is still pushed to the server.
    const id = nanoid();
    yield* Effect.promise(() => rawDb`insert into messages (id, body) values (${id}, 'seed')`);

    // Our bridge converts the resulting DB error into a per-mutation app error; it never emits a top-level
    // PushFailed reason:"database" (only OutOfOrderMutation is passed through to upstream's top-level handler).
    const result = yield* Effect.promise(() => z.mutate(mutators.rawInsertDuplicate(id)).server);
    expect(result).toMatchObject({ type: "error", error: { type: "app" } });
  }),
);

it.live(
  "the server still runs mutations named the legacy way",
  Effect.fn(function* () {
    // Zero's legacy custom mutators are named "namespace|name", like upstream's server accepts.
    const clientID = nanoid();
    const item: Message = { id: nanoid(), body: "legacy" };
    const result = yield* ZfxServer.handleMutate({
      transaction: serverTransaction,
      mutators: serverMutators,
      query: { schema: "zero_0", appID: "zero" },
      body: {
        clientGroupID: nanoid(),
        mutations: [{ type: "custom", id: 1, clientID, name: "messages|create", args: [item], timestamp: Date.now() }],
        pushVersion: 1,
        timestamp: Date.now(),
        requestID: nanoid(),
      },
      userId: undefined,
    });

    expect(result).toEqual({
      kind: "MutateResponse",
      userID: null,
      mutations: [{ id: { clientID, id: 1 }, result: {} }],
    });
    const rows = yield* Effect.promise(() => rawDb`select body from messages where id = ${item.id}`);
    expect(rows).toEqual([{ body: "legacy" }]);
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
      const result = yield* waitForComplete(stream, (result) => result.value !== undefined);
      expect(result.value).toEqual(value);
    }

    {
      const result = yield* waitForComplete(stream2, (result) => result.value.length === 1);
      expect(result.value).toEqual([value]);
    }

    {
      const value2: Message = { id: nanoid(), body: "Hello, world!" };
      yield* Effect.promise(() => ddb.insert(messages).values(value2));

      const result = yield* waitForComplete(stream2, (result) => result.value.length === 2);
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
  "Result.acceptPartialMode with 'acceptCached' should work for singular queries",
  Effect.fn(function* () {
    const value: Message = { id: nanoid(), body: "hello world" };

    const z = yield* initZero;

    const q = yield* messageByIdQuery(value.id);
    const atom = Atom.make(ZfxQuery.stream(z, q), { initialValue: ZfxQuery.initialValue(q) }).pipe(
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
  }, Effect.provide(AtomRegistry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'acceptCached' should work for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const q = yield* messagesQuery();
    const atom = Atom.make(ZfxQuery.stream(z, q), { initialValue: ZfxQuery.initialValue(q) }).pipe(
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
  }, Effect.provide(AtomRegistry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'waitForServer' should work for singular queries",
  Effect.fn(function* () {
    const value: Message = { id: nanoid(), body: "hello world" };

    const z = yield* initZero;

    const q = yield* messageByIdQuery(value.id);
    const atom = Atom.make(ZfxQuery.stream(z, q), { initialValue: ZfxQuery.initialValue(q) }).pipe(
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
  }, Effect.provide(AtomRegistry.layer)),
);

it.live(
  "Result.acceptPartialMode with 'waitForServer' should work for plural queries",
  Effect.fn(function* () {
    const z = yield* initZero;

    const q = yield* messagesQuery();
    const atom = Atom.make(ZfxQuery.stream(z, q), { initialValue: ZfxQuery.initialValue(q) }).pipe(
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
  }, Effect.provide(AtomRegistry.layer)),
);
