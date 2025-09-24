import dotenv from 'dotenv';

dotenv.config({ path: 'tests/.env', quiet: true });

import { PostgresJSConnection, ZQLDatabase } from '@rocicorp/zero/pg';
import { afterAll, beforeAll, beforeEach, expect, expectTypeOf, mock, test } from 'bun:test';
import postgres from "postgres";
import { schema, type Schema as ZeroSchema } from './schema';
import * as ZeroServer from "../src/server";
import * as ZeroClient from "../src/client";
import { Console, Duration, Effect, Layer, Option, pipe, Schema, Scope, Stream, StreamEmit, Subscribable } from 'effect';
import { Zero } from '@rocicorp/zero';
import { HttpRouter, HttpServer, HttpServerRequest, HttpServerResponse } from "@effect/platform";
import { BunHttpServer } from "@effect/platform-bun";
import { ZeroPushBody, ZeroPushParams, ZeroPushResponse } from '../src/types';
import { drizzle } from 'drizzle-orm/postgres-js';
import { count } from 'drizzle-orm';
import { messages } from './drizzle.schema';
import { nanoid } from 'nanoid';
import { Atom, Registry, Result } from '@effect-atom/atom';
import { createSessionStorage } from "bun-storage";
import type { Message } from './schema.gen';

const [sessionStorage] = createSessionStorage();
globalThis.sessionStorage = sessionStorage;

const rawDb = postgres(process.env.ZERO_UPSTREAM_DB!, { onnotice: () => { } });
const ddb = drizzle(rawDb);
const connection = new PostgresJSConnection(rawDb);
const database = new ZQLDatabase(connection, schema);

type MutatorArgs = {
  messages: {
    create: Message;
    noTransaction: void;
    doubleTransaction: void;
  };
};

const zeroClient = ZeroClient.makeClient<ZeroSchema>();

const zeroServer = ZeroServer.makeServer({
  database,
  clientTransaction: zeroClient.Transaction,
});

const createMessage = mock(Effect.fn(function* (item: Message) {
  yield* zeroClient.Transaction.use(async (tx) => {
    await tx.mutate.messages.insert(item);
  });
}));

const clientMutators = zeroClient.mutators<MutatorArgs>()({
  messages: {
    create: createMessage,
    noTransaction: Effect.fn(function* () { }),
    doubleTransaction: Effect.fn(function* () { }),
  }
});

const serverMutators = zeroServer.mutators<MutatorArgs>()({
  messages: {
    create: Effect.fn(function* (item: Message) {
      yield* createMessage(item)
        .pipe(zeroServer.Transaction.execute);
    }),
    noTransaction: Effect.fn(function* () { }),
    doubleTransaction: Effect.fn(function* () {
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: 'hello world' });
      }).pipe(zeroServer.Transaction.execute);
      yield* zeroServer.Transaction.use(async (tx) => {
        await tx.mutate.messages.insert({ id: nanoid(), body: 'hello world' });
      }).pipe(zeroServer.Transaction.execute);
    }),
  }
});

const onError = mock((...args) => console.error('onError:', ...args));

const z = new Zero({
  userID: 'anon',
  server: 'http://localhost:4848',
  schema,
  mutators: zeroClient.unwrapMutators(clientMutators).pipe(Effect.runSync),
  push: {
    url: 'http://localhost:3000/push',
  },
  onError,
});

const serverController = new AbortController();

function waitForLastItem<A, E, R>(stream: Stream.Stream<A, E, R>) {
  return pipe(
    stream,
    Stream.timeout(Duration.millis(100)),
    Stream.runLast,
    Effect.map(Option.getOrThrow),
    Effect.scoped,
  );
}

beforeAll(async () => {
  const router = HttpRouter.empty.pipe(
    HttpRouter.post('/push', Effect.gen(function* () {
      const params = yield* HttpRouter.schemaParams(ZeroPushParams);
      const payload = yield* HttpServerRequest.schemaBodyJson(ZeroPushBody);
      const result = yield* zeroServer.processPush(serverMutators, params, payload);
      const responseBody = yield* Schema.encode(ZeroPushResponse)(result);

      return (yield* HttpServerResponse.json(responseBody)).pipe(
        HttpServerResponse.setStatus(200),
        HttpServerResponse.setHeader("content-type", "application/json"),
      );
    })
      .pipe(
        Effect.catchAll((e) => {
          console.error('Push processor error:', e);
          return pipe(
            HttpServerResponse.empty(),
            HttpServerResponse.setStatus(500)
          );
        })
      )
    ),
  );

  const app = router.pipe(HttpServer.serve());
  const ServerLive = BunHttpServer.layer({ port: 3000 });
  Effect.runPromiseExit(Layer.launch(Layer.provide(app, ServerLive)), { signal: serverController.signal });
});

afterAll(() => {
  serverController.abort();
});

beforeEach(async () => {
  onError.mockReset();

  const c = await ddb.select({ count: count() }).from(messages).then((r) => r[0]!.count);
  if (c > 0) {
    await rawDb`truncate table messages`;

    // Wait until view is synced after truncation
    const sub = zeroClient.querySub(z.query.messages);
    await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === 'complete'),
      waitForLastItem,
      Effect.andThen((d) =>
        Effect.fail(new Error('not empty')).pipe(
          Effect.when(() => d.data.length > 0),
        )
      ),
      Effect.scoped,
      Effect.runPromise,
    );
  }
});

test('processPush has no extra requirements', () => {
  const effect = zeroServer.processPush(serverMutators, {} as any, {} as any);

  expectTypeOf<Effect.Effect.Context<typeof effect>>().toEqualTypeOf<never>();
});

test('rows are returned after data is inserted', async () => {
  const value1 = {
    id: nanoid(),
    body: 'hello world',
  };
  await ddb.insert(messages).values(value1);

  const sub = zeroClient.querySub(z.query.messages);

  expectTypeOf<Effect.Effect.Context<typeof sub>>().toEqualTypeOf<Scope.Scope>();

  {
    const result = await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === 'complete'),
      waitForLastItem,
      Effect.runPromise,
    );

    expect(result.data).toEqual([value1]);
  }

  const value2 = {
    id: nanoid(),
    body: 'hello world 2',
  };
  await ddb.insert(messages).values(value2);

  {
    const result = await pipe(
      Subscribable.unwrap(sub).changes,
      Stream.filter((d) => d.status === 'complete'),
      waitForLastItem,
      Effect.runPromise
    );

    expect(result.data).toBeArrayOfSize(2);
    expect(result.data).toContainAllValues([value1, value2]);
  }
});

test('atom is set to correct value', async () => {
  const atom = zeroClient.queryAtom(z.query.messages);

  {
    const result = atom.pipe(
      Atom.get,
      Effect.provide(Registry.layer),
      Effect.runSync,
      Result.getOrThrow,
    );

    expect(result).toEqual([]);
  }

  const value1 = {
    id: nanoid(),
    body: 'hello world',
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
    body: 'hello world 2',
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

test('custom mutators work', async () => {
  const item: Message = { id: nanoid(), body: 'Hello, world!' };
  await z.mutate.messages.create(item).server;

  expect(createMessage).toHaveBeenCalledWith(item);

  const result = await rawDb`select id, body from messages`;
  expect(result.slice()).toEqual([item]);
});

test.skip('mutator without transaction should reject', async () => {
  expect(z.mutate.messages.noTransaction().server).rejects.toBeDefined();
});

test.skip('mutator that invokes transaction more than once should reject', async () => {
  expect(z.mutate.messages.doubleTransaction().server).rejects.toBeDefined();
});

test.skip('out of order mutations should be rejected', async () => {
  await z.mutate.messages.create({ id: nanoid(), body: 'hello world' }).server;

  // Simulate database corruption
  await rawDb`truncate table zero_0.clients`;

  expect(z.mutate.messages.create({ id: nanoid(), body: 'hello world' }).server).rejects.toBeDefined();
});
