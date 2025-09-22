# Tests

Tests are run automatically on pull requests.

To run locally:

> [!NOTE]
> Make sure port 5432 is available for Postgres.

```sh
cd tests
cp .env.example .env
docker compose up -d --wait
bun i
bun gen
bun drizzle-kit migrate
bun test:types
bun zero-cache-dev # run in a separate terminal
bun test
```
