# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
npm install goldlapel
```

Gold Lapel doesn't bundle a Postgres driver — pick the one you already use (or would prefer) and install it alongside. Any one of the three is sufficient:

```bash
npm install pg                 # node-postgres (recommended; only driver with full pub/sub support)
npm install postgres           # postgres.js
npm install @vercel/postgres   # Vercel Postgres / Neon
```

If you don't need an internal driver connection — for example, you're only using Gold Lapel to provide a proxy URL to Prisma or Drizzle — you can skip this step entirely and pass `noConnect: true` to `start()`.

## Quick Start

```js
import * as goldlapel from 'goldlapel';

// Start the proxy and get back an instance
const gl = await goldlapel.start('postgresql://user:pass@localhost:5432/mydb');

// gl.url is the proxy URL — use it with any Postgres driver
import pg from 'pg';
const client = new pg.Client({ connectionString: gl.url });
await client.connect();
const { rows } = await client.query('SELECT * FROM users');

// Or use wrapper methods — no conn argument needed, the instance uses its
// own driver connection under the hood
const hits = await gl.search('articles', 'body', 'postgres tuning');
await gl.docInsert('events', { type: 'signup' });

await gl.stop();
```

On startup, Gold Lapel prints a one-line summary and serves a dashboard at `http://127.0.0.1:7933` by default:

```js
console.log(gl.dashboardUrl);  // "http://127.0.0.1:7933"
```

## Scoped connections

Use `gl.using(conn, cb)` to run a block against a specific connection — useful for transactions or per-request pools:

```js
import pg from 'pg';
const pool = new pg.Pool({ connectionString: gl.url });
const client = await pool.connect();

await client.query('BEGIN');
await gl.using(client, async (gl) => {
  await gl.docInsert('events', { type: 'order.created' });
  await gl.incr('counters', 'orders');
});
await client.query('COMMIT');
client.release();
```

The override survives across `await` boundaries (backed by `AsyncLocalStorage`), so nested async helpers pick up the same connection.

For a single call, pass `{ conn }` as the last argument:

```js
await gl.docInsert('events', { type: 'x' }, { conn: client });
```

## Auto-cleanup with `await using`

On Node 24+, use the TC39 explicit resource management proposal to auto-stop the proxy at scope end (explicit resource management is stable there):

```js
{
  await using gl = await goldlapel.start('postgresql://...');
  const hits = await gl.search('articles', 'body', 'hello');
} // proxy auto-stops here
```

On Node 22, the same syntax works behind the `--harmony-explicit-resource-management` flag. On earlier Node, fall back to the manual form:

```js
const gl = await goldlapel.start('postgresql://...');
try {
  const hits = await gl.search('articles', 'body', 'hello');
} finally {
  await gl.stop();
}
```

## Driver auto-detection

Gold Lapel auto-detects a Postgres driver at import time. It tries, in order:

1. `pg` (node-postgres)
2. `postgres` (postgres.js)
3. `@vercel/postgres`

The first one installed is used for the instance's internal default connection. Your own code is free to use any driver against `gl.url`.

If you don't need an internal connection (for example, you're only using Gold Lapel to provide a proxy URL to Prisma), pass `noConnect: true` to skip the driver lookup:

```js
const gl = await goldlapel.start(upstream, { noConnect: true });
// now use gl.url with Prisma / Drizzle / your driver of choice
```

## API

### `start(upstream, opts)`

Starts a Gold Lapel proxy and returns a `GoldLapel` instance. Eagerly opens an internal driver connection (unless `noConnect: true`).

- `upstream` — your Postgres connection string
- `opts.port` — proxy port (default: 7932)
- `opts.dashboardPort` — dashboard port (default: 7933; `0` disables)
- `opts.logLevel` — `trace` | `debug` | `info` | `warn` | `error`. The binary defaults to `warn`, which prints only the startup banner; `info`, `debug`, and `trace` turn on progressively more detail.
- `opts.config` — camelCase config object (see [Configuration](#configuration))
- `opts.extraArgs` — raw CLI flags passed to the binary
- `opts.noConnect` — skip opening the internal driver connection

### `gl.url`

Proxy URL string. Pass this to any Postgres driver.

### `gl.dashboardUrl`

Dashboard URL, or `null` if the dashboard is disabled or the proxy is not running.

### `gl.using(conn, callback)`

Runs `callback(gl)` with `conn` as the implicit connection for any wrapper method invoked inside. Survives across `await` via AsyncLocalStorage.

### `gl.stop()` / `gl[Symbol.asyncDispose]()`

Stops the proxy and closes the internal connection. Idempotent. Also auto-runs on process exit.

### Wrapper methods

Every wrapper method takes an optional trailing `{ conn }` option to override the resolved connection for that one call. Methods group roughly as:

- **Document store**: `docInsert`, `docFind`, `docUpdate`, `docDelete`, `docCount`, `docAggregate`, `docCreateIndex`, `docCreateTtlIndex`, `docCreateCapped`, …
- **Search**: `search`, `searchFuzzy`, `searchPhonetic`, `similar`, `suggest`, `facets`, `aggregate`
- **Percolation**: `percolateAdd`, `percolate`, `percolateDelete`
- **Pub/sub & queues**: `publish`, `subscribe`, `enqueue`, `dequeue`
- **Counters / hash maps / sorted sets**: `incr`, `getCounter`, `hset`, `hget`, `zadd`, `zrange`, …
- **Geo**: `geoadd`, `georadius`, `geodist`
- **Streams**: `streamAdd`, `streamRead`, `streamAck`, `streamClaim`, `streamCreateGroup`

## Configuration

```js
const gl = await goldlapel.start('postgresql://user:pass@localhost/mydb', {
  config: {
    mode: 'waiter',
    poolSize: 50,
    disableMatviews: true,
    replica: ['postgresql://user:pass@replica1/mydb'],
  },
});
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. Array keys produce repeated flags. Unknown keys throw immediately:

```js
import { configKeys } from 'goldlapel';
console.log(configKeys());  // Set of valid keys
```

### Raw CLI flags

```js
const gl = await goldlapel.start(upstream, {
  extraArgs: ['--threshold-duration-ms', '200', '--refresh-interval-secs', '30'],
});
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## Upgrading from v0.1

v0.2 is a breaking redesign. The old `new GoldLapel(url)` + `gl.start()` shape and the module-level `stop()` / `proxyUrl()` / `dashboardUrl()` singletons are gone.

| v0.1 | v0.2 |
| --- | --- |
| `const gl = new GoldLapel(url); await gl.start();` | `const gl = await goldlapel.start(url);` |
| `goldlapel.stop()` (module) | `await gl.stop()` (instance) |
| `goldlapel.proxyUrl()` | `gl.url` |
| `goldlapel.dashboardUrl()` | `gl.dashboardUrl` |
| `gl.client.query(...)` | `new pg.Client({ connectionString: gl.url })` |
| wrapper methods took implicit client | wrapper methods still do; use `gl.using(conn, cb)` or `{ conn }` to override |

Multiple Gold Lapel instances are now supported — each `start()` call returns a fresh instance.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in package, on PATH, or via `GOLDLAPEL_BINARY`)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Opens an internal driver connection to the proxy (unless `noConnect: true`)
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
