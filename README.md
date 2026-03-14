# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
npm install goldlapel
```

## Quick Start

```js
import goldlapel from 'goldlapel';

// Start the proxy — returns a connection string pointing at Gold Lapel
const url = await goldlapel.start('postgresql://user:pass@localhost:5432/mydb');

// Use the URL with any Postgres driver
import pg from 'pg';
const client = new pg.Client({ connectionString: url });
await client.connect();

// Or Prisma, Drizzle, Knex, TypeORM — anything that speaks Postgres
```

Gold Lapel is driver-agnostic. `start()` returns a connection string (`postgresql://...@localhost:7932/...`) that works with any Postgres driver or ORM.

After startup, Gold Lapel prints a one-line summary and serves a dashboard at `http://127.0.0.1:7933` by default:

```js
const dashUrl = goldlapel.dashboardUrl();
// => "http://127.0.0.1:7933" (or null if not running / dashboard disabled)
```

## API

### `goldlapel.start(upstream, opts)`

Starts the Gold Lapel proxy and returns the proxy connection string. Returns a Promise.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `opts.port` — proxy port (default: 7932)
- `opts.config` — config object (see [Configuration](#configuration))
- `opts.extraArgs` — additional CLI flags passed to the binary (e.g. `['--threshold-impact', '5000']`)

### `goldlapel.stop()`

Stops the proxy. Also called automatically on process exit.

### `goldlapel.proxyUrl()`

Returns the current proxy URL, or `null` if not running.

### `goldlapel.dashboardUrl()`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running or the dashboard is disabled (`dashboardPort: 0`).

### `new goldlapel.GoldLapel(upstream, opts)`

Class interface for managing multiple instances:

```js
import { GoldLapel } from 'goldlapel';

const proxy = new GoldLapel('postgresql://user:pass@localhost:5432/mydb', { port: 7932 });
const url = await proxy.start();
// ...
proxy.stop();
```

## Configuration

Pass a config object to configure the proxy:

```js
import goldlapel from 'goldlapel'

const url = await goldlapel.start('postgresql://user:pass@localhost/mydb', {
  config: {
    mode: 'butler',
    poolSize: 50,
    disableMatviews: true,
    replica: ['postgresql://user:pass@replica1/mydb'],
  },
})
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. Array keys produce repeated flags.

Unknown keys throw immediately. To see all valid keys:

```js
import { configKeys } from 'goldlapel'
console.log(configKeys())
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

### Raw CLI flags

You can also pass raw CLI flags via `extraArgs`:

```js
const url = await goldlapel.start(
    'postgresql://user:pass@localhost:5432/mydb',
    { extraArgs: ['--threshold-duration-ms', '200', '--refresh-interval-secs', '30'] }
);
```

Or set environment variables (`GOLDLAPEL_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a connection string pointing at the proxy
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
