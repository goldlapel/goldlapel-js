# Gold Lapel

Self-optimizing Postgres proxy — automatic materialized views and indexes, with an L1 native cache that serves repeated reads in microseconds. Zero code changes required.

Gold Lapel sits between your app and Postgres, watches query patterns, and automatically creates materialized views and indexes to make your database faster. Port 7932 (79 = atomic number for gold, 32 from Postgres).

## Install

```bash
npm install @goldlapel/goldlapel
```

## Quick Start

```js
import { GoldLapel } from '@goldlapel/goldlapel';

// Create a proxy instance and start it
const gl = new GoldLapel('postgresql://user:pass@localhost:5432/mydb');
await gl.start();

// Use the proxy connection directly — no driver setup needed
const result = gl.query('SELECT * FROM users WHERE id = $1', [42]);
```

After startup, Gold Lapel prints a one-line summary and serves a dashboard at `http://127.0.0.1:7933` by default:

```js
const dashUrl = gl.dashboardUrl();
// => "http://127.0.0.1:7933" (or null if not running / dashboard disabled)
```

## API

### `new GoldLapel(upstream, opts)`

Creates a Gold Lapel proxy instance.

- `upstream` — your Postgres connection string (e.g. `postgresql://user:pass@localhost:5432/mydb`)
- `opts.port` — proxy port (default: 7932)
- `opts.config` — config object (see [Configuration](#configuration))
- `opts.extraArgs` — additional CLI flags passed to the binary (e.g. `['--threshold-impact', '5000']`)

### `gl.start()`

Starts the proxy. Returns a promise that resolves when the proxy is ready.

### `gl.stop()`

Stops the proxy. Also called automatically on process exit.

### `gl.proxyUrl()`

Returns the current proxy URL, or `null` if not running.

### `gl.dashboardUrl()`

Returns the dashboard URL (e.g. `http://127.0.0.1:7933`), or `null` if not running or the dashboard is disabled (`dashboardPort: 0`).

## Configuration

Pass a config object to the constructor to configure the proxy:

```js
import { GoldLapel } from '@goldlapel/goldlapel';

const gl = new GoldLapel('postgresql://user:pass@localhost/mydb', {
  config: {
    mode: 'waiter',
    poolSize: 50,
    disableMatviews: true,
    replica: ['postgresql://user:pass@replica1/mydb'],
  },
});
await gl.start();
```

Keys use `camelCase` and map to CLI flags (`poolSize` → `--pool-size`). Boolean keys are flags — `true` enables them. Array keys produce repeated flags.

Unknown keys throw immediately. To see all valid keys:

```js
import { configKeys } from '@goldlapel/goldlapel';
console.log(configKeys());
```

For the full configuration reference, see the [main documentation](https://github.com/goldlapel/goldlapel#setting-reference).

### Raw CLI flags

You can also pass raw CLI flags via `extraArgs`:

```js
const gl = new GoldLapel('postgresql://user:pass@localhost:5432/mydb', {
  extraArgs: ['--threshold-duration-ms', '200', '--refresh-interval-secs', '30'],
});
await gl.start();
```

Or set environment variables (`GOLDLAPEL_PROXY_PORT`, `GOLDLAPEL_UPSTREAM`, etc.) — the binary reads them automatically.

## How It Works

This package bundles the Gold Lapel Rust binary for your platform. When you call `start()`, it:

1. Locates the binary (bundled in package, on PATH, or via `GOLDLAPEL_BINARY` env var)
2. Spawns it as a subprocess listening on localhost
3. Waits for the port to be ready
4. Returns a database connection with L1 native cache built in
5. Cleans up automatically on process exit

The binary does all the work — this wrapper just manages its lifecycle.

## Links

- [Website](https://goldlapel.com)
- [Documentation](https://github.com/goldlapel/goldlapel)
