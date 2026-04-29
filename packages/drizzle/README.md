# @goldlapel/drizzle

Gold Lapel plugin for [Drizzle ORM](https://orm.drizzle.team/) — automatic Postgres query optimization with one line of code. Includes L1 native cache — an in-process cache that serves repeated reads in microseconds with no TCP round-trip.

## Install

```bash
npm install @goldlapel/goldlapel @goldlapel/drizzle
```

## Quick start

### Option A: `drizzle()` (node-postgres driver)

Returns a wired Drizzle DB instance with the connection routed through Gold Lapel and L1 native cache active:

```javascript
import { drizzle } from '@goldlapel/drizzle'

const db = await drizzle()

const users = await db.select().from(usersTable)
```

Pass Drizzle options like `schema` and `logger` directly:

```javascript
import { drizzle } from '@goldlapel/drizzle'
import * as schema from './schema.js'

const db = await drizzle({ schema, logger: true })
```

### Option B: `init()` (any Drizzle driver)

Rewrites `DATABASE_URL` to point at the proxy. Works with any Drizzle driver — node-postgres, postgres.js, Neon, etc.:

```javascript
import { init } from '@goldlapel/drizzle'

await init()

// Now create Drizzle as usual — it reads the rewritten DATABASE_URL
import { drizzle } from 'drizzle-orm/node-postgres'
const db = drizzle(process.env.DATABASE_URL)
```

## Driver note

`drizzle()` uses `drizzle-orm/node-postgres` under the hood and includes L1 native cache automatically. If you use a different driver (postgres.js, Neon serverless, etc.), use `init()` instead — it rewrites `DATABASE_URL` and works with any driver, but does not include L1 cache (the proxy still handles all server-side optimizations).

## Options

Both `drizzle()` and `init()` accept an options object:

| Option | Description |
|--------|-------------|
| `url` | Upstream Postgres URL. Defaults to `process.env.DATABASE_URL`. |
| `port` | Port for the Gold Lapel proxy. Defaults to `7932`. |
| `config` | Config object passed to Gold Lapel (see below). |
| `extraArgs` | Array of extra CLI args passed to the Gold Lapel binary. |

`drizzle()` also accepts L1 cache options:

| Option | Description |
|--------|-------------|
| `invalidationPort` | Port for cache invalidation. Defaults to proxy port + 2 (`7934`). |
| `nativeCache` | Set to `false` to disable L1 native cache. Enabled by default. |

`drizzle()` forwards all other options to `drizzle-orm/node-postgres`:

```javascript
const db = await drizzle({
  url: 'postgresql://user:pass@host:5432/mydb',
  proxyPort: 9000,
  config: { poolSize: 30 },
  invalidationPort: 9002,
  schema,
  logger: true,
})
```

To disable L1 cache (proxy-only mode):

```javascript
const db = await drizzle({ nativeCache: false, schema })
```

## Config

The `config` object lets you tune Gold Lapel without CLI flags. Keys use camelCase:

```javascript
const db = await drizzle({
  config: {
    mode: 'waiter',
    poolSize: 30,
    disableN1: true,
    refreshIntervalSecs: 120,
  },
  schema,
})
```

Any key accepted by the Gold Lapel CLI works here — see the [Gold Lapel docs](https://goldlapel.com/docs) for the full list. Boolean flags like `disableN1` take `true`/`false`; everything else takes a string or number.

## Re-exports

For convenience, `@goldlapel/drizzle` re-exports the core wrapper surface from `goldlapel`:

```javascript
import {
  start, GoldLapel, wrap, NativeCache,
  DocumentsAPI, StreamsAPI,
} from '@goldlapel/drizzle'
```

`DocumentsAPI` and `StreamsAPI` are exported for type-checking / extension.

### Document store and streams

The flat `docInsert` / `docFind` / `streamAdd` / etc. utility re-exports were removed. Document-store and stream operations now live on the `GoldLapel` instance under the `documents` and `streams` namespaces — call them after `start()`:

```javascript
import { start } from '@goldlapel/drizzle'

const gl = await start(process.env.DATABASE_URL)
await gl.documents.insert('users', { name: 'Alice' })
const alice = await gl.documents.findOne('users', { name: 'Alice' })
await gl.streams.add('events', { kind: 'login', userId: alice._id })
await gl.stop()
```

`drizzle()` and `init()` are still the recommended entry points for ORM-only usage; reach for `start()` only when you also want the doc-store / stream APIs.
