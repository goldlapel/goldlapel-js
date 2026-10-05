# @goldlapel/prisma

Gold Lapel plugin for [Prisma](https://www.prisma.io/) — automatic Postgres query optimization with one line of code. The plugin starts the Gold Lapel proxy alongside your app and points Prisma at it; the proxy caches results and creates indexes for every query Prisma sends. Your `PrismaClient` is a plain, unextended client.

## Install

```bash
npm install goldlapel @goldlapel/prisma
```

## Quick start

### Option A: `withGoldLapel()` (Prisma v5/v6)

Returns a `PrismaClient` with the connection routed through Gold Lapel:

```javascript
import { withGoldLapel } from '@goldlapel/prisma'

const prisma = await withGoldLapel()

const users = await prisma.user.findMany()
```

### Option B: `init()` (all Prisma versions)

`init()` starts the proxy and rewrites `DATABASE_URL` to point at it — works with Prisma v5, v6, and v7+:

```javascript
import { init } from '@goldlapel/prisma'

await init()

import { PrismaClient } from '@prisma/client'
const prisma = new PrismaClient()
```

## Prisma v7 note

Prisma v7 removed the `datasources` constructor override. Use `init()` instead of `withGoldLapel()`.

## Options

Both `withGoldLapel()` and `init()` accept an options object:

| Option | Description |
|--------|-------------|
| `url` | Upstream Postgres URL. Defaults to `process.env.DATABASE_URL`. |
| `proxyPort` | Port for the Gold Lapel proxy. Defaults to `7932` (the dashboard listens on the next port up); a second database started in the same process gets the next free pair — `7934`, `7936`, …. |
| `config` | Config object passed to Gold Lapel (see below). |
| `extraArgs` | Array of extra CLI args passed to the Gold Lapel binary. |

Every other `goldlapel` `start()` option is accepted too and passed through as given: `dashboardPort`, `logLevel`, `mode`, `license`, `client`, `configFile`, `silent`, `mesh`, `meshTag`, `disableProxyCache`, `disableSqloptimize`, `disableAutoIndexes`. Unknown or removed options (`invalidationPort`, `nativeCache`, …) are an error rather than silently ignored.

```javascript
const prisma = await withGoldLapel({
  url: 'postgresql://user:pass@host:5432/mydb',
  proxyPort: 9000,
  config: { poolSize: 30, disableN1: true },
})
```

## Re-exports

For convenience, `@goldlapel/prisma` re-exports the core wrapper surface from `goldlapel`:

```javascript
import {
  start, GoldLapel,
  DocumentsAPI, StreamsAPI,
} from '@goldlapel/prisma'
```

`DocumentsAPI` and `StreamsAPI` are exported for type-checking / extension.

### Document store and streams

The flat `docInsert` / `docFind` / `streamAdd` / etc. utility re-exports were removed. Document-store and stream operations now live on the `GoldLapel` instance under the `documents` and `streams` namespaces — call them after `start()`:

```javascript
import { start } from '@goldlapel/prisma'

const gl = await start(process.env.DATABASE_URL)
await gl.documents.insert('users', { name: 'Alice' })
const alice = await gl.documents.findOne('users', { name: 'Alice' })
await gl.streams.add('events', { kind: 'login', userId: alice._id })
await gl.stop()
```

`withGoldLapel()` and `init()` are still the recommended entry points for ORM-only usage; reach for `start()` only when you also want the doc-store / stream APIs.
