# @goldlapel/prisma

Gold Lapel plugin for [Prisma](https://www.prisma.io/) — automatic Postgres query optimization with one line of code. Includes L1 native cache — an in-process cache that serves repeated reads in microseconds with no TCP round-trip.

## Install

```bash
npm install @goldlapel/goldlapel @goldlapel/prisma
```

## Quick start

### Option A: `withGoldLapel()` (Prisma v5/v6)

Returns a wired `PrismaClient` with the connection routed through Gold Lapel and L1 native cache active:

```javascript
import { withGoldLapel } from '@goldlapel/prisma'

const prisma = await withGoldLapel()

const users = await prisma.user.findMany()
// Second call serves from L1 cache — no round-trip
const cached = await prisma.user.findMany()
```

### Option B: `init()` + `cacheExtension()` (all Prisma versions)

`init()` rewrites `DATABASE_URL` to point at the proxy. Pair with `cacheExtension()` for L1 cache — works with Prisma v5, v6, and v7+:

```javascript
import { init, cacheExtension } from '@goldlapel/prisma'

await init()

import { PrismaClient } from '@prisma/client'
const prisma = new PrismaClient().$extends(cacheExtension())
```

### Option C: `init()` only (proxy without L1 cache)

If you only want the proxy optimization without the in-process cache:

```javascript
import { init } from '@goldlapel/prisma'

await init()

import { PrismaClient } from '@prisma/client'
const prisma = new PrismaClient()
```

## L1 native cache

The L1 cache intercepts Prisma query operations at the extension level:

- **Read operations** (`findMany`, `findFirst`, `findUnique`, `findFirstOrThrow`, `findUniqueOrThrow`, `count`, `aggregate`, `groupBy`) are cached keyed by model + operation + args
- **Write operations** (`create`, `update`, `delete`, `upsert`, `createMany`, `updateMany`, `deleteMany`) automatically invalidate all cached entries for the affected model
- **Server-side invalidation** — the cache connects to the Gold Lapel invalidation port for cross-connection cache coherence (e.g. another process writes to the same table)
- **No stale reads** — the cache is only active when connected to the invalidation channel. If the connection drops, the cache is flushed and all queries go to the database until reconnected.

## Prisma v7 note

Prisma v7 removed the `datasources` constructor override. Use `init()` + `cacheExtension()` instead of `withGoldLapel()`.

## Options

Both `withGoldLapel()` and `init()` accept an options object:

| Option | Description |
|--------|-------------|
| `url` | Upstream Postgres URL. Defaults to `process.env.DATABASE_URL`. |
| `port` | Port for the Gold Lapel proxy. Defaults to `7932`. |
| `config` | Configuration object with camelCase keys (see below). |
| `extraArgs` | Array of extra CLI args passed to the Gold Lapel binary. |

`cacheExtension()` accepts:

| Option | Description |
|--------|-------------|
| `invalidationPort` | Port for cache invalidation signals. Defaults to proxy port + 2 (`7934`). |

```javascript
const prisma = await withGoldLapel({
  url: 'postgresql://user:pass@host:5432/mydb',
  port: 9000,
  config: { mode: 'waiter', poolSize: 30, disableN1: true },
})
```

## Re-exports

For convenience, `@goldlapel/prisma` re-exports from `@goldlapel/goldlapel`:

```javascript
import { start, stop, proxyUrl, GoldLapel, NativeCache } from '@goldlapel/prisma'
```
