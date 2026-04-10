// Gold Lapel plugin for Drizzle ORM.
//
// Driver requirement: This plugin uses the `pg` npm package (node-postgres)
// and `drizzle-orm/node-postgres`. It does NOT support `postgres` (postgres.js)
// or `drizzle-orm/postgres-js`. If you need postgres.js support, use the
// goldlapel `init()` function to rewrite DATABASE_URL and create your own
// drizzle instance with the proxy URL.
import { start, stop, proxyUrl, GoldLapel, wrap, NativeCache } from '@goldlapel/goldlapel'

const DEFAULT_PORT = 7932

export async function drizzle(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'drizzle'
    const {
        url: _, port, config, extraArgs, invalidationPort, nativeCache,
        _start, _drizzle, _wrap, _pg,
        ...drizzleOptions
    } = options
    const startFn = _start || start
    const wrapFn = _wrap || wrap
    const proxyPort = port ?? DEFAULT_PORT
    const result = await startFn(url, { config, port, extraArgs })

    // Resolve proxy URL — start() may return a wrapped client or a URL string
    const proxyUrlStr = typeof result === 'string' ? result : proxyUrl()

    // Create a pg.Pool connected to the proxy
    const pg = _pg || (await import('pg')).default
    const pool = new pg.Pool({ connectionString: proxyUrlStr })

    // Wrap pool with L1 native cache unless explicitly disabled
    let client = pool
    if (nativeCache !== false) {
        const invPort = invalidationPort ?? (proxyPort + 2)
        client = wrapFn(pool, invPort)
    }

    const drizzleFn = _drizzle || (await import('drizzle-orm/node-postgres')).drizzle
    return drizzleFn(client, drizzleOptions)
}

export async function init(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'drizzle'
    const startFn = options._start || start
    const result = await startFn(url, { config: options.config, port: options.port, extraArgs: options.extraArgs })
    const proxyUrlStr = typeof result === 'string' ? result : proxyUrl()
    process.env.DATABASE_URL = proxyUrlStr
    return proxyUrlStr
}

export {
    start, stop, proxyUrl, GoldLapel, wrap, NativeCache,
    docInsert, docInsertMany, docFind, docFindOne,
    docUpdate, docUpdateOne, docDelete, docDeleteOne,
    docCount, docCreateIndex, docAggregate,
    docWatch, docUnwatch, docCreateTtlIndex, docRemoveTtlIndex,
    docCreateCapped, docRemoveCap,
} from '@goldlapel/goldlapel'
