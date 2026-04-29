// Gold Lapel plugin for Drizzle ORM.
//
// Driver requirement: This plugin uses the `pg` npm package (node-postgres)
// and `drizzle-orm/node-postgres`. It does NOT support `postgres` (postgres.js)
// or `drizzle-orm/postgres-js`. If you need postgres.js support, use the
// goldlapel `init()` function to rewrite DATABASE_URL and create your own
// drizzle instance with the proxy URL.
import { start, wrap } from 'goldlapel'

const DEFAULT_PROXY_PORT = 7932

// Resolve the proxy URL from whatever `start()` returned. start() in v0.2
// returns a GoldLapel instance; older/mocked versions may return a string.
function _resolveProxyUrl(result) {
    if (typeof result === 'string') return result
    if (result && typeof result === 'object' && typeof result.url === 'string') {
        return result.url
    }
    return null
}

export async function drizzle(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'drizzle'
    const {
        url: _, proxyPort, config, extraArgs, invalidationPort, nativeCache,
        _start, _drizzle, _wrap, _pg,
        ...drizzleOptions
    } = options
    const startFn = _start || start
    const wrapFn = _wrap || wrap
    const resolvedProxyPort = proxyPort ?? DEFAULT_PROXY_PORT
    // This plugin builds its own pg.Pool against the proxy URL, so the core
    // wrapper doesn't need to open its own driver connection.
    const result = await startFn(url, { config, proxyPort, extraArgs, noConnect: true })

    // Resolve proxy URL — start() may return a GoldLapel instance or a URL string
    const proxyUrlStr = _resolveProxyUrl(result)

    // Create a pg.Pool connected to the proxy
    const pg = _pg || (await import('pg')).default
    const pool = new pg.Pool({ connectionString: proxyUrlStr })

    // Wrap pool with L1 native cache unless explicitly disabled
    let client = pool
    if (nativeCache !== false) {
        const invPort = invalidationPort ?? (resolvedProxyPort + 2)
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
    const result = await startFn(url, {
        config: options.config,
        proxyPort: options.proxyPort,
        extraArgs: options.extraArgs,
        noConnect: true,
    })
    const proxyUrlStr = _resolveProxyUrl(result)
    process.env.DATABASE_URL = proxyUrlStr
    return proxyUrlStr
}

// Re-exports — let plugin users reach the core wrapper without a second
// `import 'goldlapel'`. `start` and `GoldLapel` are the doors into the
// nested `gl.documents.<verb>` / `gl.streams.<verb>` APIs (Phase 4 of
// schema-to-core). The flat `doc*` / `stream*` utility re-exports were
// dropped — they require a `patterns` argument resolved from the proxy's
// dashboard and only the sub-API classes know how to fetch it.
export {
    start, GoldLapel, wrap, NativeCache,
    DocumentsAPI, StreamsAPI,
} from 'goldlapel'
