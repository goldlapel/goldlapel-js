// Gold Lapel plugin for Drizzle ORM.
//
// Driver requirement: This plugin uses the `pg` npm package (node-postgres)
// and `drizzle-orm/node-postgres`. It does NOT support `postgres` (postgres.js)
// or `drizzle-orm/postgres-js`. If you need postgres.js support, use the
// goldlapel `init()` function to rewrite DATABASE_URL and create your own
// drizzle instance with the proxy URL.
import { start, startOptionKeys, _REMOVED_OPTIONS } from 'goldlapel'

// Resolve the proxy URL from whatever `start()` returned. start() in v0.2
// returns a GoldLapel instance; older/mocked versions may return a string.
function _resolveProxyUrl(result) {
    if (typeof result === 'string') return result
    if (result && typeof result === 'object' && typeof result.url === 'string') {
        return result.url
    }
    return null
}

// Split drizzle()'s options: goldlapel start() options (and ones it has
// removed, so start() can say so) go to start(); the rest — schema, logger,
// casing, … — go to drizzle-orm.
function _splitOptions(options) {
    const { url: _, _start, _drizzle, _pg, ...rest } = options
    const startKeys = startOptionKeys()
    const goldlapelOptions = {}
    const drizzleOptions = {}
    for (const [key, value] of Object.entries(rest)) {
        if (startKeys.has(key) || Object.hasOwn(_REMOVED_OPTIONS, key)) {
            goldlapelOptions[key] = value
        } else {
            drizzleOptions[key] = value
        }
    }
    // This plugin builds its own pg.Pool against the proxy URL, so the core
    // wrapper doesn't need to open its own driver connection.
    goldlapelOptions.noConnect = true
    return { goldlapelOptions, drizzleOptions }
}

export async function drizzle(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'drizzle'
    const { goldlapelOptions, drizzleOptions } = _splitOptions(options)
    const startFn = options._start || start
    const result = await startFn(url, goldlapelOptions)

    // Resolve proxy URL — start() may return a GoldLapel instance or a URL string
    const proxyUrlStr = _resolveProxyUrl(result)

    // Create a pg.Pool connected to the proxy
    const pg = options._pg || (await import('pg')).default
    const pool = new pg.Pool({ connectionString: proxyUrlStr })

    const drizzleFn = options._drizzle || (await import('drizzle-orm/node-postgres')).drizzle
    return drizzleFn(pool, drizzleOptions)
}

export async function init(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'drizzle'
    const { goldlapelOptions, drizzleOptions } = _splitOptions(options)
    const unknown = Object.keys(drizzleOptions)
    if (unknown.length > 0) {
        // init() makes no drizzle instance, so these would go nowhere.
        throw new Error(`Unknown options: ${unknown.sort().join(', ')}`)
    }
    const startFn = options._start || start
    const result = await startFn(url, goldlapelOptions)
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
    start, GoldLapel,
    DocumentsAPI, StreamsAPI,
} from 'goldlapel'
