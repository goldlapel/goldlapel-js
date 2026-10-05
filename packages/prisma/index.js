import { start } from 'goldlapel'

// Resolve the proxy URL from whatever `start()` returned. start() in v0.2
// returns a GoldLapel instance; older/mocked versions may return a string.
function _resolveProxyUrl(result) {
    if (typeof result === 'string') return result
    if (result && typeof result === 'object' && typeof result.url === 'string') {
        return result.url
    }
    return null
}

// Every option besides `url` and the test seams is a goldlapel start()
// option and goes to it as given — start() rejects unknown ones.
function _startOptions(options) {
    const { url: _, _start, _PrismaClient, ...goldlapelOptions } = options
    // Prisma has its own driver; we don't need the wrapper's internal conn.
    return { ...goldlapelOptions, noConnect: true }
}

export async function withGoldLapel(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'prisma'
    const startFn = options._start || start
    const result = await startFn(url, _startOptions(options))
    const proxyUrlStr = _resolveProxyUrl(result)
    process.env.DATABASE_URL = proxyUrlStr

    const PC = options._PrismaClient || (await import('@prisma/client')).PrismaClient
    return new PC({ datasources: { db: { url: proxyUrlStr } } })
}

export async function init(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'prisma'
    const startFn = options._start || start
    const result = await startFn(url, _startOptions(options))
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
