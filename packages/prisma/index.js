import { start, stop, proxyUrl, GoldLapel, NativeCache } from '@goldlapel/goldlapel'

const DEFAULT_PORT = 7932
const READ_OPS = new Set([
    'findFirst', 'findFirstOrThrow',
    'findUnique', 'findUniqueOrThrow',
    'findMany', 'count', 'aggregate', 'groupBy',
])
const WRITE_OPS = new Set([
    'create', 'createMany', 'createManyAndReturn',
    'update', 'updateMany', 'updateManyAndReturn',
    'upsert', 'delete', 'deleteMany',
])

function makeKey(model, operation, args) {
    try {
        return 'prisma\0' + model + '\0' + operation + '\0' + JSON.stringify(args ?? null)
    } catch {
        return null
    }
}

// Build a mapping from Prisma model names to actual database table names
// using Prisma's DMMF (data model meta format). When @@map() is used, the
// model name differs from the table name — this resolves that mapping.
function buildTableMap(dmmf) {
    const map = new Map()
    if (!dmmf?.datamodel?.models) return map
    for (const model of dmmf.datamodel.models) {
        // model.dbName is the @@map() value, null if no mapping
        const tableName = (model.dbName || model.name).toLowerCase()
        map.set(model.name, tableName)
    }
    return map
}

function modelToTable(model, tableMap) {
    if (tableMap && tableMap.has(model)) {
        return tableMap.get(model)
    }
    return model.toLowerCase()
}

export function cacheExtension(options = {}) {
    const port = options.invalidationPort ?? options._invalidationPort ?? (DEFAULT_PORT + 2)
    const cache = options._cache || new NativeCache()
    if (!options._cache && !cache._socket) {
        cache.connectInvalidation(port)
    }

    const tableMap = options._tableMap || null

    return {
        name: 'goldlapel-cache',
        query: {
            $allOperations({ model, operation, args, query }) {
                if (!model) return query(args)

                const table = modelToTable(model, tableMap)

                if (WRITE_OPS.has(operation)) {
                    cache.invalidateTable(table)
                    return query(args)
                }

                if (!READ_OPS.has(operation)) return query(args)

                const key = makeKey(model, operation, args)
                if (key === null) return query(args)

                const entry = cache._cache.get(key)
                if (entry !== undefined && cache._invalidationConnected) {
                    cache._cache.delete(key)
                    cache._cache.set(key, entry)
                    cache.statsHits++
                    return entry.result
                }

                cache.statsMisses++
                return query(args).then((result) => {
                    if (cache._enabled && cache._invalidationConnected) {
                        if (cache._cache.has(key)) {
                            cache._cache.delete(key)
                        } else if (cache._cache.size >= cache._maxEntries) {
                            cache._evictOne()
                        }
                        const tables = new Set([table])
                        cache._cache.set(key, { result, tables })
                        let keys = cache._tableIndex.get(table)
                        if (!keys) {
                            keys = new Set()
                            cache._tableIndex.set(table, keys)
                        }
                        keys.add(key)
                    }
                    return result
                })
            },
        },
    }
}

export async function withGoldLapel(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'prisma'
    const startFn = options._start || start
    const result = await startFn(url, { config: options.config, port: options.port, extraArgs: options.extraArgs })

    // start() may return a wrapped client or a URL string
    const proxyUrlStr = typeof result === 'string' ? result : proxyUrl()
    process.env.DATABASE_URL = proxyUrlStr

    const proxyPort = options.port ?? DEFAULT_PORT
    const invPort = options.config?.invalidationPort ?? (proxyPort + 2)

    const PC = options._PrismaClient || (await import('@prisma/client')).PrismaClient

    // Build @@map() table name mapping from Prisma DMMF if available
    let tableMap = null
    if (options._dmmf) {
        tableMap = buildTableMap(options._dmmf)
    } else {
        try {
            const { Prisma } = await import('@prisma/client')
            if (Prisma?.dmmf) {
                tableMap = buildTableMap(Prisma.dmmf)
            }
        } catch {
            // DMMF not available — fall back to lowercase model names
        }
    }

    const ext = cacheExtension({
        invalidationPort: invPort,
        _tableMap: tableMap,
        ...options._cacheOptions,
    })
    return new PC({ datasources: { db: { url: proxyUrlStr } } }).$extends(ext)
}

export async function init(options = {}) {
    const url = options.url || process.env.DATABASE_URL
    if (!url) throw new Error('Gold Lapel: DATABASE_URL not set. Pass { url } or set DATABASE_URL.')
    if (!process.env.GOLDLAPEL_CLIENT) process.env.GOLDLAPEL_CLIENT = 'prisma'
    const startFn = options._start || start
    const result = await startFn(url, { config: options.config, port: options.port, extraArgs: options.extraArgs })
    const proxyUrlStr = typeof result === 'string' ? result : proxyUrl()
    process.env.DATABASE_URL = proxyUrlStr
    return proxyUrlStr
}

export {
    start, stop, proxyUrl, GoldLapel, NativeCache,
    docInsert, docInsertMany, docFind, docFindOne,
    docUpdate, docUpdateOne, docDelete, docDeleteOne,
    docCount, docCreateIndex, docAggregate,
    docWatch, docUnwatch, docCreateTtlIndex, docRemoveTtlIndex,
    docCreateCapped, docRemoveCap,
} from '@goldlapel/goldlapel'
