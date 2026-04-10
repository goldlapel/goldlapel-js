import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'

import { withGoldLapel, init, cacheExtension, start, stop, proxyUrl, GoldLapel, NativeCache } from '../index.js'

const origGoldlapelClient = process.env.GOLDLAPEL_CLIENT

function mockStart(returnUrl) {
    const calls = []
    async function _start(upstream, opts) {
        calls.push({ upstream, opts })
        return returnUrl
    }
    return { _start, calls }
}

class MockPrismaClient {
    constructor(opts) {
        this._opts = opts
        this._extensions = []
    }

    $extends(ext) {
        this._extensions.push(ext)
        return this
    }
}

function mockCache() {
    NativeCache._reset()
    const cache = new NativeCache()
    cache._invalidationConnected = true
    return cache
}


describe('withGoldLapel', () => {
    const origUrl = process.env.DATABASE_URL

    beforeEach(() => {
        delete process.env.DATABASE_URL
        delete process.env.GOLDLAPEL_CLIENT
        NativeCache._reset()
    })

    afterEach(() => {
        if (origUrl !== undefined) {
            process.env.DATABASE_URL = origUrl
        } else {
            delete process.env.DATABASE_URL
        }
        if (origGoldlapelClient !== undefined) {
            process.env.GOLDLAPEL_CLIENT = origGoldlapelClient
        } else {
            delete process.env.GOLDLAPEL_CLIENT
        }
        NativeCache._reset()
    })

    it('calls start with DATABASE_URL and returns PrismaClient with proxy URL', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const client = await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(calls.length, 1)
        assert.strictEqual(calls[0].upstream, 'postgresql://user:pass@host:5432/mydb')
        assert.deepStrictEqual(calls[0].opts, { config: undefined, port: undefined, extraArgs: undefined })
        assert(client instanceof MockPrismaClient)
        assert.strictEqual(client._opts.datasources.db.url, 'postgresql://user:pass@localhost:7932/mydb')
        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('uses explicit url over env', async () => {
        process.env.DATABASE_URL = 'postgresql://env@host:5432/db'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await withGoldLapel({
            url: 'postgresql://explicit@host:5432/db',
            _start,
            _PrismaClient: MockPrismaClient,
        })

        assert.strictEqual(calls[0].upstream, 'postgresql://explicit@host:5432/db')
        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('throws when no DATABASE_URL', async () => {
        await assert.rejects(
            () => withGoldLapel({ _start: mockStart('x')._start, _PrismaClient: MockPrismaClient }),
            /DATABASE_URL not set/,
        )
    })

    it('passes port to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')

        await withGoldLapel({ port: 9000, _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(calls[0].opts.port, 9000)
    })

    it('passes extraArgs to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await withGoldLapel({
            extraArgs: ['--verbose'],
            _start,
            _PrismaClient: MockPrismaClient,
        })

        assert.deepStrictEqual(calls[0].opts.extraArgs, ['--verbose'])
    })

    it('passes config to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const config = { mode: 'waiter', poolSize: 30, disableN1: true }

        await withGoldLapel({ config, _start, _PrismaClient: MockPrismaClient })

        assert.deepStrictEqual(calls[0].opts.config, { mode: 'waiter', poolSize: 30, disableN1: true })
    })

    it('omits config when not provided', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(calls[0].opts.config, undefined)
    })

    it('applies cache extension via $extends', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const client = await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(client._extensions.length, 1)
        assert.strictEqual(client._extensions[0].name, 'goldlapel-cache')
        assert.strictEqual(typeof client._extensions[0].query.$allOperations, 'function')
    })

    it('uses custom invalidation port from config', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const client = await withGoldLapel({
            _start,
            _PrismaClient: MockPrismaClient,
            config: { invalidationPort: 9999 },
        })

        assert.strictEqual(client._extensions.length, 1)
    })

    it('derives invalidation port from custom proxy port', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:9000/mydb')

        const client = await withGoldLapel({
            port: 9000,
            _start,
            _PrismaClient: MockPrismaClient,
        })

        assert.strictEqual(client._extensions.length, 1)
    })

    it('builds tableMap from DMMF when provided', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const dmmf = {
            datamodel: {
                models: [
                    { name: 'User', dbName: null },
                    { name: 'BlogPost', dbName: 'blog_posts' },
                ],
            },
        }

        const client = await withGoldLapel({
            _start,
            _PrismaClient: MockPrismaClient,
            _dmmf: dmmf,
        })

        assert.strictEqual(client._extensions.length, 1)
        assert.strictEqual(client._extensions[0].name, 'goldlapel-cache')
    })

    it('sets GOLDLAPEL_CLIENT env var', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'prisma')
    })
})


describe('init', () => {
    const origUrl = process.env.DATABASE_URL

    beforeEach(() => {
        delete process.env.DATABASE_URL
    })

    afterEach(() => {
        if (origUrl !== undefined) {
            process.env.DATABASE_URL = origUrl
        } else {
            delete process.env.DATABASE_URL
        }
    })

    it('rewrites process.env.DATABASE_URL to proxy URL', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await init({ _start })

        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('uses explicit url over env', async () => {
        process.env.DATABASE_URL = 'postgresql://env@host:5432/db'
        const { _start, calls } = mockStart('postgresql://explicit@localhost:7932/db')

        await init({ url: 'postgresql://explicit@host:5432/db', _start })

        assert.strictEqual(calls[0].upstream, 'postgresql://explicit@host:5432/db')
    })

    it('throws when no DATABASE_URL', async () => {
        await assert.rejects(
            () => init({ _start: mockStart('x')._start }),
            /DATABASE_URL not set/,
        )
    })

    it('returns the proxy URL', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const result = await init({ _start })

        assert.strictEqual(result, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('passes port to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')

        await init({ port: 9000, _start })

        assert.strictEqual(calls[0].opts.port, 9000)
    })

    it('passes extraArgs to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await init({ extraArgs: ['--verbose'], _start })

        assert.deepStrictEqual(calls[0].opts.extraArgs, ['--verbose'])
    })

    it('passes config to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const config = { mode: 'waiter', poolSize: 30, disableN1: true }

        await init({ config, _start })

        assert.deepStrictEqual(calls[0].opts.config, { mode: 'waiter', poolSize: 30, disableN1: true })
    })

    it('omits config when not provided', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await init({ _start })

        assert.strictEqual(calls[0].opts.config, undefined)
    })

    it('sets DATABASE_URL even when using explicit url', async () => {
        process.env.DATABASE_URL = 'postgresql://original@host:5432/db'
        const { _start } = mockStart('postgresql://explicit@localhost:7932/db')

        await init({ url: 'postgresql://explicit@host:5432/db', _start })

        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://explicit@localhost:7932/db')
    })
})


describe('cacheExtension', () => {
    afterEach(() => {
        NativeCache._reset()
    })

    it('returns extension with name and query hook', () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })
        assert.strictEqual(ext.name, 'goldlapel-cache')
        assert.strictEqual(typeof ext.query.$allOperations, 'function')
    })

    it('passes through non-model operations', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })
        let called = false
        const query = async () => { called = true; return 'result' }

        const result = await ext.query.$allOperations({
            model: undefined,
            operation: 'findMany',
            args: {},
            query,
        })

        assert.strictEqual(called, true)
        assert.strictEqual(result, 'result')
    })

    it('passes through unknown operations', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })
        let called = false
        const query = async () => { called = true; return 'result' }

        const result = await ext.query.$allOperations({
            model: 'User',
            operation: '$queryRaw',
            args: {},
            query,
        })

        assert.strictEqual(called, true)
        assert.strictEqual(result, 'result')
    })

    it('caches read operations when invalidation is connected', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let callCount = 0
        const query = async () => {
            callCount++
            return [{ id: 1, name: 'Alice' }]
        }

        const args = { where: { id: 1 } }

        const result1 = await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args,
            query,
        })

        assert.strictEqual(callCount, 1)
        assert.deepStrictEqual(result1, [{ id: 1, name: 'Alice' }])

        const result2 = await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args,
            query,
        })

        assert.strictEqual(callCount, 1, 'second call should hit cache')
        assert.deepStrictEqual(result2, [{ id: 1, name: 'Alice' }])
    })

    it('does not cache when invalidation is not connected', async () => {
        NativeCache._reset()
        const cache = new NativeCache()
        // Leave _invalidationConnected as false
        const ext = cacheExtension({ _cache: cache })

        let callCount = 0
        const query = async () => {
            callCount++
            return [{ id: 1 }]
        }

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: { where: { id: 1 } },
            query,
        })
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: { where: { id: 1 } },
            query,
        })

        assert.strictEqual(callCount, 2, 'both calls should go to database')
    })

    it('invalidates cache on write operations', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let readCount = 0
        const readQuery = async () => {
            readCount++
            return [{ id: 1, name: 'Alice' }]
        }

        const writeQuery = async () => ({ id: 1, name: 'Bob' })

        const readArgs = { where: { active: true } }

        // Populate cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: readArgs,
            query: readQuery,
        })
        assert.strictEqual(readCount, 1)

        // Write invalidates cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'update',
            args: { where: { id: 1 }, data: { name: 'Bob' } },
            query: writeQuery,
        })

        // Read again — should miss cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: readArgs,
            query: readQuery,
        })
        assert.strictEqual(readCount, 2, 'should re-fetch after write invalidation')
    })

    it('caches different operations independently', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let findManyCount = 0
        let countCount = 0

        const findManyQuery = async () => { findManyCount++; return [{ id: 1 }] }
        const countQuery = async () => { countCount++; return 5 }

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query: findManyQuery,
        })
        await ext.query.$allOperations({
            model: 'User',
            operation: 'count',
            args: {},
            query: countQuery,
        })

        assert.strictEqual(findManyCount, 1)
        assert.strictEqual(countCount, 1)

        // Both should hit cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query: findManyQuery,
        })
        await ext.query.$allOperations({
            model: 'User',
            operation: 'count',
            args: {},
            query: countQuery,
        })

        assert.strictEqual(findManyCount, 1)
        assert.strictEqual(countCount, 1)
    })

    it('caches different args independently', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let callCount = 0
        const query = async (args) => { callCount++; return [{ id: args?.where?.id ?? 0 }] }

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: { where: { id: 1 } },
            query,
        })
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: { where: { id: 2 } },
            query,
        })

        assert.strictEqual(callCount, 2, 'different args should not share cache')
    })

    it('caches different models independently', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let userCount = 0
        let postCount = 0
        const userQuery = async () => { userCount++; return [{ id: 1, name: 'Alice' }] }
        const postQuery = async () => { postCount++; return [{ id: 1, title: 'Hello' }] }

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query: userQuery,
        })
        await ext.query.$allOperations({
            model: 'Post',
            operation: 'findMany',
            args: {},
            query: postQuery,
        })

        assert.strictEqual(userCount, 1)
        assert.strictEqual(postCount, 1)

        // Invalidate User — Post should still be cached
        await ext.query.$allOperations({
            model: 'User',
            operation: 'delete',
            args: { where: { id: 1 } },
            query: async () => ({ id: 1 }),
        })

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query: userQuery,
        })
        await ext.query.$allOperations({
            model: 'Post',
            operation: 'findMany',
            args: {},
            query: postQuery,
        })

        assert.strictEqual(userCount, 2, 'user cache invalidated')
        assert.strictEqual(postCount, 1, 'post cache still intact')
    })

    it('handles all write operations', async () => {
        const writeOps = [
            'create', 'createMany', 'createManyAndReturn',
            'update', 'updateMany', 'updateManyAndReturn',
            'upsert', 'delete', 'deleteMany',
        ]

        for (const op of writeOps) {
            const cache = mockCache()
            const ext = cacheExtension({ _cache: cache })

            let readCount = 0
            const readQuery = async () => { readCount++; return [] }
            const writeQuery = async () => ({})

            // Populate
            await ext.query.$allOperations({
                model: 'Item',
                operation: 'findMany',
                args: {},
                query: readQuery,
            })
            assert.strictEqual(readCount, 1, `${op}: initial read`)

            // Write
            await ext.query.$allOperations({
                model: 'Item',
                operation: op,
                args: {},
                query: writeQuery,
            })

            // Should miss cache
            await ext.query.$allOperations({
                model: 'Item',
                operation: 'findMany',
                args: {},
                query: readQuery,
            })
            assert.strictEqual(readCount, 2, `${op}: should invalidate cache`)
        }
    })

    it('handles all read operations', async () => {
        const readOps = [
            'findFirst', 'findFirstOrThrow',
            'findUnique', 'findUniqueOrThrow',
            'findMany', 'count', 'aggregate', 'groupBy',
        ]

        for (const op of readOps) {
            const cache = mockCache()
            const ext = cacheExtension({ _cache: cache })

            let callCount = 0
            const query = async () => { callCount++; return { data: op } }

            await ext.query.$allOperations({
                model: 'Widget',
                operation: op,
                args: { id: 1 },
                query,
            })
            await ext.query.$allOperations({
                model: 'Widget',
                operation: op,
                args: { id: 1 },
                query,
            })

            assert.strictEqual(callCount, 1, `${op}: second call should hit cache`)
        }
    })

    it('converts model name to lowercase table name', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let callCount = 0
        const readQuery = async () => { callCount++; return [] }
        const writeQuery = async () => ({})

        // Read with PascalCase model
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'findMany',
            args: {},
            query: readQuery,
        })

        // Write with same model
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'create',
            args: {},
            query: writeQuery,
        })

        // Should miss cache
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'findMany',
            args: {},
            query: readQuery,
        })
        assert.strictEqual(callCount, 2, 'write should invalidate reads for same table')
    })

    it('tracks cache stats', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        const query = async () => [{ id: 1 }]

        // Miss
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })

        // Hit
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })

        assert.strictEqual(cache.statsHits, 1)
        assert.strictEqual(cache.statsMisses, 1)
    })

    it('respects server-side invalidation signals', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let callCount = 0
        const query = async () => { callCount++; return [{ id: 1 }] }

        // Populate cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })
        assert.strictEqual(callCount, 1)

        // Simulate server-side invalidation signal
        cache.invalidateTable('user')

        // Should miss cache
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })
        assert.strictEqual(callCount, 2, 'should re-fetch after server invalidation')
    })

    it('handles invalidateAll', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        let userCount = 0
        let postCount = 0
        const userQuery = async () => { userCount++; return [] }
        const postQuery = async () => { postCount++; return [] }

        // Populate cache for two models
        await ext.query.$allOperations({ model: 'User', operation: 'findMany', args: {}, query: userQuery })
        await ext.query.$allOperations({ model: 'Post', operation: 'findMany', args: {}, query: postQuery })
        assert.strictEqual(userCount, 1)
        assert.strictEqual(postCount, 1)

        // Invalidate all
        cache.invalidateAll()

        // Both should miss
        await ext.query.$allOperations({ model: 'User', operation: 'findMany', args: {}, query: userQuery })
        await ext.query.$allOperations({ model: 'Post', operation: 'findMany', args: {}, query: postQuery })
        assert.strictEqual(userCount, 2, 'user refetched')
        assert.strictEqual(postCount, 2, 'post refetched')
    })

    it('uses default invalidation port when none specified', () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })
        // Should work without specifying a port
        assert.strictEqual(ext.name, 'goldlapel-cache')
    })

    it('does not store rows or fields in cache entries', async () => {
        const cache = mockCache()
        const ext = cacheExtension({ _cache: cache })

        const query = async () => [{ id: 1 }]

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })

        // Inspect the cache entry — should only have result and tables
        const entries = [...cache._cache.values()]
        assert.strictEqual(entries.length, 1)
        assert.ok('result' in entries[0])
        assert.ok('tables' in entries[0])
        assert.strictEqual('rows' in entries[0], false, 'rows should not be stored')
        assert.strictEqual('fields' in entries[0], false, 'fields should not be stored')
    })

    it('resolves @@map() table names via tableMap', async () => {
        const cache = mockCache()
        // Simulate DMMF: model "UserProfile" maps to table "user_profiles"
        const tableMap = new Map([['UserProfile', 'user_profiles']])
        const ext = cacheExtension({ _cache: cache, _tableMap: tableMap })

        let readCount = 0
        const readQuery = async () => { readCount++; return [] }
        const writeQuery = async () => ({})

        // Read with model name
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'findMany',
            args: {},
            query: readQuery,
        })
        assert.strictEqual(readCount, 1)

        // Write with same model — should invalidate using mapped table name
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'create',
            args: {},
            query: writeQuery,
        })

        // Read should miss cache since we invalidated the correct table
        await ext.query.$allOperations({
            model: 'UserProfile',
            operation: 'findMany',
            args: {},
            query: readQuery,
        })
        assert.strictEqual(readCount, 2, 'cache should be invalidated via mapped table name')
    })

    it('falls back to lowercase model name when no tableMap entry', async () => {
        const cache = mockCache()
        const tableMap = new Map([['OtherModel', 'other_table']])
        const ext = cacheExtension({ _cache: cache, _tableMap: tableMap })

        let readCount = 0
        const query = async () => { readCount++; return [] }

        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })
        await ext.query.$allOperations({
            model: 'User',
            operation: 'findMany',
            args: {},
            query,
        })

        assert.strictEqual(readCount, 1, 'second call should hit cache')
    })

    it('invalidation uses mapped table name for server-side signals', async () => {
        const cache = mockCache()
        const tableMap = new Map([['Account', 'accounts_v2']])
        const ext = cacheExtension({ _cache: cache, _tableMap: tableMap })

        let callCount = 0
        const query = async () => { callCount++; return [{ id: 1 }] }

        // Populate cache
        await ext.query.$allOperations({
            model: 'Account',
            operation: 'findMany',
            args: {},
            query,
        })
        assert.strictEqual(callCount, 1)

        // Simulate server-side invalidation using the ACTUAL table name
        cache.invalidateTable('accounts_v2')

        // Should miss cache since the table name matches the mapped name
        await ext.query.$allOperations({
            model: 'Account',
            operation: 'findMany',
            args: {},
            query,
        })
        assert.strictEqual(callCount, 2, 'should re-fetch after invalidation of mapped table name')
    })
})


describe('re-exports', () => {
    it('re-exports start from goldlapel', () => {
        assert.strictEqual(typeof start, 'function')
    })

    it('re-exports stop from goldlapel', () => {
        assert.strictEqual(typeof stop, 'function')
    })

    it('re-exports proxyUrl from goldlapel', () => {
        assert.strictEqual(typeof proxyUrl, 'function')
    })

    it('re-exports GoldLapel from goldlapel', () => {
        assert.strictEqual(typeof GoldLapel, 'function')
    })

    it('re-exports NativeCache from goldlapel', () => {
        assert.strictEqual(typeof NativeCache, 'function')
    })

    it('exports cacheExtension', () => {
        assert.strictEqual(typeof cacheExtension, 'function')
    })
})
