import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'

import { drizzle, init, start, GoldLapel, wrap, NativeCache } from '../index.js'

function mockStart(returnUrl) {
    const calls = []
    async function _start(upstream, opts) {
        calls.push({ upstream, opts })
        return returnUrl
    }
    return { _start, calls }
}

function mockDrizzle() {
    const calls = []
    function _drizzle(client, options) {
        calls.push({ client, options })
        return { _mock: true, client, options }
    }
    return { _drizzle, calls }
}

function mockWrap() {
    const calls = []
    function _wrap(client, invalidationPort) {
        calls.push({ client, invalidationPort })
        return { _wrapped: true, _client: client, _invalidationPort: invalidationPort }
    }
    return { _wrap, calls }
}

function mockPg() {
    const pools = []
    class Pool {
        constructor(opts) {
            this._opts = opts
            this._mockPool = true
            pools.push(this)
        }
    }
    return { _pg: { Pool }, pools }
}


describe('drizzle', () => {
    const origUrl = process.env.DATABASE_URL
    const origClient = process.env.GOLDLAPEL_CLIENT

    beforeEach(() => {
        delete process.env.DATABASE_URL
        delete process.env.GOLDLAPEL_CLIENT
    })

    afterEach(() => {
        if (origUrl !== undefined) {
            process.env.DATABASE_URL = origUrl
        } else {
            delete process.env.DATABASE_URL
        }
        if (origClient !== undefined) {
            process.env.GOLDLAPEL_CLIENT = origClient
        } else {
            delete process.env.GOLDLAPEL_CLIENT
        }
    })

    it('calls start with DATABASE_URL and passes wrapped pool to drizzle', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg, pools } = mockPg()

        const db = await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(calls.length, 1)
        assert.strictEqual(calls[0].upstream, 'postgresql://user:pass@host:5432/mydb')
        assert.deepStrictEqual(calls[0].opts, { config: undefined, port: undefined, extraArgs: undefined, noConnect: true })
        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
        assert.strictEqual(wrapCalls.length, 1)
        assert.strictEqual(wrapCalls[0].client, pools[0])
        assert.strictEqual(wrapCalls[0].invalidationPort, 7934)
        assert.strictEqual(drizzleCalls.length, 1)
        assert.strictEqual(drizzleCalls[0].client._wrapped, true)
        assert.strictEqual(db._mock, true)
    })

    it('uses explicit url over env', async () => {
        process.env.DATABASE_URL = 'postgresql://env@host:5432/db'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            url: 'postgresql://explicit@host:5432/db',
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.strictEqual(calls[0].upstream, 'postgresql://explicit@host:5432/db')
    })

    it('throws when no DATABASE_URL', async () => {
        await assert.rejects(
            () => drizzle({
                _start: mockStart('x')._start,
                _drizzle: mockDrizzle()._drizzle,
                _wrap: mockWrap()._wrap,
                _pg: mockPg()._pg,
            }),
            /DATABASE_URL not set/,
        )
    })

    it('passes port to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({ port: 9000, _start, _drizzle, _wrap, _pg })

        assert.strictEqual(calls[0].opts.port, 9000)
    })

    it('passes extraArgs to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            extraArgs: ['--verbose'],
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.deepStrictEqual(calls[0].opts.extraArgs, ['--verbose'])
    })

    it('passes config to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            config: { mode: 'waiter', poolSize: 30, disableN1: true },
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.deepStrictEqual(calls[0].opts.config, { mode: 'waiter', poolSize: 30, disableN1: true })
    })

    it('strips GL options from drizzle options', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            config: { mode: 'waiter' },
            schema: { users: 'mock' },
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.strictEqual(drizzleCalls[0].options.config, undefined)
        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' } })
    })

    it('forwards drizzle options and strips all GL options', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            schema: { users: 'mock' },
            logger: true,
            url: 'postgresql://user:pass@host:5432/mydb',
            port: 9000,
            config: { mode: 'waiter' },
            extraArgs: ['--verbose'],
            invalidationPort: 8000,
            nativeCache: true,
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' }, logger: true })
    })

    it('returns instance from drizzle factory', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        const db = await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(db._mock, true)
    })

    it('sets GOLDLAPEL_CLIENT only if not already set', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        // When no GOLDLAPEL_CLIENT is set, drizzle sets it
        await drizzle({ _start, _drizzle, _wrap, _pg })
        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'drizzle')

        // When GOLDLAPEL_CLIENT is already set, it should not be overwritten
        process.env.GOLDLAPEL_CLIENT = 'prisma'
        await drizzle({ _start, _drizzle, _wrap, _pg })
        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'prisma')
    })
})


describe('drizzle L1 cache', () => {
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

    it('wraps pool with L1 cache by default', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg, pools } = mockPg()

        await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls.length, 1)
        assert.strictEqual(wrapCalls[0].client, pools[0])
        assert.strictEqual(drizzleCalls[0].client._wrapped, true)
    })

    it('uses default invalidation port (proxy port + 2)', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls[0].invalidationPort, 7934)
    })

    it('uses custom invalidation port when specified', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({ invalidationPort: 9999, _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls[0].invalidationPort, 9999)
    })

    it('computes invalidation port from custom proxy port', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({ port: 9000, _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls[0].invalidationPort, 9002)
    })

    it('explicit invalidationPort overrides port-based default', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({ port: 9000, invalidationPort: 5555, _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls[0].invalidationPort, 5555)
    })

    it('skips wrapping when nativeCache is false', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap, calls: wrapCalls } = mockWrap()
        const { _pg, pools } = mockPg()

        await drizzle({ nativeCache: false, _start, _drizzle, _wrap, _pg })

        assert.strictEqual(wrapCalls.length, 0)
        assert.strictEqual(drizzleCalls[0].client._mockPool, true)
        assert.strictEqual(drizzleCalls[0].client, pools[0])
    })

    it('creates pg.Pool with proxy URL connection string', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg, pools } = mockPg()

        await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('handles start returning a GoldLapel instance by using instance.url', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        // Simulate start() returning an instance (new v0.2 behavior)
        const instance = { url: 'postgresql://user:pass@localhost:7932/mydb', query: () => {} }
        const { _start } = mockStart(instance)
        const { _drizzle } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg, pools } = mockPg()

        await drizzle({ _start, _drizzle, _wrap, _pg })

        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('strips nativeCache and invalidationPort from drizzle options', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _wrap } = mockWrap()
        const { _pg } = mockPg()

        await drizzle({
            schema: { users: 'mock' },
            invalidationPort: 9999,
            nativeCache: true,
            _start,
            _drizzle,
            _wrap,
            _pg,
        })

        assert.strictEqual(drizzleCalls[0].options.invalidationPort, undefined)
        assert.strictEqual(drizzleCalls[0].options.nativeCache, undefined)
        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' } })
    })
})


describe('init', () => {
    const origUrl = process.env.DATABASE_URL
    const origClient = process.env.GOLDLAPEL_CLIENT

    beforeEach(() => {
        delete process.env.DATABASE_URL
        delete process.env.GOLDLAPEL_CLIENT
    })

    afterEach(() => {
        if (origUrl !== undefined) {
            process.env.DATABASE_URL = origUrl
        } else {
            delete process.env.DATABASE_URL
        }
        if (origClient !== undefined) {
            process.env.GOLDLAPEL_CLIENT = origClient
        } else {
            delete process.env.GOLDLAPEL_CLIENT
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

        await init({ config: { mode: 'waiter', poolSize: 30, disableN1: true }, _start })

        assert.deepStrictEqual(calls[0].opts.config, { mode: 'waiter', poolSize: 30, disableN1: true })
    })

    it('sets DATABASE_URL even when using explicit url', async () => {
        process.env.DATABASE_URL = 'postgresql://original@host:5432/db'
        const { _start } = mockStart('postgresql://explicit@localhost:7932/db')

        await init({ url: 'postgresql://explicit@host:5432/db', _start })

        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://explicit@localhost:7932/db')
    })

    it('handles start returning a GoldLapel instance via instance.url', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const instance = { url: 'postgresql://user:pass@localhost:7932/mydb' }
        const { _start } = mockStart(instance)

        const result = await init({ _start })

        assert.strictEqual(result, 'postgresql://user:pass@localhost:7932/mydb')
        assert.strictEqual(process.env.DATABASE_URL, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('does not overwrite existing GOLDLAPEL_CLIENT', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        process.env.GOLDLAPEL_CLIENT = 'prisma'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        await init({ _start })

        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'prisma')
    })
})


describe('re-exports', () => {
    it('re-exports start from goldlapel', () => {
        assert.strictEqual(typeof start, 'function')
    })

    it('re-exports GoldLapel from goldlapel', () => {
        assert.strictEqual(typeof GoldLapel, 'function')
    })

    it('re-exports wrap from goldlapel', () => {
        assert.strictEqual(typeof wrap, 'function')
    })

    it('re-exports NativeCache from goldlapel', () => {
        assert.strictEqual(typeof NativeCache, 'function')
    })
})
