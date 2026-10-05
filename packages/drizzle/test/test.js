import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'

import {
    drizzle, init,
    start, GoldLapel,
    DocumentsAPI, StreamsAPI,
} from '../index.js'
import * as plugin from '../index.js'

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

    it('calls start with DATABASE_URL and passes the plain pool to drizzle', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _pg, pools } = mockPg()

        const db = await drizzle({ _start, _drizzle, _pg })

        assert.strictEqual(calls.length, 1)
        assert.strictEqual(calls[0].upstream, 'postgresql://user:pass@host:5432/mydb')
        assert.deepStrictEqual(calls[0].opts, { noConnect: true })
        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
        assert.strictEqual(drizzleCalls.length, 1)
        assert.strictEqual(drizzleCalls[0].client, pools[0])
        assert.strictEqual(db._mock, true)
    })

    it('uses explicit url over env', async () => {
        process.env.DATABASE_URL = 'postgresql://env@host:5432/db'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({
            url: 'postgresql://explicit@host:5432/db',
            _start,
            _drizzle,
            _pg,
        })

        assert.strictEqual(calls[0].upstream, 'postgresql://explicit@host:5432/db')
    })

    it('throws when no DATABASE_URL', async () => {
        await assert.rejects(
            () => drizzle({
                _start: mockStart('x')._start,
                _drizzle: mockDrizzle()._drizzle,
                _pg: mockPg()._pg,
            }),
            /DATABASE_URL not set/,
        )
    })

    it('passes port to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({ proxyPort: 9000, _start, _drizzle, _pg })

        assert.strictEqual(calls[0].opts.proxyPort, 9000)
    })

    it('passes extraArgs to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({
            extraArgs: ['--verbose'],
            _start,
            _drizzle,
            _pg,
        })

        assert.deepStrictEqual(calls[0].opts.extraArgs, ['--verbose'])
    })

    it('passes config to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({
            config: { poolMode: 'transaction', poolSize: 30, disableN1: true },
            _start,
            _drizzle,
            _pg,
        })

        assert.deepStrictEqual(calls[0].opts.config, { poolMode: 'transaction', poolSize: 30, disableN1: true })
    })

    it('strips GL options from drizzle options', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({
            config: { poolMode: 'transaction' },
            schema: { users: 'mock' },
            _start,
            _drizzle,
            _pg,
        })

        assert.strictEqual(drizzleCalls[0].options.config, undefined)
        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' } })
    })

    it('forwards drizzle options and strips all GL options', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({
            schema: { users: 'mock' },
            logger: true,
            url: 'postgresql://user:pass@host:5432/mydb',
            proxyPort: 9000,
            config: { poolMode: 'transaction' },
            extraArgs: ['--verbose'],
            _start,
            _drizzle,
            _pg,
        })

        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' }, logger: true })
    })

    it('returns instance from drizzle factory', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        const db = await drizzle({ _start, _drizzle, _pg })

        assert.strictEqual(db._mock, true)
    })

    it('sets GOLDLAPEL_CLIENT only if not already set', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()

        // When no GOLDLAPEL_CLIENT is set, drizzle sets it
        await drizzle({ _start, _drizzle, _pg })
        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'drizzle')

        // When GOLDLAPEL_CLIENT is already set, it should not be overwritten
        process.env.GOLDLAPEL_CLIENT = 'prisma'
        await drizzle({ _start, _drizzle, _pg })
        assert.strictEqual(process.env.GOLDLAPEL_CLIENT, 'prisma')
    })
})


describe('drizzle pool', () => {
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

    it('creates pg.Pool with proxy URL connection string', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle } = mockDrizzle()
        const { _pg, pools } = mockPg()

        await drizzle({ _start, _drizzle, _pg })

        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
    })

    it('handles start returning a GoldLapel instance by using instance.url', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        // Simulate start() returning an instance (new v0.2 behavior)
        const instance = { url: 'postgresql://user:pass@localhost:7932/mydb', query: () => {} }
        const { _start } = mockStart(instance)
        const { _drizzle } = mockDrizzle()
        const { _pg, pools } = mockPg()

        await drizzle({ _start, _drizzle, _pg })

        assert.strictEqual(pools.length, 1)
        assert.strictEqual(pools[0]._opts.connectionString, 'postgresql://user:pass@localhost:7932/mydb')
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

    it('passes proxyPort to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')

        await init({ proxyPort: 9000, _start })

        assert.strictEqual(calls[0].opts.proxyPort, 9000)
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

    it('no longer exports the in-process cache', () => {
        assert.strictEqual(plugin.wrap, undefined)
        assert.strictEqual(plugin.NativeCache, undefined)
    })

    it('re-exports DocumentsAPI from goldlapel', () => {
        assert.strictEqual(typeof DocumentsAPI, 'function')
    })

    it('re-exports StreamsAPI from goldlapel', () => {
        assert.strictEqual(typeof StreamsAPI, 'function')
    })

    // Regression guard: the broken flat doc-store re-exports were dropped in
    // favor of the nested gl.documents.<verb> surface (see CHANGELOG). Make
    // sure they don't sneak back in — re-adding any of them would silently
    // break callers, since the underlying utils now require a `patterns`
    // arg they can't supply.
    it('does not re-export flat doc-store utilities', () => {
        const removed = [
            'docInsert', 'docInsertMany', 'docFind', 'docFindOne',
            'docUpdate', 'docUpdateOne', 'docDelete', 'docDeleteOne',
            'docCount', 'docCreateIndex', 'docAggregate',
            'docWatch', 'docUnwatch', 'docCreateTtlIndex', 'docRemoveTtlIndex',
            'docCreateCapped', 'docRemoveCap',
        ]
        for (const name of removed) {
            assert.strictEqual(plugin[name], undefined, `${name} should not be re-exported`)
        }
    })
})


describe('goldlapel options', () => {
    const origUrl = process.env.DATABASE_URL

    beforeEach(() => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
    })

    afterEach(() => {
        if (origUrl !== undefined) {
            process.env.DATABASE_URL = origUrl
        } else {
            delete process.env.DATABASE_URL
        }
    })

    // Every start() option, as a caller might set it.
    const allOptions = {
        proxyPort: 9000, dashboardPort: 9100, logLevel: 'debug', mode: 'waiter',
        license: '/etc/gl.pem', client: 'my-app', configFile: 'goldlapel.toml',
        config: { poolSize: 5 }, extraArgs: ['--verbose'], silent: true,
        mesh: true, meshTag: 'eu', disableProxyCache: true,
        disableSqloptimize: true, disableAutoIndexes: true,
    }

    it('covers every start() option', async () => {
        const { startOptionKeys } = await import('goldlapel')
        const keys = new Set(Object.keys(allOptions))
        keys.add('noConnect')
        assert.deepStrictEqual([...keys].sort(), [...startOptionKeys()].sort())
    })

    it('drizzle forwards every start() option to start and none to drizzle-orm', async () => {
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({ ...allOptions, schema: { users: 'mock' }, casing: 'snake_case', _start, _drizzle, _pg })

        assert.deepStrictEqual(calls[0].opts, { ...allOptions, noConnect: true })
        assert.deepStrictEqual(drizzleCalls[0].options, { schema: { users: 'mock' }, casing: 'snake_case' })
    })

    it('init forwards every start() option', async () => {
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')
        await init({ ...allOptions, _start })
        assert.deepStrictEqual(calls[0].opts, { ...allOptions, noConnect: true })
    })

    it('removed options go to start(), never to drizzle-orm', async () => {
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        const { _drizzle, calls: drizzleCalls } = mockDrizzle()
        const { _pg } = mockPg()

        await drizzle({ nativeCache: true, invalidationPort: 7934, _start, _drizzle, _pg })

        assert.deepStrictEqual(calls[0].opts, { nativeCache: true, invalidationPort: 7934, noConnect: true })
        assert.deepStrictEqual(drizzleCalls[0].options, {})
    })

    it('rejects removed options, naming why', async () => {
        const { _drizzle } = mockDrizzle()
        const { _pg } = mockPg()
        await assert.rejects(
            () => drizzle({ nativeCache: true, _drizzle, _pg }),
            /Unknown options: nativeCache \(removed with the in-process cache\)/,
        )
    })

    it('init rejects options that are not start() options', async () => {
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')
        await assert.rejects(
            () => init({ schema: {}, _start }),
            /Unknown options: schema/,
        )
        assert.strictEqual(calls.length, 0)
    })
})
