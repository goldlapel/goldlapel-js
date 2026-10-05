import { describe, it, beforeEach, afterEach } from 'node:test'
import assert from 'node:assert'

import {
    withGoldLapel, init,
    start, GoldLapel,
    DocumentsAPI, StreamsAPI,
} from '../index.js'
import * as plugin from '../index.js'

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
    }
}


describe('withGoldLapel', () => {
    const origUrl = process.env.DATABASE_URL

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
        if (origGoldlapelClient !== undefined) {
            process.env.GOLDLAPEL_CLIENT = origGoldlapelClient
        } else {
            delete process.env.GOLDLAPEL_CLIENT
        }
    })

    it('calls start with DATABASE_URL and returns PrismaClient with proxy URL', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const client = await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(calls.length, 1)
        assert.strictEqual(calls[0].upstream, 'postgresql://user:pass@host:5432/mydb')
        assert.deepStrictEqual(calls[0].opts, { noConnect: true })
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

    it('passes proxyPort to start', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')

        await withGoldLapel({ proxyPort: 9000, _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(calls[0].opts.proxyPort, 9000)
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

    it('returns a plain PrismaClient (no client extensions)', async () => {
        process.env.DATABASE_URL = 'postgresql://user:pass@host:5432/mydb'
        const { _start } = mockStart('postgresql://user:pass@localhost:7932/mydb')

        const client = await withGoldLapel({ _start, _PrismaClient: MockPrismaClient })

        assert.strictEqual(Object.getPrototypeOf(client), MockPrismaClient.prototype)
        assert.deepStrictEqual(client._opts, {
            datasources: { db: { url: 'postgresql://user:pass@localhost:7932/mydb' } },
        })
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


describe('re-exports', () => {
    it('re-exports start from goldlapel', () => {
        assert.strictEqual(typeof start, 'function')
    })

    it('re-exports GoldLapel from goldlapel', () => {
        assert.strictEqual(typeof GoldLapel, 'function')
    })

    it('re-exports DocumentsAPI from goldlapel', () => {
        assert.strictEqual(typeof DocumentsAPI, 'function')
    })

    it('re-exports StreamsAPI from goldlapel', () => {
        assert.strictEqual(typeof StreamsAPI, 'function')
    })

    it('no longer exports the in-process cache', () => {
        assert.strictEqual(plugin.cacheExtension, undefined)
        assert.strictEqual(plugin.NativeCache, undefined)
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

    for (const [name, fn] of [['withGoldLapel', withGoldLapel], ['init', init]]) {
        it(`${name} forwards every start() option as given`, async () => {
            const { _start, calls } = mockStart('postgresql://user:pass@localhost:9000/mydb')
            await fn({ ...allOptions, _start, _PrismaClient: MockPrismaClient })
            assert.deepStrictEqual(calls[0].opts, { ...allOptions, noConnect: true })
        })

        it(`${name} rejects options start() removed, naming why`, async () => {
            await assert.rejects(
                () => fn({ invalidationPort: 7934, _PrismaClient: MockPrismaClient }),
                /Unknown options: invalidationPort \(removed with the in-process cache/,
            )
        })
    }
})
