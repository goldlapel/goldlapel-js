import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GoldLapel, DocumentsAPI, StreamsAPI } from '../index.js';

function mockClient(queryResult) {
    const calls = [];
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], rowCount: 0 };
        },
        on() {},
        end: async () => {},
        _calls: calls,
    };
}

// ─── connection resolution ────────────────────────────────────────────────

describe('_resolveConn', () => {
    it('throws before start() and with no override / scoped conn', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.throws(
            () => gl._resolveConn(),
            /Not connected/
        );
    });

    it('returns default conn when set', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const mock = mockClient();
        gl._defaultConn = mock;
        assert.strictEqual(gl._resolveConn(), mock);
    });

    it('override wins over default', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient();
        const override = mockClient();
        gl._defaultConn = def;
        assert.strictEqual(gl._resolveConn(override), override);
    });
});

// ─── stop() disconnects default conn ──────────────────────────────────────

describe('stop() with default conn', () => {
    it('calls close on default conn and nulls it', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        let closeCalled = false;
        gl._defaultConn = { end: async () => {} };
        gl._defaultClose = async () => { closeCalled = true; };
        await gl.stop();
        assert.strictEqual(closeCalled, true);
        assert.strictEqual(gl._defaultConn, null);
    });

    it('nulls default conn even when no process', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = { end: async () => {} };
        gl._defaultClose = async () => {};
        await gl.stop();
        assert.strictEqual(gl._defaultConn, null);
    });
});

// ─── nested namespaces (Phase 4 schema-to-core) ───────────────────────────

describe('nested namespaces', () => {
    it('gl.documents is a DocumentsAPI bound to the parent client', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.ok(gl.documents instanceof DocumentsAPI);
        assert.strictEqual(gl.documents._gl, gl);
    });

    it('gl.streams is a StreamsAPI bound to the parent client', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.ok(gl.streams instanceof StreamsAPI);
        assert.strictEqual(gl.streams._gl, gl);
    });

    it('legacy flat doc* methods are gone (hard cut, no aliases)', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        for (const legacy of [
            'docInsert', 'docFind', 'docUpdate', 'docDelete', 'docCount',
            'docCreateCollection', 'docCreateIndex', 'docAggregate',
        ]) {
            assert.strictEqual(
                gl[legacy], undefined,
                `Legacy flat method ${legacy} should have been removed; use gl.documents.<verb> instead.`,
            );
        }
    });

    it('legacy flat stream* methods are gone (hard cut, no aliases)', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        for (const legacy of [
            'streamAdd', 'streamCreateGroup', 'streamRead',
            'streamAck', 'streamClaim',
        ]) {
            assert.strictEqual(
                gl[legacy], undefined,
                `Legacy flat method ${legacy} should have been removed; use gl.streams.<verb> instead.`,
            );
        }
    });
});

// ─── wrapper method delegation to default conn ────────────────────────────

describe('wrapper methods delegate to default conn', () => {
    it('search delegates with spread args', async () => {
        const rows = [{ id: 1, _score: 0.8 }];
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows, rowCount: 1 });

        const result = await gl.search('articles', 'body', 'hello');
        assert.deepEqual(result, rows);
        assert.strictEqual(gl._defaultConn._calls.length, 1);
        const sql = gl._defaultConn._calls[0].text;
        assert.ok(sql.includes('FROM articles'));
        assert.ok(sql.includes("to_tsvector($1, coalesce(body, ''))"));
    });

    it('publish delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [], rowCount: 0 });

        await gl.publish('events', 'hello');
        assert.strictEqual(gl._defaultConn._calls.length, 1);
        assert.ok(gl._defaultConn._calls[0].text.includes('pg_notify'));
    });

    it('percolateAdd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [], rowCount: 0 });

        await gl.percolateAdd('alerts', 'q1', 'breaking news');
        // CREATE TABLE + CREATE INDEX + INSERT = 3 calls
        assert.strictEqual(gl._defaultConn._calls.length, 3);
        assert.ok(gl._defaultConn._calls[0].text.includes('CREATE TABLE IF NOT EXISTS alerts'));
    });

    it('analyze delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [{ alias: 'asciiword' }], rowCount: 1 });

        const result = await gl.analyze('hello');
        assert.deepEqual(result, [{ alias: 'asciiword' }]);
    });

    it('countDistinct delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [{ cnt: '42' }], rowCount: 1 });

        const result = await gl.countDistinct('users', 'email');
        assert.strictEqual(result, 42);
    });
});

// ─── wrapper methods throw when no conn resolvable ────────────────────────

describe('wrapper methods throw before conn is available', () => {
    it('search throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.search('articles', 'body', 'hello'),
            /Not connected/
        );
    });

    it('gl.documents.find throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        // Before start(), there's no dashboard token + no default conn —
        // expect either flavour of "not connected" (no token, or no
        // conn after a token came from env/file).
        await assert.rejects(
            () => gl.documents.find('users', { x: 1 }),
            /(No dashboard token|Not connected|dashboard not reachable|No dashboard port)/i,
        );
    });

    it('gl.streams.add throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.streams.add('events', { type: 'click' }),
            /(No dashboard token|Not connected|dashboard not reachable|No dashboard port)/i,
        );
    });
});

// ─── all methods exist on the class ───────────────────────────────────────

describe('all wrapper methods exist', () => {
    // Note: doc* and stream* methods moved under gl.documents.<verb> /
    // gl.streams.<verb> in Phase 4. Phase 5 moved the Redis-compat helpers
    // (counter / zset / hash / queue / geo) to their own namespaces too.
    // Search / pub-sub / analysis / misc stay flat — they'll migrate when
    // their own schema-to-core phase fires.
    const expectedMethods = [
        // Search
        'search', 'searchFuzzy', 'searchPhonetic', 'similar', 'suggest',
        'facets', 'aggregate', 'createSearchConfig',
        // Percolation
        'percolateAdd', 'percolate', 'percolateDelete',
        // Analysis
        'analyze', 'explainScore',
        // Pub/Sub
        'publish', 'subscribe',
        // Misc
        'countDistinct', 'script',
    ];

    const gl = new GoldLapel('postgresql://localhost:5432/mydb');

    for (const method of expectedMethods) {
        it(`has ${method}()`, () => {
            assert.strictEqual(typeof gl[method], 'function');
        });
    }

    it('count matches expected total (17)', () => {
        assert.strictEqual(expectedMethods.length, 17);
    });

    it('has all Phase 4 + Phase 5 sub-APIs', async () => {
        const { CountersAPI, ZsetsAPI, HashesAPI, QueuesAPI, GeosAPI } = await import('../index.js');
        assert.ok(gl.documents instanceof DocumentsAPI);
        assert.ok(gl.streams instanceof StreamsAPI);
        assert.ok(gl.counters instanceof CountersAPI);
        assert.ok(gl.zsets instanceof ZsetsAPI);
        assert.ok(gl.hashes instanceof HashesAPI);
        assert.ok(gl.queues instanceof QueuesAPI);
        assert.ok(gl.geos instanceof GeosAPI);
    });

    it('Phase 5 hard cut — legacy flat methods are gone (no aliases)', () => {
        // Per CLAUDE.md / master-plan no-aliases rule: every Phase-5 helper
        // moved to its sub-API namespace. Search/replace migration on
        // upgrade; no compat shims.
        const removed = [
            'incr', 'getCounter',
            'hset', 'hget', 'hgetall', 'hdel',
            'zadd', 'zincrby', 'zrange', 'zrank', 'zscore', 'zrem',
            'geoadd', 'georadius', 'geodist',
            'enqueue', 'dequeue',
        ];
        for (const legacy of removed) {
            assert.strictEqual(
                gl[legacy], undefined,
                `Phase 5 removed flat ${legacy} — use gl.<family>.<verb> instead.`,
            );
        }
    });

    it('list matches actual public wrapper surface', () => {
        // Enumerate public methods on the prototype (skip _private, stop, using,
        // and getters like url/running/dashboardUrl). This guards against drift
        // in either direction — new public method added without test coverage,
        // or method removed without updating the list.
        const proto = Object.getPrototypeOf(gl);
        const actual = new Set();
        for (const name of Object.getOwnPropertyNames(proto)) {
            if (name === 'constructor') continue;
            if (name === 'stop' || name === 'using') continue;
            if (name.startsWith('_')) continue;
            const desc = Object.getOwnPropertyDescriptor(proto, name);
            if (desc.get || desc.set) continue;
            if (typeof desc.value === 'function') actual.add(name);
        }
        const expected = new Set(expectedMethods);
        const missing = [...actual].filter((n) => !expected.has(n));
        const extra = [...expected].filter((n) => !actual.has(n));
        assert.deepStrictEqual(
            { missing, extra },
            { missing: [], extra: [] },
            `expectedMethods drift: missing=${JSON.stringify(missing)} extra=${JSON.stringify(extra)}`
        );
    });

    it('has using()', () => {
        assert.strictEqual(typeof gl.using, 'function');
    });

    it('has Symbol.asyncDispose', () => {
        assert.strictEqual(typeof gl[Symbol.asyncDispose], 'function');
    });
});

// ─── per-method { conn } override ─────────────────────────────────────────

describe('per-method { conn } override', () => {
    it('search uses override conn', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({ rows: [{ id: 42 }], rowCount: 1 });
        gl._defaultConn = def;

        const result = await gl.search('articles', 'body', 'hello', { conn: override });
        assert.deepEqual(result, [{ id: 42 }]);
        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(override._calls.length, 1);
    });

    it('publish (no options) uses override conn', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        await gl.publish('events', 'hello', { conn: override });
        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(override._calls.length, 1);
    });

    it('method works when no override conn key (normal options pass through)', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        // {limit: 10} should reach search() as options, not be treated as override
        await gl.search('articles', 'body', 'hello', { limit: 10 });
        assert.strictEqual(def._calls.length, 1);
        // values: [lang, lang, query, limit]
        assert.deepEqual(def._calls[0].values, ['english', 'english', 'hello', 10]);
    });

    it('{ conn } mixed with other options strips conn and keeps other options', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        await gl.search('articles', 'body', 'hello', { limit: 10, conn: override });

        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(override._calls.length, 1);
        // limit still respected: values: [lang, lang, query, limit]
        assert.deepEqual(override._calls[0].values, ['english', 'english', 'hello', 10]);
    });
});
