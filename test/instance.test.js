import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GoldLapel } from '../index.js';

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

// ─── wrapper method delegation to default conn ────────────────────────────

describe('wrapper methods delegate to default conn', () => {
    it('docInsert delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const row = { _id: 'abc', data: { name: 'Alice' }, created_at: 'now' };
        gl._defaultConn = mockClient({ rows: [row], rowCount: 1 });

        const result = await gl.docInsert('users', { name: 'Alice' });
        assert.deepEqual(result, row);
        // ensureCollection + INSERT = 2 calls
        assert.strictEqual(gl._defaultConn._calls.length, 2);
        assert.ok(gl._defaultConn._calls[0].text.includes('CREATE TABLE IF NOT EXISTS users'));
        assert.ok(gl._defaultConn._calls[1].text.includes('INSERT INTO users'));
    });

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

    it('docFind delegates with spread args', async () => {
        const rows = [{ _id: 'a', data: { x: 1 } }];
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows, rowCount: 1 });

        const result = await gl.docFind('items', { x: 1 });
        assert.deepEqual(result, rows);
        assert.ok(gl._defaultConn._calls[0].text.includes('FROM items'));
    });

    it('incr delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [{ value: '5' }], rowCount: 1 });

        const result = await gl.incr('counters', 'page_views', 1);
        assert.strictEqual(result, 5);
    });

    it('hset delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [], rowCount: 0 });

        await gl.hset('cache', 'session1', 'user', { id: 42 });
        // CREATE TABLE + INSERT = 2 calls
        assert.strictEqual(gl._defaultConn._calls.length, 2);
        assert.ok(gl._defaultConn._calls[1].text.includes('jsonb_build_object'));
    });

    it('zadd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [], rowCount: 0 });

        await gl.zadd('leaderboard', 'player1', 100);
        // CREATE TABLE + INSERT = 2 calls
        assert.strictEqual(gl._defaultConn._calls.length, 2);
        assert.ok(gl._defaultConn._calls[1].text.includes('INSERT INTO leaderboard'));
    });

    it('publish delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [], rowCount: 0 });

        await gl.publish('events', 'hello');
        assert.strictEqual(gl._defaultConn._calls.length, 1);
        assert.ok(gl._defaultConn._calls[0].text.includes('pg_notify'));
    });

    it('streamAdd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._defaultConn = mockClient({ rows: [{ id: '1' }], rowCount: 1 });

        const id = await gl.streamAdd('events', { type: 'click' });
        assert.strictEqual(id, 1);
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
    it('docInsert throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.docInsert('users', { name: 'Alice' }),
            /Not connected/
        );
    });

    it('search throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.search('articles', 'body', 'hello'),
            /Not connected/
        );
    });

    it('hset throws', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.hset('cache', 'k', 'f', 'v'),
            /Not connected/
        );
    });
});

// ─── all methods exist on the class ───────────────────────────────────────

describe('all wrapper methods exist', () => {
    const expectedMethods = [
        // Document store
        'docInsert', 'docInsertMany', 'docFind', 'docFindOne',
        'docUpdate', 'docUpdateOne', 'docDelete', 'docDeleteOne',
        'docCount', 'docCreateIndex', 'docAggregate',
        // Search
        'search', 'searchFuzzy', 'searchPhonetic', 'similar', 'suggest',
        'facets', 'aggregate', 'createSearchConfig',
        // Percolation
        'percolateAdd', 'percolate', 'percolateDelete',
        // Analysis
        'analyze', 'explainScore',
        // Pub/Sub & Queues
        'publish', 'subscribe', 'enqueue', 'dequeue',
        // Counters
        'incr', 'getCounter',
        // Hash maps
        'hset', 'hget', 'hgetall', 'hdel',
        // Sorted sets
        'zadd', 'zincrby', 'zrange', 'zrank', 'zscore', 'zrem',
        // Geo
        'geoadd', 'georadius', 'geodist',
        // Misc
        'countDistinct', 'script',
        // Streams
        'streamAdd', 'streamCreateGroup', 'streamRead', 'streamAck', 'streamClaim',
    ];

    const gl = new GoldLapel('postgresql://localhost:5432/mydb');

    for (const method of expectedMethods) {
        it(`has ${method}()`, () => {
            assert.strictEqual(typeof gl[method], 'function');
        });
    }

    it('count matches expected total (50)', () => {
        assert.strictEqual(expectedMethods.length, 50);
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
    it('docInsert uses override conn', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [{ _id: 'd', data: {}, created_at: 'now' }], rowCount: 1 });
        const override = mockClient({ rows: [{ _id: 'o', data: {}, created_at: 'now' }], rowCount: 1 });
        gl._defaultConn = def;

        const result = await gl.docInsert('users', { name: 'Alice' }, { conn: override });
        assert.strictEqual(result._id, 'o');
        assert.strictEqual(def._calls.length, 0, 'default conn not touched');
        assert.strictEqual(override._calls.length, 2, 'override conn used');
    });

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

    it('docInsertMany uses override conn', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({
            rows: [
                { _id: 'a', data: {}, created_at: 'now' },
                { _id: 'b', data: {}, created_at: 'now' },
            ],
            rowCount: 2,
        });
        gl._defaultConn = def;

        const result = await gl.docInsertMany('items', [{ x: 1 }, { x: 2 }], { conn: override });
        assert.strictEqual(result.length, 2);
        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(override._calls.length, 2);
    });
});
