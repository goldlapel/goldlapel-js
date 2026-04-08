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

// ─── client getter ─────────────────────────────────────────────────────────

describe('client getter', () => {
    it('throws before start()', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.throws(
            () => gl.client,
            /Not connected\. Call start\(\) before accessing client\./
        );
    });

    it('returns client when _client is set', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const mock = mockClient();
        gl._client = mock;
        assert.strictEqual(gl.client, mock);
    });
});

// ─── stop() disconnects client ─────────────────────────────────────────────

describe('stop() with client', () => {
    it('calls end() on client and nulls it', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        let endCalled = false;
        gl._client = {
            end: async () => { endCalled = true; },
        };
        gl.stop();
        assert.strictEqual(endCalled, true);
        assert.strictEqual(gl._client, null);
    });

    it('nulls client even when no process', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = { end: async () => {} };
        gl.stop();
        assert.strictEqual(gl._client, null);
    });
});

// ─── instance method delegation ────────────────────────────────────────────

describe('instance methods delegate to utils', () => {
    it('docInsert delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const row = { _id: 'abc', data: { name: 'Alice' }, created_at: 'now' };
        gl._client = mockClient({ rows: [row], rowCount: 1 });

        const result = await gl.docInsert('users', { name: 'Alice' });
        assert.deepEqual(result, row);
        // ensureCollection + INSERT = 2 calls
        assert.strictEqual(gl._client._calls.length, 2);
        assert.ok(gl._client._calls[0].text.includes('CREATE TABLE IF NOT EXISTS users'));
        assert.ok(gl._client._calls[1].text.includes('INSERT INTO users'));
    });

    it('search delegates with spread args', async () => {
        const rows = [{ id: 1, _score: 0.8 }];
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows, rowCount: 1 });

        const result = await gl.search('articles', 'body', 'hello');
        assert.deepEqual(result, rows);
        assert.strictEqual(gl._client._calls.length, 1);
        const sql = gl._client._calls[0].text;
        assert.ok(sql.includes('FROM articles'));
        assert.ok(sql.includes("to_tsvector($1, coalesce(body, ''))"));
    });

    it('docFind delegates with spread args', async () => {
        const rows = [{ _id: 'a', data: { x: 1 } }];
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows, rowCount: 1 });

        const result = await gl.docFind('items', { x: 1 });
        assert.deepEqual(result, rows);
        assert.ok(gl._client._calls[0].text.includes('FROM items'));
    });

    it('incr delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [{ value: '5' }], rowCount: 1 });

        const result = await gl.incr('counters', 'page_views', 1);
        assert.strictEqual(result, 5);
    });

    it('hset delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [], rowCount: 0 });

        await gl.hset('cache', 'session1', 'user', { id: 42 });
        // CREATE TABLE + INSERT = 2 calls
        assert.strictEqual(gl._client._calls.length, 2);
        assert.ok(gl._client._calls[1].text.includes('jsonb_build_object'));
    });

    it('zadd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [], rowCount: 0 });

        await gl.zadd('leaderboard', 'player1', 100);
        // CREATE TABLE + INSERT = 2 calls
        assert.strictEqual(gl._client._calls.length, 2);
        assert.ok(gl._client._calls[1].text.includes('INSERT INTO leaderboard'));
    });

    it('publish delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [], rowCount: 0 });

        await gl.publish('events', 'hello');
        assert.strictEqual(gl._client._calls.length, 1);
        assert.ok(gl._client._calls[0].text.includes('pg_notify'));
    });

    it('streamAdd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [{ id: '1' }], rowCount: 1 });

        const id = await gl.streamAdd('events', { type: 'click' });
        assert.strictEqual(id, 1);
    });

    it('percolateAdd delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [], rowCount: 0 });

        await gl.percolateAdd('alerts', 'q1', 'breaking news');
        // CREATE TABLE + CREATE INDEX + INSERT = 3 calls
        assert.strictEqual(gl._client._calls.length, 3);
        assert.ok(gl._client._calls[0].text.includes('CREATE TABLE IF NOT EXISTS alerts'));
    });

    it('analyze delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [{ alias: 'asciiword' }], rowCount: 1 });

        const result = await gl.analyze('hello');
        assert.deepEqual(result, [{ alias: 'asciiword' }]);
    });

    it('countDistinct delegates with spread args', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl._client = mockClient({ rows: [{ cnt: '42' }], rowCount: 1 });

        const result = await gl.countDistinct('users', 'email');
        assert.strictEqual(result, 42);
    });
});

// ─── instance methods throw when not connected ─────────────────────────────

describe('instance methods throw before start()', () => {
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

// ─── all methods exist on the class ────────────────────────────────────────

describe('all instance methods exist', () => {
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
});
