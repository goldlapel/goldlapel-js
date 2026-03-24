import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { NativeCache } from '../cache.js';
import { wrap, CachedClient } from '../wrap.js';

function makeConnectedCache() {
    NativeCache._reset();
    const cache = new NativeCache();
    cache._invalidationConnected = true;
    return cache;
}

function mockClient(queryResult) {
    const calls = [];
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        },
        connect: async () => {},
        end: async () => {},
        on: () => {},
        off: () => {},
        once: () => {},
        _calls: calls,
        someProp: 'test-value',
    };
}

afterEach(() => NativeCache._reset());

// --- wrap() ---

describe('wrap', () => {
    it('returns a proxy-wrapped object', () => {
        const client = mockClient();
        makeConnectedCache();
        const wrapped = wrap(client, 9999);
        assert.ok(wrapped);
    });
});

// --- Cache hit ---

describe('cache hit', () => {
    it('skips real query', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], [{ name: 'id' }]);
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders');
        assert.equal(client._calls.length, 0);
    });

    it('returns cached rows', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1, name: 'widget' }], [{ name: 'id' }]);
        const cached = new CachedClient(client, cache);
        const result = await cached.query('SELECT * FROM orders');
        assert.deepEqual(result.rows, [{ id: 1, name: 'widget' }]);
    });

    it('returns correct rowCount', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }, { id: 2 }], []);
        const cached = new CachedClient(client, cache);
        const result = await cached.query('SELECT * FROM orders');
        assert.equal(result.rowCount, 2);
    });

    it('returns SELECT command', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        const result = await cached.query('SELECT * FROM orders');
        assert.equal(result.command, 'SELECT');
    });
});

// --- Cache miss ---

describe('cache miss', () => {
    it('calls real query', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders');
        assert.equal(client._calls.length, 1);
    });

    it('caches result', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders');
        const entry = cache.get('SELECT * FROM orders', undefined);
        assert.ok(entry);
        assert.deepEqual(entry.rows, [{ id: 1 }]);
    });

    it('subsequent call is cache hit', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders');
        await cached.query('SELECT * FROM orders');
        assert.equal(client._calls.length, 1); // only called once
        assert.equal(cache.statsHits, 1);
    });
});

// --- Writes ---

describe('writes', () => {
    it('invalidates table', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('INSERT INTO orders VALUES (2)');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });

    it('delegates to real client', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('INSERT INTO orders VALUES (2)');
        assert.equal(client._calls.length, 1);
    });

    it('DDL invalidates all', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 0, command: 'CREATE' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ id: 2 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('CREATE TABLE foo (id int)');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
        assert.equal(cache.get('SELECT * FROM users', null), null);
    });
});

// --- Transactions ---

describe('transactions', () => {
    it('BEGIN disables cache', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query('SELECT * FROM orders');
        assert.ok(client._calls.length >= 2); // both went to real
    });

    it('COMMIT re-enables cache', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query('COMMIT');
        client._calls.length = 0;
        await cached.query('SELECT * FROM orders');
        assert.equal(client._calls.length, 0); // cache hit
    });

    it('ROLLBACK re-enables cache', async () => {
        const client = mockClient({ rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query('ROLLBACK');
        client._calls.length = 0;
        await cached.query('SELECT * FROM orders');
        assert.equal(client._calls.length, 0); // cache hit
    });

    it('write in transaction still invalidates', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query('INSERT INTO orders VALUES (2)');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });
});

// --- Query forms ---

describe('query forms', () => {
    it('text + values form', async () => {
        const client = mockClient({ rows: [{ id: 42 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        const r = await cached.query('SELECT * FROM users WHERE id = $1', [42]);
        assert.deepEqual(r.rows, [{ id: 42 }]);
    });

    it('config object form', async () => {
        const client = mockClient({ rows: [{ id: 42 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        const r = await cached.query({ text: 'SELECT * FROM users WHERE id = $1', values: [42] });
        assert.deepEqual(r.rows, [{ id: 42 }]);
    });
});

// --- Proxy forwarding ---

describe('proxy forwarding', () => {
    it('forwards unknown properties', () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const wrapped = wrap(client, 9999);
        assert.equal(wrapped.someProp, 'test-value');
    });

    it('connect delegates', async () => {
        let called = false;
        const client = mockClient();
        client.connect = async () => { called = true; };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.connect();
        assert.ok(called);
    });

    it('end delegates', async () => {
        let called = false;
        const client = mockClient();
        client.end = async () => { called = true; };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.end();
        assert.ok(called);
    });
});

// --- Edge cases ---

describe('edge cases', () => {
    it('query after query resets state', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        cache.put('SELECT 2', null, [{ x: 2 }], []);
        const cached = new CachedClient(client, cache);
        const r1 = await cached.query('SELECT 1');
        assert.deepEqual(r1.rows, [{ x: 1 }]);
        const r2 = await cached.query('SELECT 2');
        assert.deepEqual(r2.rows, [{ x: 2 }]);
    });

    it('write after cache hit clears state', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders');
        await cached.query('INSERT INTO orders VALUES (2)');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });
});
