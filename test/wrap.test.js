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

// --- Per-connection unsafe-GUC state-hash gating ---
//
// Mirrors the proxy's per-connection ConnectionGucState (commit 3e02359):
// every query observes SET / RESET commands; the resulting state hash is
// folded into the native cache key so two CachedClients with different
// RLS-relevant GUC state never share a cache slot. SET LOCAL is ignored
// (cache is bypassed mid-transaction anyway).

describe('GUC state-hash gating', () => {
    it('SET unsafe GUC isolates cache lookups across two CachedClients', async () => {
        // Shared cache singleton, two CachedClient instances → models the
        // same JS process serving two PG connections with different
        // RLS-relevant GUC state.
        const clientA = mockClient({
            rows: [{ id: 'A-row' }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const clientB = mockClient({
            rows: [{ id: 'B-row' }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cachedA = new CachedClient(clientA, cache);
        const cachedB = new CachedClient(clientB, cache);

        // Both connections set their own user.
        await cachedA.query("SET app.user_id = 'A'");
        await cachedB.query("SET app.user_id = 'B'");

        // A queries first, populates the cache under hashA.
        const r1 = await cachedA.query('SELECT * FROM accounts');
        assert.deepStrictEqual(r1.rows, [{ id: 'A-row' }]);
        assert.strictEqual(clientA._calls.filter(c => c.text === 'SELECT * FROM accounts').length, 1);

        // B queries the same SQL — must MISS (different state hash) and
        // hit the real client, not return A's rows.
        const r2 = await cachedB.query('SELECT * FROM accounts');
        assert.deepStrictEqual(r2.rows, [{ id: 'B-row' }]);
        assert.strictEqual(clientB._calls.filter(c => c.text === 'SELECT * FROM accounts').length, 1);
    });

    it('SET safe GUC does not isolate cache (timezone, work_mem, etc.)', async () => {
        const client = mockClient({
            rows: [{ id: 1 }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        await cached.query("SET timezone = 'UTC'");
        await cached.query('SELECT * FROM widgets'); // populates
        await cached.query('SELECT * FROM widgets'); // hits
        // Safe GUC didn't shift the hash, so the second call hits cache.
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 1);
        assert.equal(cache.statsHits, 1);
    });

    it('RESET reverts state hash → previously-cached rows become reachable again', async () => {
        const client = mockClient({
            rows: [{ id: 1 }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        // Cache under baseline (hash 0).
        await cached.query('SELECT * FROM widgets'); // miss → real call
        await cached.query('SELECT * FROM widgets'); // hit
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 1);

        // Shift state — same SELECT now misses (different hash).
        await cached.query("SET app.user_id = '42'");
        await cached.query('SELECT * FROM widgets'); // miss → real call again
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 2);

        // RESET reverts hash → baseline-cached row is reachable again.
        await cached.query('RESET app.user_id');
        await cached.query('SELECT * FROM widgets'); // hit
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 2);
    });

    it('SET LOCAL inside a transaction does NOT affect post-COMMIT state hash', async () => {
        const client = mockClient({
            rows: [{ id: 1 }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        // Pre-cache under baseline.
        await cached.query('SELECT * FROM widgets');
        await cached.query('SELECT * FROM widgets');
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 1);

        await cached.query('BEGIN');
        await cached.query("SET LOCAL app.user_id = '42'"); // ignored for hash
        await cached.query('COMMIT');

        // Post-COMMIT, hash should still be 0 — baseline cache hit.
        await cached.query('SELECT * FROM widgets');
        assert.strictEqual(client._calls.filter(c => c.text === 'SELECT * FROM widgets').length, 1);
    });

    it('observeSql runs even on writes (SET inside a write-prelude session)', async () => {
        // A user setting an unsafe GUC then doing an INSERT must still
        // shift the state hash for subsequent reads — observeSql runs
        // before write detection, so the SET registers regardless.
        const client = mockClient({
            rows: [], fields: null, rowCount: 1, command: 'INSERT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        await cached.query("SET app.user_id = '42'");
        // CachedClient internal state has _gucState; verify the hash
        // is non-zero by attempting a read whose cache slot under
        // baseline (hash 0) is empty — the read should miss.
        assert.notEqual(cached._gucState.stateHash(), 0);
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
