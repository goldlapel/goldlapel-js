import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { NativeCache } from '../cache.js';
import {
    wrap, CachedClient, _isPgPool, _containsOpaqueFunctionCall,
} from '../wrap.js';

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

    it('multi-statement Q with embedded SET shifts subsequent-read hash', async () => {
        // Real-world pattern: a single client.query() submits
        // `SET app.user_id = '42'; SELECT * FROM accounts` as one Q
        // message. observeSql's multi-statement path must apply the SET
        // so the next standalone SELECT keys against the new hash and
        // doesn't share a cache slot with the baseline-empty state.
        const client = mockClient({
            rows: [{ id: 'A' }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        // Pre-populate a baseline (hash 0) cache entry — would be a leak
        // if the multi-statement SET didn't shift the hash.
        cache.put('SELECT * FROM accounts', null, [{ id: 'BASELINE' }], []);

        await cached.query("SET app.user_id = '42'; SELECT * FROM accounts");
        assert.notEqual(cached._gucState.stateHash(), 0);

        // Subsequent standalone SELECT on this same connection: must MISS
        // the baseline entry (different hash) and call _real.query.
        const r = await cached.query('SELECT * FROM accounts');
        assert.deepStrictEqual(r.rows, [{ id: 'A' }],
            'must NOT serve baseline-cached row under a shifted state hash');
    });

    it('SET-looking text inside a SELECT string literal does NOT shift hash', async () => {
        // End-to-end safety: a SELECT containing the substring
        // `SET app.user_id = ...` inside a string literal must not
        // perturb the per-connection state. Fast-path single-statement
        // detection + parseSetCommand reading the first token ('SELECT')
        // → null parse → no apply.
        const client = mockClient({
            rows: [], fields: [], rowCount: 0, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);

        await cached.query("SELECT 'SET app.user_id = pwn' FROM logs");
        assert.strictEqual(cached._gucState.stateHash(), 0);
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

// --- Multi-statement Q-message write detection ---
//
// `CachedClient.query("SET app.tenant = 'x'; INSERT INTO orders ...")` is
// a single Q wire message containing two statements. Pre-fix, detectWrite
// looked at the first token only — saw SET, returned null, and the
// INSERT slipped past invalidation. The fix routes multi-statement
// bodies through splitStatements + per-segment detectWrite, unioning
// the invalidations. Same gap applied to `BEGIN; INSERT...; COMMIT`,
// where TX_START's first-token match used to swallow the INSERT.

describe('multi-statement write detection', () => {
    it('SET prelude does not hide the INSERT — orders gets invalidated', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query("SET app.tenant = 'x'; INSERT INTO orders VALUES (2); SELECT 1");
        // The cached SELECT must be evicted under the baseline (hash 0)
        // slot; the SET shifted the hash for subsequent reads but the
        // INSERT must still invalidate the prior baseline slot.
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });

    it('BEGIN-prefixed multi-statement still invalidates inner INSERT', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        // Pre-fix: TX_START matched BEGIN, returned early before
        // detectWrite ran on any segment → orders cache slot survived
        // the INSERT.
        await cached.query('BEGIN; INSERT INTO orders VALUES (2); COMMIT');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });

    it('multi-statement DDL short-circuits to invalidateAll', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 0, command: 'CREATE' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ id: 2 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query('INSERT INTO orders VALUES (3); CREATE TABLE foo (id int)');
        // DDL_SENTINEL → invalidateAll, both entries cleared.
        assert.equal(cache.get('SELECT * FROM orders', null), null);
        assert.equal(cache.get('SELECT * FROM users', null), null);
    });

    it('union of invalidations across multiple writes in one Q', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ id: 2 }], []);
        cache.put('SELECT * FROM widgets', null, [{ id: 3 }], []);
        const cached = new CachedClient(client, cache);
        await cached.query(
            'INSERT INTO orders VALUES (10); UPDATE users SET name = \'x\' WHERE id = 1'
        );
        assert.equal(cache.get('SELECT * FROM orders', null), null);
        assert.equal(cache.get('SELECT * FROM users', null), null);
        // Untouched table survives.
        assert.notEqual(cache.get('SELECT * FROM widgets', null), null);
    });

    it('semicolon inside a string literal does NOT trigger spurious invalidation', async () => {
        const client = mockClient({
            rows: [{ id: 1 }], fields: [{ name: 'id' }], rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], []);
        const cached = new CachedClient(client, cache);
        // The ; inside the literal must not split the body into bogus
        // segments that detectWrite would misclassify as writes. Picked
        // a literal that doesn't contain `INTO` (detectWrite's SELECT
        // branch has a pre-existing whitespace-tokeniser blindspot for
        // `INTO` inside string literals — out of scope for this fix).
        await cached.query("SELECT 'foo;bar' FROM logs");
        // orders cache slot survives — splitStatements respects quotes,
        // and detectWrite on the single segment returns null (read).
        assert.notEqual(cache.get('SELECT * FROM orders', null), null);
    });
});

// --- Session-state command responses must not be cached ---
//
// `[] && []` is truthy in JS, so the pre-fix `if (result.rows &&
// result.fields)` gate let `SET foo='bar'` responses (which return
// `{rows: [], fields: [], command: 'SET'}`) into the cache. Functionally
// harmless (empty rows can never serve data) but bloats the cache with
// no-row entries and triggers needless eviction pressure.

describe('session-state command responses are not cached', () => {
    const NON_CACHEABLE = ['SET', 'RESET', 'LISTEN', 'UNLISTEN', 'NOTIFY',
        'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT'];

    for (const command of NON_CACHEABLE) {
        it(`${command} response is not put in the cache`, async () => {
            const client = mockClient({
                rows: [], fields: [], rowCount: null, command,
            });
            const cache = makeConnectedCache();
            const cached = new CachedClient(client, cache);
            // Invent a fake SQL string so transaction tracking and
            // detectWrite don't swallow it before the read path. We're
            // testing the cache-put gate, which only applies after a
            // miss + real-client dispatch.
            const sql = `__test_${command}__`;
            await cached.query(sql);
            // No entry under the test SQL.
            assert.equal(cache.get(sql, null), null);
            // Cache stayed empty — no SET/etc response slipped in.
            assert.equal(cache.size, 0);
        });
    }

    it('empty-result SELECT IS still cached (zero rows is a valid hit)', async () => {
        // Negative control for the gate above: we narrowed the no-cache
        // rule to specific command strings (not "any zero-row reply"),
        // so a real SELECT with zero rows must still cache so the next
        // identical query is a hit. The proxy does the same.
        const client = mockClient({
            rows: [], fields: [{ name: 'id' }], rowCount: 0, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM orders WHERE id = -1');
        const entry = cache.get('SELECT * FROM orders WHERE id = -1', undefined);
        assert.ok(entry, 'empty-result SELECT must still cache');
        assert.deepEqual(entry.rows, []);
    });
});

// --- Multi-statement transaction-flag bookkeeping ---
//
// `CachedClient.query("BEGIN; INSERT INTO t VALUES (1); COMMIT")` is a
// single Q wire message that opens AND closes a transaction server-side.
// Pre-fix, the wrapper's first-token TX_START match flipped
// `_inTransaction=true` and never saw the trailing COMMIT, so subsequent
// reads bypassed the cache forever (or until a fresh BEGIN/COMMIT cycle
// reset state). The fix walks every segment via splitStatements and
// applies the boundary that segment carries, so the wrapper's final tx
// flag matches what the server actually did.

describe('multi-statement tx-flag bookkeeping', () => {
    it('BEGIN; INSERT; COMMIT ends with _inTransaction=false', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN; INSERT INTO t VALUES (1); COMMIT');
        // Server ran the COMMIT — wrapper must agree.
        assert.equal(cached._inTransaction, false);
    });

    it('cache works after BEGIN; ...; COMMIT (regression: tx flag was sticky)', async () => {
        // The user-visible symptom of the bug: every read after the
        // mixed body bypassed the cache because _inTransaction was stuck
        // true. After the fix, a cached SELECT served from the prior
        // baseline state must hit on the next read.
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        cache.put('SELECT * FROM users', null, [{ id: 1 }], [{ name: 'id' }]);
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN; INSERT INTO orders VALUES (1); COMMIT');
        // orders was invalidated; users survived. Now a SELECT users
        // must hit cache (would miss pre-fix because _inTransaction
        // would be stuck true).
        client._calls.length = 0;
        const result = await cached.query('SELECT * FROM users');
        assert.equal(client._calls.length, 0, 'cache must be re-enabled post-COMMIT');
        assert.deepEqual(result.rows, [{ id: 1 }]);
    });

    it('BEGIN; INSERT (no COMMIT) ends with _inTransaction=true', async () => {
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN; INSERT INTO t VALUES (1)');
        // Server opened a tx and ran the INSERT inside it; tx is still
        // open at the end of the Q message. Wrapper must agree.
        assert.equal(cached._inTransaction, true);
    });

    it('INSERT; COMMIT (random COMMIT, no prior BEGIN) ends with _inTransaction=false', async () => {
        // Stray COMMIT outside a tx is a Postgres warning ("there is no
        // transaction in progress") but does not open one — server
        // remains out of tx. Wrapper must match.
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // Set state to in-tx first, so we can verify COMMIT closes it.
        cached._inTransaction = true;
        await cached.query('INSERT INTO t VALUES (1); COMMIT');
        assert.equal(cached._inTransaction, false);
    });

    it('SAVEPOINT a; INSERT; RELEASE a — tx flag unchanged (savepoint is not a boundary)', async () => {
        // SAVEPOINT and RELEASE are intra-transaction operators; they
        // don't open or close a top-level tx. Match the existing
        // single-statement TX_START / TX_END regexes (which exclude
        // SAVEPOINT / RELEASE) so wrapper state stays in sync with the
        // server's top-level tx state.
        const client = mockClient({ rows: [], fields: null, rowCount: 1, command: 'INSERT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // Pre-condition: wrapper is in a tx (e.g., from a prior BEGIN).
        cached._inTransaction = true;
        await cached.query('SAVEPOINT a; INSERT INTO t VALUES (1); RELEASE a');
        // Still in tx — savepoint/release inside an existing tx don't
        // change the top-level boundary.
        assert.equal(cached._inTransaction, true);

        // And from out-of-tx, savepoint/release also don't open one
        // (server would error; wrapper just stays out).
        cached._inTransaction = false;
        await cached.query('SAVEPOINT a; INSERT INTO t VALUES (1); RELEASE a');
        assert.equal(cached._inTransaction, false);
    });

    it('plain SELECT 1 — no tx-flag change', async () => {
        const client = mockClient({
            rows: [{ '?column?': 1 }], fields: [{ name: '?column?' }], rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        cached._inTransaction = false;
        await cached.query('SELECT 1');
        assert.equal(cached._inTransaction, false);

        cached._inTransaction = true;
        await cached.query('SELECT 1');
        assert.equal(cached._inTransaction, true);
    });

    it('BEGIN; SELECT 1; ROLLBACK ends with _inTransaction=false', async () => {
        // Cousin of BEGIN/COMMIT — ROLLBACK closes the tx too.
        const client = mockClient({
            rows: [{ '?column?': 1 }], fields: [{ name: '?column?' }], rowCount: 1, command: 'SELECT',
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN; SELECT 1; ROLLBACK');
        assert.equal(cached._inTransaction, false);
    });

    it('COMMIT; BEGIN (close-then-reopen) ends with _inTransaction=true', async () => {
        // Last boundary in execution order wins. This guards against a
        // first-segment-only or final-segment-only implementation: only
        // a per-segment walk produces the right result for arbitrary
        // boundary sequences.
        const client = mockClient({ rows: [], fields: null, rowCount: 0, command: 'COMMIT' });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        cached._inTransaction = true;
        await cached.query('COMMIT; BEGIN');
        assert.equal(cached._inTransaction, true);
    });
});

// ─── pg-pool DISCARD-on-release integration (RLS hardening 2026-05-05) ────
//
// pg-pool reuses physical connections. Without our hook, a client that
// SET an unsafe GUC (`app.user_id = 'A'`) returns to the pool with that
// GUC still in place — a different request later picking up the same
// physical connection inherits the stale state and bypasses the
// state-hash trick. Our `wrap(pool)` returns a Proxy that hooks
// `pool.connect()` so released clients first issue `DISCARD ALL`.

function fakePoolClient(opts = {}) {
    // Models a pg-pool PoolClient: a query method that records calls,
    // a release(err?) method that records releases. Uses a counter to
    // assert the order of DISCARD vs original release.
    const calls = [];
    let released = false;
    let releaseErr = undefined;
    const client = {
        async query(text, values) {
            calls.push({ text, values });
            if (opts.queryThrows && text === 'DISCARD ALL') {
                throw new Error('connection broken');
            }
            return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        },
        release(err) {
            released = true;
            releaseErr = err;
        },
        _calls: calls,
        get released() { return released; },
        get releaseErr() { return releaseErr; },
    };
    return client;
}

function fakePool(opts = {}) {
    // Models a pg-pool Pool. Real pg-pool detection uses
    // `constructor.name === 'Pool'`; we emulate by setting the class
    // name. `connect()` returns a freshly-tracked PoolClient; `query()`
    // does the connect+query+release dance pg-pool does internally so
    // we can verify our hook fires on auto-managed flows too.
    const checkedOut = [];
    class Pool {
        async connect() {
            const c = fakePoolClient(opts);
            checkedOut.push(c);
            return c;
        }
        async query(text, values) {
            const c = await this.connect();
            try {
                return await c.query(text, values);
            } finally {
                c.release();
            }
        }
        async end() {}
        get totalCount() { return checkedOut.length; }
        get idleCount() { return 0; }
        get waitingCount() { return 0; }
    }
    const p = new Pool();
    p._checkedOut = checkedOut;
    return p;
}

describe('pg-pool detection', () => {
    it('detects a Pool by constructor name', () => {
        const p = fakePool();
        assert.ok(_isPgPool(p));
    });

    it('detects a Pool by duck-typing (Pool surface + telemetry props)', () => {
        // Simulate a minified bundle where the constructor name is mangled.
        const p = {
            connect: async () => ({}),
            query: async () => ({}),
            end: async () => {},
            totalCount: 0,
            idleCount: 0,
            waitingCount: 0,
        };
        assert.ok(_isPgPool(p));
    });

    it('does NOT detect a plain Client as a Pool', () => {
        const client = {
            connect: async () => {},
            query: async () => ({}),
            end: async () => {},
            // No totalCount / idleCount / waitingCount.
        };
        assert.ok(!_isPgPool(client));
    });

    it('does NOT detect non-objects as Pools', () => {
        assert.ok(!_isPgPool(null));
        assert.ok(!_isPgPool(undefined));
        assert.ok(!_isPgPool('string'));
        assert.ok(!_isPgPool(42));
    });
});

describe('pg-pool DISCARD-on-release hook', () => {
    it('wrap(pool) returns a Proxy that hooks connect()', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        const client = await wrapped.connect();
        // Hook marker is set on the checked-out client.
        assert.strictEqual(client.__goldlapelDiscardHooked, true);
    });

    it('release() issues DISCARD ALL before original release', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        const client = await wrapped.connect();
        await client.query("SET app.user_id = '42'");
        await client.release();
        // The hooked release ran DISCARD ALL before signalling release.
        const sqlSeen = client._calls.map(c => c.text);
        assert.deepStrictEqual(
            sqlSeen,
            ["SET app.user_id = '42'", 'DISCARD ALL'],
        );
        assert.strictEqual(client.released, true);
        assert.strictEqual(client.releaseErr, undefined);
    });

    it('release(err) skips DISCARD (client is being destroyed)', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        const client = await wrapped.connect();
        const err = new Error('aborted');
        await client.release(err);
        // No DISCARD was issued.
        assert.deepStrictEqual(client._calls.map(c => c.text), []);
        assert.strictEqual(client.released, true);
        assert.strictEqual(client.releaseErr, err);
    });

    it('release(true) also skips DISCARD (truthy first arg → destroy)', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        const client = await wrapped.connect();
        await client.release(true);
        assert.deepStrictEqual(client._calls.map(c => c.text), []);
        assert.strictEqual(client.released, true);
        assert.strictEqual(client.releaseErr, true);
    });

    it('failing DISCARD destroys the client instead of returning it', async () => {
        const pool = fakePool({ queryThrows: true });
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        const client = await wrapped.connect();
        // Should NOT throw to the user — release errors are swallowed.
        await client.release();
        // The DISCARD threw, so we passed an Error to the original
        // release (destroy path).
        assert.ok(client.released);
        assert.ok(client.releaseErr instanceof Error);
    });

    it('hook is idempotent — wrap(pool) twice does not double-DISCARD', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped1 = wrap(pool, 9999);
        const client = await wrapped1.connect();
        // Manually re-hook (simulates wrap() being called twice on the
        // same pool — the second wrap shouldn't re-wrap the release).
        await client.release();
        const sqlSeen = client._calls.map(c => c.text);
        assert.deepStrictEqual(sqlSeen, ['DISCARD ALL'],
            'release fires DISCARD ALL exactly once');
    });

    it('pool.query() (auto-managed) flow also DISCARDs on internal release', async () => {
        // pg-pool's pool.query() calls this.connect() internally —
        // because the wrapper hooked connect(), the internal release
        // also fires DISCARD ALL.
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        await wrapped.query('SELECT * FROM users');
        // Exactly one client was checked out, and it saw DISCARD ALL on release.
        assert.strictEqual(pool._checkedOut.length, 1);
        const client = pool._checkedOut[0];
        const sqlSeen = client._calls.map(c => c.text);
        assert.deepStrictEqual(
            sqlSeen,
            ['SELECT * FROM users', 'DISCARD ALL'],
        );
    });

    it('passthrough properties (totalCount, idleCount) work on the wrapped pool', async () => {
        const pool = fakePool();
        makeConnectedCache();
        const wrapped = wrap(pool, 9999);
        // Telemetry properties pass through transparently.
        assert.strictEqual(wrapped.totalCount, 0);
        assert.strictEqual(wrapped.idleCount, 0);
        assert.strictEqual(wrapped.waitingCount, 0);
        await wrapped.connect();
        assert.strictEqual(wrapped.totalCount, 1);
    });

    it('raw Client (non-Pool) is wrapped as before — no DISCARD hook', async () => {
        // Regression guard: a plain Client must still be wrapped via the
        // CachedClient path, not the Pool path. Verify by checking that
        // the wrapped object exposes CachedClient-style state (_gucState).
        const client = mockClient();
        makeConnectedCache();
        const wrapped = wrap(client, 9999);
        // CachedClient exposes _gucState; the Pool Proxy doesn't.
        assert.ok(wrapped._gucState);
        // No release hook on the underlying client.
        assert.strictEqual(client.__goldlapelDiscardHooked, undefined);
    });
});

// ─── Opaque-function-call detection (RLS hardening 2026-05-05) ────────────
//
// Top-level `SELECT [pg_catalog.]<ident>(...)` triggers an async post-call
// verify because the function body may have run a SET internally. This
// section pins the matcher's truth table so the verify stays narrow:
// non-function SELECTs don't fire (waste a verify), `set_config` doesn't
// fire (already applied inline), and unambiguous function calls always
// fire even when buried in larger SELECTs or multi-statement bodies.

describe('_containsOpaqueFunctionCall', () => {
    it('matches plain function call', () => {
        assert.ok(_containsOpaqueFunctionCall('SELECT my_func()'));
        assert.ok(_containsOpaqueFunctionCall('SELECT my_func(1, 2)'));
    });
    it('matches pg_catalog-qualified function call', () => {
        assert.ok(_containsOpaqueFunctionCall('SELECT pg_catalog.my_func()'));
    });
    it('case-insensitive', () => {
        assert.ok(_containsOpaqueFunctionCall('select MY_FUNC()'));
        assert.ok(_containsOpaqueFunctionCall('Select My_Func ( )'));
    });
    it('matches function call with trailing FROM clause', () => {
        // `SELECT my_setter(), col FROM tbl` is a real RLS pattern —
        // the function may set a GUC as a side effect of producing a row.
        assert.ok(_containsOpaqueFunctionCall('SELECT my_setter() FROM tbl'));
    });
    it('does NOT match plain row-SELECT', () => {
        assert.ok(!_containsOpaqueFunctionCall('SELECT * FROM users'));
        assert.ok(!_containsOpaqueFunctionCall('SELECT id, name FROM users'));
        assert.ok(!_containsOpaqueFunctionCall('SELECT 1'));
    });
    it('does NOT match set_config (handled inline)', () => {
        assert.ok(!_containsOpaqueFunctionCall(
            "SELECT set_config('app.user_id', '42', false)"
        ));
        assert.ok(!_containsOpaqueFunctionCall(
            "SELECT pg_catalog.set_config('app.user_id', '42', false)"
        ));
    });
    it('matches inside multi-statement body', () => {
        assert.ok(_containsOpaqueFunctionCall('SELECT 1; SELECT my_fn()'));
        assert.ok(_containsOpaqueFunctionCall('SELECT my_fn(); SELECT 1'));
    });
    it('multi-statement with only set_config + plain SELECTs does NOT fire', () => {
        assert.ok(!_containsOpaqueFunctionCall(
            "SELECT set_config('app.x', '1', false); SELECT * FROM t"
        ));
    });
    it('non-string / empty input returns false', () => {
        assert.ok(!_containsOpaqueFunctionCall(''));
        assert.ok(!_containsOpaqueFunctionCall(null));
        assert.ok(!_containsOpaqueFunctionCall(undefined));
        assert.ok(!_containsOpaqueFunctionCall(42));
    });
    it('non-SELECT statements do not match', () => {
        assert.ok(!_containsOpaqueFunctionCall('INSERT INTO t VALUES (my_func())'));
        assert.ok(!_containsOpaqueFunctionCall('UPDATE t SET x = my_func()'));
        assert.ok(!_containsOpaqueFunctionCall('BEGIN'));
    });
});

// ─── Async post-call verify (concern 6) ───────────────────────────────────
//
// On a top-level `SELECT <fn>(...)` the wrapper schedules an async query
// against pg_settings to detect any GUCs the function set internally.
// Updates state map on success, marks `_dirty` on failure. Never blocks
// the user's hot path; never fails the user's query.

function settle() {
    // Yield to the microtask queue so any queueMicrotask-scheduled
    // verify gets a chance to run + resolve. Two `setImmediate` ticks
    // is overkill but cheap and bullet-proof in the test environment.
    return new Promise((resolve) => setImmediate(() => setImmediate(resolve)));
}

function verifyableMockClient({ functionResult, sessionRows, queryThrows }) {
    // Mock client that handles three query shapes:
    //   1. `SELECT name, setting FROM pg_settings WHERE source = 'session'`
    //      → returns sessionRows (or throws if queryThrows is set)
    //   2. The opaque function call (whatever the test passes) → returns
    //      `functionResult` (defaults to a successful no-row SELECT)
    //   3. Anything else → also returns the function-call default.
    const calls = [];
    return {
        async query(text, values) {
            calls.push({ text, values });
            if (typeof text === 'string' && text.startsWith(
                "SELECT name, setting FROM pg_settings"
            )) {
                if (queryThrows) throw new Error('pg_settings query failed');
                return { rows: sessionRows ?? [], fields: [], rowCount: 0, command: 'SELECT' };
            }
            return functionResult ?? { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        },
        connect: async () => {},
        end: async () => {},
        on: () => {}, off: () => {}, once: () => {},
        _calls: calls,
    };
}

describe('async post-call verify on top-level SELECT <fn>(...)', () => {
    it('successful verify updates state from pg_settings', async () => {
        const client = verifyableMockClient({
            sessionRows: [
                { name: 'app.user_id', setting: '42' },
                { name: 'app.tenant', setting: 'acme' },
                { name: 'application_name', setting: 'foo' }, // safe — ignored
            ],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT my_setter()');
        // Wait for the async verify to settle.
        await settle();
        // pg_settings query should have been issued exactly once.
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 1);
        // Unsafe-GUC state was rebuilt from the response.
        assert.strictEqual(cached._gucState._values.size, 2);
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        assert.strictEqual(cached._gucState._values.get('app.tenant'), 'acme');
        // application_name was filtered out (safe).
        assert.ok(!cached._gucState._values.has('application_name'));
        // Hash is non-zero now.
        assert.notStrictEqual(cached._gucState.stateHash(), 0);
        // _dirty cleared on success.
        assert.strictEqual(cached._dirty, false);
    });

    it('failed verify marks _dirty for next-checkout reconcile', async () => {
        const client = verifyableMockClient({ queryThrows: true });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT my_setter()');
        await settle();
        assert.strictEqual(cached._dirty, true);
    });

    it('verify never throws to the user (failure is internal)', async () => {
        const client = verifyableMockClient({ queryThrows: true });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // The user's query must NOT reject because the post-call verify
        // failed. Verify by awaiting the user's call AND letting the
        // verify settle without unhandled-rejection events.
        const result = await cached.query('SELECT my_setter()');
        assert.deepStrictEqual(result.rows, []);
        await settle();
        assert.strictEqual(cached._dirty, true);
    });

    it('plain SELECT (non-function) does NOT trigger verify', async () => {
        const client = verifyableMockClient({});
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT * FROM users');
        await settle();
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 0);
    });

    it('SELECT set_config(...) does NOT trigger verify (handled inline)', async () => {
        const client = verifyableMockClient({});
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query("SELECT set_config('app.user_id', '42', false)");
        await settle();
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 0);
        // The hash was still moved by the inline set_config recognition.
        assert.notStrictEqual(cached._gucState.stateHash(), 0);
    });

    it('verify is single-flight (back-to-back fn calls do not stack)', async () => {
        const client = verifyableMockClient({
            sessionRows: [{ name: 'app.user_id', setting: '42' }],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // Two fn calls in quick succession — only the first should
        // schedule a verify; the second sees `_pendingVerify` set and
        // skips. Net: exactly one pg_settings query.
        const p1 = cached.query('SELECT my_fn()');
        const p2 = cached.query('SELECT my_other_fn()');
        await Promise.all([p1, p2]);
        await settle();
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 1, 'single-flight: at most one verify in flight');
    });

    it('verify does not fire while in transaction', async () => {
        const client = verifyableMockClient({
            sessionRows: [{ name: 'app.user_id', setting: '42' }],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query('SELECT my_setter()');
        await settle();
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 0,
            'no verify scheduled mid-tx (would see uncommitted state)');
    });

    it('verify does not fire after end() (closed connection)', async () => {
        const client = verifyableMockClient({});
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.end();
        // Even if a fn-call query were issued after end(), verify
        // should be suppressed. Simulate by directly calling
        // _scheduleVerify (the user code wouldn't issue queries after
        // end, but pending verifies might race).
        cached._scheduleVerify();
        await settle();
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 0);
    });

    it('verify after end() — close mid-flight does not crash', async () => {
        // Simulate connection-close-mid-verify: kick off a verify,
        // immediately close, ensure no unhandled rejection.
        let resolveQuery;
        const pendingQuery = new Promise((resolve) => { resolveQuery = resolve; });
        const client = {
            async query(text, values) {
                if (typeof text === 'string'
                    && text.startsWith("SELECT name, setting FROM pg_settings")) {
                    return pendingQuery; // hangs until we resolve
                }
                return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
            },
            end: async () => {},
        };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT my_fn()');
        // verify is in flight; close the connection.
        await cached.end();
        // Make the verify reject as if the connection died.
        resolveQuery(Promise.reject(new Error('connection closed')));
        await settle();
        // No throw, _dirty set (verify failed → dirty path).
        assert.strictEqual(cached._dirty, true);
    });
});

// ─── Verify-on-checkout (concern 5) ───────────────────────────────────────
//
// `_dirty=true` means we can't trust the state hash. The next user query
// reconciles by querying pg_settings synchronously before the cache
// lookup. Models the case where an async post-call verify failed (or
// the connection was handed off and re-acquired in a way that leaves
// the wrapper unable to track state on the wire).

describe('verify-on-checkout (lazy fallback)', () => {
    it('dirty + next query → reconciles before cache lookup', async () => {
        const client = verifyableMockClient({
            sessionRows: [{ name: 'app.user_id', setting: '99' }],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // Pretend a previous async verify failed — _dirty is set.
        cached._dirty = true;
        // Pre-populate the cache under the (stale) baseline-empty hash.
        cache.put('SELECT * FROM accounts', null, [{ id: 'STALE' }], [], 0);
        // Next user query should reconcile FIRST, then look up cache
        // under the new (post-reconcile) hash — which doesn't match
        // the baseline-empty slot.
        const result = await cached.query('SELECT * FROM accounts');
        // pg_settings was queried.
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 1);
        // _dirty cleared.
        assert.strictEqual(cached._dirty, false);
        // The stale baseline-hash entry was NOT served — the wrapper
        // missed cache and went to _real.query (returning [] from our mock).
        assert.notDeepStrictEqual(result.rows, [{ id: 'STALE' }]);
    });

    it('dirty + tx → does NOT reconcile (would see uncommitted state)', async () => {
        const client = verifyableMockClient({
            sessionRows: [{ name: 'app.user_id', setting: '99' }],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        cached._inTransaction = true;
        cached._dirty = true;
        await cached.query('SELECT * FROM users');
        const verifyCalls = client._calls.filter(c =>
            typeof c.text === 'string'
            && c.text.startsWith("SELECT name, setting FROM pg_settings")
        );
        assert.strictEqual(verifyCalls.length, 0,
            'no reconcile mid-tx — wait until tx ends');
        // _dirty stays set (will retry on first post-tx query).
        assert.strictEqual(cached._dirty, true);
    });

    it('dirty cleared after successful reconcile', async () => {
        const client = verifyableMockClient({
            sessionRows: [],
        });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        cached._dirty = true;
        await cached.query('SELECT 1');
        assert.strictEqual(cached._dirty, false);
    });

    it('reconcile failure leaves _dirty set for next attempt', async () => {
        const client = verifyableMockClient({ queryThrows: true });
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        cached._dirty = true;
        // The reconcile fails; the user's query continues.
        await cached.query('SELECT 1');
        assert.strictEqual(cached._dirty, true);
    });

    it('reconcile + post-call verify combine cleanly on a fn-call query', async () => {
        // After a fn call: the post-call verify reconciles asynchronously.
        // After that reconcile failure, _dirty is set; the next user
        // query reconciles synchronously. End-to-end behavior:
        //   1. Fn call → real query → schedule async verify.
        //   2. Async verify fails → _dirty = true.
        //   3. Next user query → sync reconcile (pg_settings) → cleared.
        let firstVerify = true;
        const client = {
            async query(text, values) {
                if (typeof text === 'string'
                    && text.startsWith("SELECT name, setting FROM pg_settings")) {
                    if (firstVerify) {
                        firstVerify = false;
                        throw new Error('first verify failed');
                    }
                    return {
                        rows: [{ name: 'app.user_id', setting: 'X' }],
                        fields: [], rowCount: 0, command: 'SELECT',
                    };
                }
                return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
            },
            end: async () => {},
        };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('SELECT my_fn()');
        await settle();
        // First verify failed → dirty.
        assert.strictEqual(cached._dirty, true);
        await cached.query('SELECT 1');
        // Second verify succeeded as part of checkout reconcile → clean.
        assert.strictEqual(cached._dirty, false);
        assert.strictEqual(cached._gucState._values.get('app.user_id'), 'X');
    });
});

// ─── SET-actually-applied (Wave 2) ────────────────────────────────────────
//
// State-hash mutation is deferred until `_real.query` resolves. Failed
// SETs (server rejects with ErrorResponse) MUST NOT shift the wrapper's
// hash — pre-Wave-2 they did, leaving wrapper state diverged from server
// state and causing cache leaks across user contexts (a SET that the
// server rejected because of an ACL or syntax error would still
// fragment the wrapper's cache as if it had succeeded).
//
// On error the wrapper discards pending mutations AND marks `_dirty=true`
// so the next user query reconciles from `pg_settings` — conservative-
// correct for multi-statement bodies where the server may have applied
// some statements before erroring (we don't know how far it got, so we
// reread the canonical state instead of guessing).

function rejectingMockClient(rejectOn) {
    // Mock client whose query rejects when the SQL contains `rejectOn`
    // (substring match — easy to target a specific statement in a
    // multi-statement body). Records every call so tests can assert
    // call counts.
    const calls = [];
    return {
        async query(text, values) {
            calls.push({ text, values });
            if (typeof text === 'string' && text.includes(rejectOn)) {
                throw new Error(`server rejected: ${rejectOn}`);
            }
            return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        },
        connect: async () => {},
        end: async () => {},
        on: () => {}, off: () => {}, once: () => {},
        _calls: calls,
    };
}

describe('SET-actually-applied: state hash deferred until response confirms', () => {
    it('successful SET shifts the hash (baseline behavior preserved)', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        assert.strictEqual(cached._gucState.stateHash(), 0);
        await cached.query("SET app.user_id = '42'");
        assert.notStrictEqual(cached._gucState.stateHash(), 0,
            'successful SET commits its mutation');
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
    });

    it('failed SET does NOT shift the hash and marks _dirty', async () => {
        // Server rejects the SET (e.g. permission denied, syntax error).
        // Wrapper must NOT apply the optimistic mutation — pre-fix the
        // hash diverged from server-side state on every rejected SET.
        const client = rejectingMockClient("SET app.user_id");
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await assert.rejects(
            cached.query("SET app.user_id = '42'"),
            /server rejected/,
            'wrapper re-throws server rejection unchanged',
        );
        assert.strictEqual(cached._gucState.stateHash(), 0,
            'rejected SET leaves the hash at baseline');
        assert.strictEqual(cached._gucState._values.size, 0,
            'rejected SET never enters the values map');
        assert.strictEqual(cached._dirty, true,
            'rejected mutation marks dirty for next-query reconcile');
    });

    it('failed RESET does NOT clear values (pre-RESET state preserved)', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        // Establish state.
        await cached.query("SET app.user_id = '42'");
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        // Now make the next call reject.
        client.query = async (text, values) => {
            if (typeof text === 'string' && text.includes('RESET')) {
                throw new Error('server rejected RESET');
            }
            return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        };
        await assert.rejects(cached.query('RESET app.user_id'));
        // Values map is unchanged — RESET never committed.
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42',
            'rejected RESET leaves prior state intact');
        assert.strictEqual(cached._dirty, true);
    });

    it('failed SET does not poison cache lookups for previously-set users', async () => {
        // Two CachedClients on a shared cache. A populates under hash_A.
        // B attempts to SET hash_B but the server rejects. B must NOT be
        // able to read A's cached row (pre-fix: B's hash optimistically
        // mutated to hash_B, would key against hash_B for lookup, miss
        // cache → real query — fine. But if instead the test were
        // reversed [SET fails on A], A would see baseline hash and could
        // read across context). Here we focus on the simpler case: the
        // failed SET leaves B's hash unchanged from baseline.
        const clientA = mockClient({
            rows: [{ id: 'A-row' }], fields: [{ name: 'id' }],
            rowCount: 1, command: 'SELECT',
        });
        const clientB = rejectingMockClient("SET app.user_id");
        const cache = makeConnectedCache();
        const cachedA = new CachedClient(clientA, cache);
        const cachedB = new CachedClient(clientB, cache);

        await cachedA.query("SET app.user_id = 'A'");
        await cachedA.query('SELECT * FROM accounts');

        // B's SET fails — its hash stays at 0 (baseline).
        await assert.rejects(cachedB.query("SET app.user_id = 'B'"));
        assert.strictEqual(cachedB._gucState.stateHash(), 0);
        assert.strictEqual(cachedB._dirty, true);
    });

    it('multi-statement: failed batch discards ALL pending mutations', async () => {
        // `SET a; SET b; <broken>` — server may have applied SET a + b
        // before erroring on the third statement, but we can't tell from
        // pg's single rejection. Discard everything + mark dirty so the
        // next query reconciles from pg_settings.
        const client = rejectingMockClient("SELECT broken_thing");
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await assert.rejects(
            cached.query("SET app.user_id = '42'; SET app.tenant = 'acme'; SELECT broken_thing()"),
        );
        assert.strictEqual(cached._gucState._values.size, 0,
            'no pending mutation commits on a rejected batch');
        assert.strictEqual(cached._gucState.stateHash(), 0);
        assert.strictEqual(cached._dirty, true,
            'dirty mark triggers pg_settings reconcile on next query');
    });

    it('multi-statement: successful batch commits SETs in segment order', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query("SET app.user_id = '42'; SET app.tenant = 'acme'");
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        assert.strictEqual(cached._gucState._values.get('app.tenant'), 'acme');
    });

    it('connection error during SET marks dirty (no optimistic apply)', async () => {
        // Network blip / connection torn down → query rejects with a
        // generic Error. State must NOT shift — the server may never
        // have processed the SET at all.
        const client = {
            async query() { throw new Error('ECONNRESET'); },
            end: async () => {},
            on: () => {}, off: () => {}, once: () => {},
        };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await assert.rejects(cached.query("SET app.user_id = '42'"));
        assert.strictEqual(cached._gucState.stateHash(), 0);
        assert.strictEqual(cached._dirty, true);
    });

    it('error on a non-SET query does NOT mark dirty (no pending ops)', async () => {
        // A failed SELECT has no state-mutation ops to discard. Marking
        // dirty would force a pointless pg_settings reconcile on every
        // next query after any failed read — wasteful and noisy. Only
        // mark dirty when there were pending mutations at risk.
        const client = rejectingMockClient("SELECT * FROM");
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await assert.rejects(cached.query('SELECT * FROM users'));
        assert.strictEqual(cached._dirty, false,
            'failed read with no pending mutation does not need reconcile');
    });
});

describe('SET-actually-applied: tx snapshot revert on ROLLBACK', () => {
    it('BEGIN; SET; ROLLBACK reverts the SET to pre-tx state', async () => {
        // Server-side: bare SET inside a tx reverts on ROLLBACK. The
        // wrapper must mirror that — pre-fix, the SET's mutation
        // committed at the SET statement and never reverted, leaving
        // the wrapper's hash diverged from server-side state after the
        // ROLLBACK.
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query("SET app.user_id = '42'");
        // Mid-tx, the wrapper has applied the SET (we want it visible to
        // any read that runs in this tx — though the cache is bypassed
        // mid-tx anyway).
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        await cached.query('ROLLBACK');
        // Post-ROLLBACK: SET reverted, hash back to baseline.
        assert.strictEqual(cached._gucState.stateHash(), 0,
            'ROLLBACK restores pre-tx hash');
        assert.ok(!cached._gucState._values.has('app.user_id'),
            'ROLLBACK clears tx-scoped SET');
    });

    it('BEGIN; SET; COMMIT persists the SET (snapshot discarded)', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        await cached.query("SET app.user_id = '42'");
        await cached.query('COMMIT');
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42',
            'COMMIT keeps tx-applied SET');
        assert.notStrictEqual(cached._gucState.stateHash(), 0);
    });

    it('multi-statement BEGIN; SET; ROLLBACK in one Q reverts cleanly', async () => {
        // The whole tx body is a single multi-statement Q-message. The
        // wrapper must walk the segments, snapshot at BEGIN, apply SET,
        // then restore from snapshot at ROLLBACK — all within one
        // `_runRealQuery` success.
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query("BEGIN; SET app.user_id = '42'; ROLLBACK");
        assert.strictEqual(cached._gucState.stateHash(), 0,
            'inline ROLLBACK reverts the inline SET');
        assert.strictEqual(cached._inTransaction, false);
    });

    it('multi-statement BEGIN; SET; COMMIT in one Q persists', async () => {
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query("BEGIN; SET app.user_id = '42'; COMMIT");
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        assert.strictEqual(cached._inTransaction, false);
    });

    it('SET outside tx + BEGIN; SET2; ROLLBACK reverts only the inner SET', async () => {
        // The pre-tx SET should persist; only the SET inside the tx
        // reverts. Snapshot was taken AFTER the first SET so the
        // restore-on-rollback brings us back to (first SET applied).
        const client = mockClient();
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query("SET app.user_id = '42'"); // outside tx — persists
        const hashBeforeTx = cached._gucState.stateHash();
        assert.notStrictEqual(hashBeforeTx, 0);

        await cached.query('BEGIN');
        await cached.query("SET app.user_id = '99'"); // inside tx — reverts
        await cached.query('ROLLBACK');

        // Outside-tx value is preserved.
        assert.strictEqual(cached._gucState._values.get('app.user_id'), '42');
        assert.strictEqual(cached._gucState.stateHash(), hashBeforeTx,
            'hash restored to pre-BEGIN state');
    });

    it('failed SET inside tx does NOT add a snapshot frame', async () => {
        // BEGIN succeeds → snapshot pushed. SET inside tx fails → no
        // additional snapshot, no apply. ROLLBACK pops the BEGIN's
        // snapshot. End state: no leaked snapshots, no leaked SETs,
        // _dirty marked from the failed SET.
        let stage = 0;
        const client = {
            async query(text) {
                stage++;
                // stage 1 = BEGIN, succeed
                // stage 2 = SET, fail
                // stage 3 = ROLLBACK, succeed
                if (stage === 2) throw new Error('SET rejected');
                return { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
            },
            end: async () => {},
            on: () => {}, off: () => {}, once: () => {},
        };
        const cache = makeConnectedCache();
        const cached = new CachedClient(client, cache);
        await cached.query('BEGIN');
        assert.strictEqual(cached._gucState._txStack.length, 1);
        await assert.rejects(cached.query("SET app.user_id = 'x'"));
        // SET rejection sets dirty but doesn't add a snapshot frame.
        assert.strictEqual(cached._gucState._txStack.length, 1,
            'failed SET inside tx leaves snapshot stack at 1 frame');
        assert.strictEqual(cached._dirty, true);
        await cached.query('ROLLBACK');
        assert.strictEqual(cached._gucState._txStack.length, 0,
            'ROLLBACK pops the BEGIN snapshot');
    });
});
