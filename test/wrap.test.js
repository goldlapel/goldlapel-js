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
