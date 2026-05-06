import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'net';
import {
    NativeCache, makeKey, detectWrite, extractTables,
    DDL_SENTINEL, TX_START, TX_END,
    EVICT_RATE_WINDOW, EVICT_RATE_HIGH, EVICT_RATE_LOW,
    isUnsafeGuc, parseSetCommand, splitStatements, ConnectionGucState,
    KNOWN_SAFE_GUCS,
} from '../cache.js';

function makeCache(opts = {}) {
    NativeCache._reset();
    if (opts.maxEntries) process.env.GOLDLAPEL_NATIVE_CACHE_SIZE = String(opts.maxEntries);
    if (opts.enabled === false) process.env.GOLDLAPEL_NATIVE_CACHE = 'false';
    if (opts.reportStats === false) process.env.GOLDLAPEL_REPORT_STATS = 'false';
    const cache = new NativeCache();
    if (opts.connected !== false) cache._invalidationConnected = true;
    delete process.env.GOLDLAPEL_NATIVE_CACHE_SIZE;
    delete process.env.GOLDLAPEL_NATIVE_CACHE;
    delete process.env.GOLDLAPEL_REPORT_STATS;
    return cache;
}

afterEach(() => NativeCache._reset());

// --- makeKey ---

describe('makeKey', () => {
    // Cache key shape (post-Option-Y, 2026-05-04):
    //   `<state-hash-hex>\0<sql>\0<json-encoded-values>`
    // The hex prefix is `0` for the default empty-state baseline so two
    // CachedClients that never SET an unsafe GUC still share cache slots.
    it('null values', () => {
        assert.equal(makeKey('SELECT 1', null), '0\0SELECT 1\0null');
    });

    it('array values', () => {
        assert.equal(makeKey('SELECT $1', [42]), '0\0SELECT $1\0[42]');
    });

    it('undefined values treated as null', () => {
        assert.equal(makeKey('SELECT 1', undefined), '0\0SELECT 1\0null');
    });

    it('different params produce different keys', () => {
        assert.notEqual(makeKey('SELECT $1', [1]), makeKey('SELECT $1', [2]));
    });

    it('same sql same params produce same key', () => {
        assert.equal(makeKey('SELECT $1', [42]), makeKey('SELECT $1', [42]));
    });
});

// --- detectWrite ---

describe('detectWrite', () => {
    it('INSERT', () => assert.equal(detectWrite('INSERT INTO orders VALUES (1)'), 'orders'));
    it('INSERT with schema', () => assert.equal(detectWrite('INSERT INTO public.orders VALUES (1)'), 'orders'));
    it('UPDATE', () => assert.equal(detectWrite('UPDATE orders SET name = \'x\''), 'orders'));
    it('DELETE', () => assert.equal(detectWrite('DELETE FROM orders WHERE id = 1'), 'orders'));
    it('TRUNCATE', () => assert.equal(detectWrite('TRUNCATE orders'), 'orders'));
    it('TRUNCATE TABLE', () => assert.equal(detectWrite('TRUNCATE TABLE orders'), 'orders'));
    it('CREATE DDL', () => assert.equal(detectWrite('CREATE TABLE foo (id int)'), DDL_SENTINEL));
    it('ALTER DDL', () => assert.equal(detectWrite('ALTER TABLE foo ADD COLUMN bar int'), DDL_SENTINEL));
    it('DROP DDL', () => assert.equal(detectWrite('DROP TABLE foo'), DDL_SENTINEL));
    it('SELECT returns null', () => assert.equal(detectWrite('SELECT * FROM orders'), null));
    it('case insensitive', () => assert.equal(detectWrite('insert INTO Orders VALUES (1)'), 'orders'));
    it('COPY FROM', () => assert.equal(detectWrite("COPY orders FROM '/tmp/data.csv'"), 'orders'));
    it('COPY TO returns null', () => assert.equal(detectWrite("COPY orders TO '/tmp/data.csv'"), null));
    it('COPY subquery returns null', () => assert.equal(detectWrite("COPY (SELECT * FROM orders) TO '/tmp/data.csv'"), null));
    it('WITH CTE INSERT', () => assert.equal(detectWrite('WITH x AS (SELECT 1) INSERT INTO foo SELECT * FROM x'), DDL_SENTINEL));
    it('WITH CTE SELECT', () => assert.equal(detectWrite('WITH x AS (SELECT 1) SELECT * FROM x'), null));
    it('empty returns null', () => assert.equal(detectWrite(''), null));
    it('whitespace returns null', () => assert.equal(detectWrite('   '), null));
    it('COPY with columns', () => assert.equal(detectWrite("COPY orders(id, name) FROM '/tmp/data.csv'"), 'orders'));

    // Regression: SELECT with `INTO` inside a string literal must not be
    // classified as SELECT-INTO DDL. The pre-fix tokenizer split on
    // whitespace only, so a literal like `'INSERT INTO orders'` smuggled
    // the bare word INTO into the scan and tripped the DDL sentinel,
    // forcing a full cache invalidation on a plain read.
    it('SELECT with INTO in single-quoted literal is not DDL', () => {
        assert.equal(detectWrite("SELECT 'INSERT INTO orders;' FROM audit_log"), null);
    });
    it('SELECT with INTO in double-quoted identifier is not DDL', () => {
        assert.equal(detectWrite('SELECT * FROM "into_table"'), null);
    });
    it('SELECT with INTO in LIKE-pattern literal is not DDL', () => {
        assert.equal(detectWrite("SELECT message FROM logs WHERE message LIKE '%INTO%'"), null);
    });
    it('SELECT with INTO inside doubled-quote escape is not DDL', () => {
        assert.equal(detectWrite("SELECT 'It''s INTO trouble' FROM notes"), null);
    });
    it('real SELECT INTO new_table is still DDL (regression guard)', () => {
        assert.equal(detectWrite('SELECT * INTO new_table FROM source'), DDL_SENTINEL);
    });
    it('real SELECT INTO TEMP table is still DDL', () => {
        assert.equal(detectWrite('SELECT id INTO TEMP scratch FROM source'), DDL_SENTINEL);
    });
});

// --- extractTables ---

describe('extractTables', () => {
    it('simple FROM', () => {
        const t = extractTables('SELECT * FROM orders');
        assert.ok(t.has('orders'));
    });

    it('JOIN', () => {
        const t = extractTables('SELECT * FROM orders o JOIN customers c ON o.cid = c.id');
        assert.ok(t.has('orders'));
        assert.ok(t.has('customers'));
    });

    it('schema qualified', () => {
        const t = extractTables('SELECT * FROM public.orders');
        assert.ok(t.has('orders'));
    });

    it('multiple joins', () => {
        const t = extractTables('SELECT * FROM orders JOIN items ON 1=1 JOIN products ON 1=1');
        assert.equal(t.size, 3);
    });

    it('case insensitive', () => {
        const t = extractTables('SELECT * FROM ORDERS');
        assert.ok(t.has('orders'));
    });

    it('no tables', () => {
        assert.equal(extractTables('SELECT 1').size, 0);
    });

    it('subquery', () => {
        const t = extractTables('SELECT * FROM orders WHERE id IN (SELECT oid FROM users)');
        assert.ok(t.has('orders'));
        assert.ok(t.has('users'));
    });
});

// --- Transaction detection ---

describe('transaction detection', () => {
    it('BEGIN', () => assert.ok(TX_START.test('BEGIN')));
    it('START TRANSACTION', () => assert.ok(TX_START.test('START TRANSACTION')));
    it('COMMIT', () => assert.ok(TX_END.test('COMMIT')));
    it('ROLLBACK', () => assert.ok(TX_END.test('ROLLBACK')));
    it('END', () => assert.ok(TX_END.test('END')));
    it('SAVEPOINT not start', () => assert.ok(!TX_START.test('SAVEPOINT x')));
    it('SET TRANSACTION not start', () => assert.ok(!TX_START.test('SET TRANSACTION ISOLATION LEVEL')));
    it('SELECT not start', () => assert.ok(!TX_START.test('SELECT 1')));
});

// --- Cache operations ---

describe('cache operations', () => {
    it('put and get', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM users', null, [{ id: 1 }], [{ name: 'id' }]);
        const entry = cache.get('SELECT * FROM users', null);
        assert.ok(entry);
        assert.deepEqual(entry.rows, [{ id: 1 }]);
    });

    it('miss returns null', () => {
        const cache = makeCache();
        assert.equal(cache.get('SELECT 1', null), null);
    });

    it('disabled returns null', () => {
        const cache = makeCache({ enabled: false });
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        assert.equal(cache.get('SELECT 1', null), null);
    });

    it('not connected returns null', () => {
        const cache = makeCache({ connected: false });
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        assert.equal(cache.get('SELECT 1', null), null);
    });

    it('params differentiate keys', () => {
        const cache = makeCache();
        cache.put('SELECT $1', [1], [{ id: 1 }], []);
        cache.put('SELECT $1', [2], [{ id: 2 }], []);
        assert.deepEqual(cache.get('SELECT $1', [1]).rows, [{ id: 1 }]);
        assert.deepEqual(cache.get('SELECT $1', [2]).rows, [{ id: 2 }]);
    });

    it('stats tracking', () => {
        const cache = makeCache();
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        cache.get('SELECT 1', null);
        cache.get('SELECT 2', null);
        assert.equal(cache.statsHits, 1);
        assert.equal(cache.statsMisses, 1);
    });
});

// --- LRU ---

describe('LRU eviction', () => {
    it('evicts at capacity', () => {
        const cache = makeCache({ maxEntries: 3 });
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        cache.put('SELECT 2', null, [{ x: 2 }], []);
        cache.put('SELECT 3', null, [{ x: 3 }], []);
        cache.put('SELECT 4', null, [{ x: 4 }], []);
        assert.equal(cache.get('SELECT 1', null), null);
        assert.ok(cache.get('SELECT 4', null));
    });

    it('access refreshes LRU', () => {
        const cache = makeCache({ maxEntries: 3 });
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        cache.put('SELECT 2', null, [{ x: 2 }], []);
        cache.put('SELECT 3', null, [{ x: 3 }], []);
        cache.get('SELECT 1', null); // refresh 1
        cache.put('SELECT 4', null, [{ x: 4 }], []); // evicts 2
        assert.ok(cache.get('SELECT 1', null));
        assert.equal(cache.get('SELECT 2', null), null);
    });

    it('eviction cleans table index', () => {
        const cache = makeCache({ maxEntries: 2 });
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ x: 2 }], []);
        cache.put('SELECT * FROM products', null, [{ x: 3 }], []);
        const ordersKeys = cache._tableIndex.get('orders');
        assert.ok(!ordersKeys || ordersKeys.size === 0);
    });
});

// --- Invalidation ---

describe('invalidation', () => {
    it('invalidate table', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ x: 2 }], []);
        cache.invalidateTable('orders');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
        assert.ok(cache.get('SELECT * FROM users', null));
    });

    it('invalidate all', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache.put('SELECT * FROM users', null, [{ x: 2 }], []);
        cache.invalidateAll();
        assert.equal(cache.get('SELECT * FROM orders', null), null);
        assert.equal(cache.get('SELECT * FROM users', null), null);
    });

    it('cross-referenced cleanup', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders JOIN users ON 1=1', null, [{ x: 1 }], []);
        cache.invalidateTable('orders');
        assert.equal(cache.get('SELECT * FROM orders JOIN users ON 1=1', null), null);
        const usersKeys = cache._tableIndex.get('users');
        assert.ok(!usersKeys || usersKeys.size === 0);
    });

    it('invalidation stats', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache.invalidateTable('orders');
        assert.equal(cache.statsInvalidations, 1);
    });
});

// --- Signal processing ---

describe('signal processing', () => {
    it('table signal', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache._processSignal('I:orders');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });

    it('wildcard signal', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache._processSignal('I:*');
        assert.equal(cache.get('SELECT * FROM orders', null), null);
    });

    it('keepalive preserves cache', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache._processSignal('P:');
        assert.ok(cache.get('SELECT * FROM orders', null));
    });

    it('unknown signal preserves cache', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);
        cache._processSignal('X:something');
        assert.ok(cache.get('SELECT * FROM orders', null));
    });
});

// --- Push invalidation via socket ---

describe('push invalidation', () => {
    it('remote signal clears cache', async () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);

        const server = createServer();
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;

        cache._invalidationConnected = false;
        cache.connectInvalidation(port);

        const conn = await new Promise(resolve => server.once('connection', resolve));
        await new Promise(resolve => setTimeout(resolve, 100));

        assert.ok(cache.connected);
        conn.write('I:orders\n');
        await new Promise(resolve => setTimeout(resolve, 100));

        assert.equal(cache.get('SELECT * FROM orders', null), null);

        conn.destroy();
        server.close();
        cache.stopInvalidation();
    });

    it('connection drop clears cache', async () => {
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ x: 1 }], []);

        const server = createServer();
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;

        cache._invalidationConnected = false;
        cache.connectInvalidation(port);

        const conn = await new Promise(resolve => server.once('connection', resolve));
        await new Promise(resolve => setTimeout(resolve, 100));
        assert.ok(cache.connected);

        conn.destroy();
        await new Promise(resolve => setTimeout(resolve, 100));

        assert.ok(!cache.connected);
        assert.equal(cache.size, 0);

        server.close();
        cache.stopInvalidation();
    });
});

// --- Concurrent access ---
//
// Node's event loop runs JS on a single thread, so there is no classical data
// race inside a single synchronous put()/get()/invalidateTable() call. But the
// push-invalidation socket's 'data' callback fires asynchronously and will
// interleave with any user async flow that awaits between cache operations.
// This test exercises that interleaving: ~100 put()s and ~100 get()s racing
// in parallel via Promise.all, plus ~10 invalidation signals delivered via
// the real socket path, all yielding to each other on every iteration.
// Mirrors Python's TestThreadSafety and Go's TestConcurrent{PutAndGet,Invalidation}.

describe('concurrent put/get/invalidate', () => {
    it('no exceptions, stats consistent, size bounded', async () => {
        const OPS = 100;
        const TABLES = 10; // t0..t9
        const MAX = 50;    // below OPS*2 so eviction races with invalidation
        const cache = makeCache({ maxEntries: MAX });

        // Stand up a real invalidation server so socket signals drive real
        // invalidation callbacks (the only genuine async source here).
        const server = createServer();
        await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;

        cache._invalidationConnected = false;
        cache.connectInvalidation(port);
        const conn = await new Promise(resolve => server.once('connection', resolve));
        // Wait for the client-side 'connect' event to flip the flag.
        await new Promise(resolve => setTimeout(resolve, 50));
        assert.ok(cache.connected, 'socket should be connected before racing');

        const yieldTick = () => new Promise(resolve => setImmediate(resolve));
        const sqlFor = i => `SELECT * FROM t${i % TABLES} WHERE id = ${i}`;

        // Writers: OPS puts, each yielding so socket data can interleave.
        const writer = async (start) => {
            for (let i = start; i < start + OPS; i++) {
                cache.put(sqlFor(i), [i], [{ id: i }], [{ name: 'id' }]);
                await yieldTick();
            }
        };

        // Readers: OPS gets, counting local hits/misses to cross-check stats.
        let localHits = 0;
        let localMisses = 0;
        const reader = async (start) => {
            for (let i = start; i < start + OPS; i++) {
                const r = cache.get(sqlFor(i), [i]);
                if (r) localHits++; else localMisses++;
                await yieldTick();
            }
        };

        // Invalidator: drives invalidation via the real socket listener path.
        const invalidator = async () => {
            for (let i = 0; i < TABLES; i++) {
                conn.write(`I:t${i}\n`);
                // Give the data handler a tick to drain.
                await new Promise(resolve => setTimeout(resolve, 2));
            }
        };

        await assert.doesNotReject(Promise.all([
            writer(0),
            writer(OPS),
            reader(0),
            reader(OPS),
            invalidator(),
        ]));

        // Drain any straggler socket data before asserting stats.
        await new Promise(resolve => setTimeout(resolve, 50));

        // Stats invariants: every get() call bumped exactly one counter.
        assert.equal(
            cache.statsHits + cache.statsMisses,
            OPS * 2,
            'hits + misses must equal total get() calls',
        );
        assert.equal(cache.statsHits, localHits, 'cache.statsHits matches observed hits');
        assert.equal(cache.statsMisses, localMisses, 'cache.statsMisses matches observed misses');

        // Size invariant: never exceeds configured max.
        assert.ok(cache.size <= MAX, `cache.size=${cache.size} exceeds max ${MAX}`);

        // statsInvalidations must equal the number of keys actually removed
        // by invalidation — never negative, never exceed total puts.
        assert.ok(cache.statsInvalidations >= 0);
        assert.ok(
            cache.statsInvalidations <= OPS * 2,
            `statsInvalidations=${cache.statsInvalidations} exceeds total puts`,
        );

        // Table index invariant: every key it references must still be in _cache.
        for (const [table, keys] of cache._tableIndex) {
            assert.ok(keys.size > 0, `empty key set left for table ${table}`);
            for (const key of keys) {
                assert.ok(
                    cache._cache.has(key),
                    `tableIndex[${table}] references missing cache key`,
                );
            }
        }

        conn.destroy();
        server.close();
        cache.stopInvalidation();
    });
});

// ─── L1 telemetry: counters + snapshot ─────────────────────────────────────

describe('evictions counter', () => {
    it('starts at zero', () => {
        const cache = makeCache({ maxEntries: 4 });
        assert.equal(cache.statsEvictions, 0);
    });

    it('bumps on overflow', () => {
        const cache = makeCache({ maxEntries: 4 });
        for (let i = 0; i < 8; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        // 8 puts, capacity 4 → 4 evictions.
        assert.equal(cache.statsEvictions, 4);
    });

    it('no bump within capacity', () => {
        const cache = makeCache({ maxEntries: 8 });
        for (let i = 0; i < 4; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        assert.equal(cache.statsEvictions, 0);
    });
});

describe('snapshot shape', () => {
    it('carries required fields', () => {
        const cache = makeCache({ maxEntries: 64 });
        cache.put('SELECT 1', null, [{ x: 1 }], []);
        cache.get('SELECT 1', null);
        cache.get('SELECT MISS', null);
        const snap = cache._buildSnapshot();
        assert.equal(snap.wrapper_id, cache._wrapperId);
        assert.equal(snap.lang, 'javascript');
        assert.ok('version' in snap);
        assert.equal(snap.hits, 1);
        assert.equal(snap.misses, 1);
        assert.equal(snap.evictions, 0);
        assert.equal(snap.invalidations, 0);
        assert.equal(snap.current_size_entries, 1);
        assert.equal(snap.capacity_entries, 64);
    });

    it('wrapper_id is a UUID v4', () => {
        const cache = makeCache();
        // RFC 4122 UUID v4 format: 8-4-4-4-12 hex with version nibble = 4
        // and variant nibble in {8,9,a,b}.
        const re = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
        assert.match(cache._wrapperId, re);
    });

    it('wrapper_id stable across snapshots', () => {
        const cache = makeCache();
        const a = cache._buildSnapshot().wrapper_id;
        const b = cache._buildSnapshot().wrapper_id;
        assert.equal(a, b);
    });
});

// ─── L1 telemetry: emit pipeline (unit, no socket) ─────────────────────────

describe('emit pipeline', () => {
    it('process_request snapshot emits R: line', () => {
        const cache = makeCache();
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._processRequest('snapshot');
        const rLines = emissions.filter(l => l.startsWith('R:'));
        assert.equal(rLines.length, 1);
        const payload = JSON.parse(rLines[0].slice(2));
        assert.equal(payload.wrapper_id, cache._wrapperId);
        assert.equal(payload.lang, 'javascript');
        assert.ok(typeof payload.ts_ms === 'number');
    });

    it('process_request empty body treated as snapshot', () => {
        const cache = makeCache();
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._processRequest('');
        const rLines = emissions.filter(l => l.startsWith('R:'));
        assert.equal(rLines.length, 1);
    });

    it('process_request unknown body silently dropped', () => {
        const cache = makeCache();
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._processRequest('future_request_type');
        const rLines = emissions.filter(l => l.startsWith('R:'));
        assert.equal(rLines.length, 0);
    });

    it('process_signal routes ?:snapshot to response', () => {
        const cache = makeCache();
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._processSignal('?:snapshot');
        assert.equal(emissions.filter(l => l.startsWith('R:')).length, 1);
    });

    it('emit_state_change adds state field', () => {
        const cache = makeCache();
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._emitStateChange('wrapper_connected');
        const sLines = emissions.filter(l => l.startsWith('S:'));
        assert.equal(sLines.length, 1);
        const payload = JSON.parse(sLines[0].slice(2));
        assert.equal(payload.state, 'wrapper_connected');
        assert.equal(payload.wrapper_id, cache._wrapperId);
    });

    it('report_stats=false suppresses emissions', () => {
        const cache = makeCache({ reportStats: false });
        assert.equal(cache._reportStats, false);
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        cache._emitStateChange('wrapper_connected');
        cache._emitResponse();
        cache._processRequest('snapshot');
        // _sendLine isn't even reached, but the spy here demonstrates
        // the upstream emit functions short-circuit before calling it.
        // _sendLine itself also short-circuits (covered separately).
        assert.equal(emissions.length, 0);
    });
});

// ─── L1 telemetry: eviction-rate state changes ─────────────────────────────

describe('eviction-rate state change', () => {
    it('cache_full fires when evictions dominate', () => {
        // Capacity 4 — every put past the 4th evicts. Once we cross
        // the warmup window the rate is ~99% and cache_full fires.
        const cache = makeCache({ maxEntries: 4 });
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        for (let i = 0; i < EVICT_RATE_WINDOW + 10; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        const fullLines = emissions.filter(l => l.includes('cache_full'));
        assert.ok(fullLines.length >= 1, `expected at least one cache_full, got ${emissions.length} emissions`);
    });

    it('cache_full does not fire below window', () => {
        // Warmup gate — fewer puts than the window means no signal yet.
        const cache = makeCache({ maxEntries: 2 });
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        for (let i = 0; i < EVICT_RATE_WINDOW - 1; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        assert.ok(!emissions.some(l => l.includes('cache_full')));
    });

    it('cache_full fires only once per high-rate run (latched)', () => {
        // Once cache_full has fired, sustained-high doesn't re-emit.
        // Hysteresis keeps the dashboard from drowning in repeats.
        const cache = makeCache({ maxEntries: 4 });
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        // Big run to make sure we cross the window threshold and stay
        // above HIGH for a long time.
        for (let i = 0; i < EVICT_RATE_WINDOW * 3; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        const fullLines = emissions.filter(l => l.includes('cache_full'));
        assert.equal(fullLines.length, 1);
    });

    it('cache_recovered fires when rate drops back below LOW', () => {
        // First saturate to flip the latch. Then put lots of duplicate
        // keys (re-puts don't evict) to dilute the eviction rate below
        // EVICT_RATE_LOW and trigger cache_recovered.
        const cache = makeCache({ maxEntries: 4 });
        const emissions = [];
        cache._sendLine = (line) => emissions.push(line);
        // Phase 1: distinct keys → high eviction rate → cache_full.
        for (let i = 0; i < EVICT_RATE_WINDOW + 10; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        assert.ok(emissions.some(l => l.includes('cache_full')));
        // Phase 2: re-put existing keys (well, re-put the same SELECT
        // 0..3 which are still in the cache after phase 1 saturation).
        // Actually the cache is full of the latest 4 entries, so re-put
        // those.
        emissions.length = 0;
        for (let i = 0; i < EVICT_RATE_WINDOW + 10; i++) {
            // Pick keys that round-trip in the small capacity; cycling
            // through a *small* set under capacity means no evictions.
            const k = i % 4;
            // Need keys that are currently in the cache — phase 1 left
            // the LAST 4 puts in. Keep re-putting those exact keys.
            const idx = (EVICT_RATE_WINDOW + 6 + k);
            cache.put(`SELECT ${idx}`, null, [{ x: idx }], []);
        }
        // After enough no-eviction puts, the window is dominated by 0s.
        assert.ok(emissions.some(l => l.includes('cache_recovered')),
            `expected cache_recovered emission, got ${emissions.map(e => e.slice(0, 40))}`);
    });
});

// ─── L1 telemetry: integration via real socket ─────────────────────────────

function _spawnServer() {
    return new Promise((resolve) => {
        const server = createServer();
        server.listen(0, '127.0.0.1', () => {
            resolve({ server, port: server.address().port });
        });
    });
}

function _attachReader(conn) {
    // Returns a `{ lines, waitFor, stop }` triple. `lines` is the live
    // accumulator, `waitFor(predicate, timeout)` polls until the
    // predicate is true (or rejects), `stop()` tears the reader down.
    const lines = [];
    let buf = '';
    const onData = (chunk) => {
        buf += chunk.toString('utf8');
        let idx;
        while ((idx = buf.indexOf('\n')) !== -1) {
            lines.push(buf.slice(0, idx));
            buf = buf.slice(idx + 1);
        }
    };
    conn.on('data', onData);
    const waitFor = (predicate, timeout = 2000) => new Promise((resolve, reject) => {
        const deadline = Date.now() + timeout;
        const tick = () => {
            if (predicate(lines)) return resolve();
            if (Date.now() >= deadline) {
                return reject(new Error(`timeout; lines=${JSON.stringify(lines)}`));
            }
            setTimeout(tick, 20);
        };
        tick();
    });
    const stop = () => {
        conn.off('data', onData);
        try { conn.destroy(); } catch {}
    };
    return { lines, waitFor, stop };
}

describe('state-change emission via socket', () => {
    it('wrapper_connected emitted on socket connect', async () => {
        const cache = makeCache();
        cache._invalidationConnected = false; // start unconnected
        const { server, port } = await _spawnServer();
        try {
            cache.connectInvalidation(port);
            const conn = await new Promise(resolve => server.once('connection', resolve));
            const reader = _attachReader(conn);
            try {
                await reader.waitFor(lines => lines.some(l => l.startsWith('S:')));
                const sLines = reader.lines.filter(l => l.startsWith('S:'));
                assert.ok(sLines.length >= 1);
                const payload = JSON.parse(sLines[0].slice(2));
                assert.equal(payload.state, 'wrapper_connected');
                assert.equal(payload.wrapper_id, cache._wrapperId);
                assert.equal(payload.lang, 'javascript');
            } finally {
                reader.stop();
            }
        } finally {
            cache.stopInvalidation();
            server.close();
        }
    });

    it('snapshot request returns R: response', async () => {
        const cache = makeCache();
        cache._invalidationConnected = false;
        // Pre-warm some counters so the response is non-trivial.
        const { server, port } = await _spawnServer();
        try {
            cache.connectInvalidation(port);
            const conn = await new Promise(resolve => server.once('connection', resolve));
            const reader = _attachReader(conn);
            try {
                // Wait for wrapper_connected so we know the socket is
                // wired both ways.
                await reader.waitFor(lines => lines.some(l => l.startsWith('S:')));
                cache.put('SELECT 1', null, [{ x: 1 }], []);
                cache.get('SELECT 1', null);
                conn.write('?:snapshot\n');
                await reader.waitFor(lines => lines.some(l => l.startsWith('R:')));
                const rLines = reader.lines.filter(l => l.startsWith('R:'));
                assert.ok(rLines.length >= 1);
                const payload = JSON.parse(rLines[0].slice(2));
                assert.equal(payload.wrapper_id, cache._wrapperId);
                assert.equal(payload.hits, 1);
                assert.equal(payload.current_size_entries, 1);
                // R: payloads do NOT carry a state field — that's S:'s
                // job. The proxy's parser uses prefix to discriminate.
                assert.equal(payload.state, undefined);
            } finally {
                reader.stop();
            }
        } finally {
            cache.stopInvalidation();
            server.close();
        }
    });

    it('cache_full emitted to socket after sustained eviction', async () => {
        const cache = makeCache({ maxEntries: 4 });
        cache._invalidationConnected = false;
        const { server, port } = await _spawnServer();
        try {
            cache.connectInvalidation(port);
            const conn = await new Promise(resolve => server.once('connection', resolve));
            const reader = _attachReader(conn);
            try {
                await reader.waitFor(lines => lines.some(l => l.startsWith('S:')));
                // Saturate the eviction window. Each put past capacity
                // bumps the eviction sample to 1. After a full window
                // of 1s we cross HIGH and emit cache_full.
                for (let i = 0; i < EVICT_RATE_WINDOW + 10; i++) {
                    cache.put(`SELECT ${i}`, null, [{ x: i }], []);
                }
                await reader.waitFor(
                    lines => lines.some(l => l.startsWith('S:') && l.includes('cache_full')),
                    3000,
                );
                const fullLine = reader.lines.find(
                    l => l.startsWith('S:') && l.includes('cache_full'),
                );
                const payload = JSON.parse(fullLine.slice(2));
                assert.equal(payload.state, 'cache_full');
                // First HIGH crossing happens as soon as the window
                // fills with mostly-evicting puts. With capacity=4 and
                // window=200 the first 4 puts don't evict, the next
                // 196 do — so we expect ~196 evictions when the latch
                // first flips. Lower bound is loose because the exact
                // emit point can vary if the threshold check ever
                // becomes async. Just assert "lots happened".
                assert.ok(payload.evictions >= EVICT_RATE_WINDOW / 2,
                    `expected meaningful eviction count, got ${payload.evictions}`);
            } finally {
                reader.stop();
            }
        } finally {
            cache.stopInvalidation();
            server.close();
        }
    });

    it('report_stats=false suppresses S: and R: lines on socket', async () => {
        const cache = makeCache({ reportStats: false });
        cache._invalidationConnected = false;
        assert.equal(cache._reportStats, false);
        const { server, port } = await _spawnServer();
        try {
            cache.connectInvalidation(port);
            const conn = await new Promise(resolve => server.once('connection', resolve));
            const reader = _attachReader(conn);
            try {
                // Give it a beat, then probe with a snapshot request.
                await new Promise(resolve => setTimeout(resolve, 100));
                conn.write('?:snapshot\n');
                await new Promise(resolve => setTimeout(resolve, 200));
                const noisy = reader.lines.filter(
                    l => l.startsWith('S:') || l.startsWith('R:'),
                );
                assert.deepEqual(noisy, []);
            } finally {
                reader.stop();
            }
        } finally {
            cache.stopInvalidation();
            server.close();
        }
    });

    it('unknown proxy prefixes silently ignored', () => {
        // Forward-compat: future proxies might extend the protocol.
        // The wrapper must not crash on unknown prefixes.
        const cache = makeCache();
        // Should not throw.
        cache._processSignal('Z:future-prefix');
        cache._processSignal('$:bogus');
        cache._processSignal('');
    });
});

// ─── disableNativeCache — explicit native-cache off without losing size ───
//
// `disableNativeCache: true` flips the cache into a no-op pass-through:
//   * get() never returns an entry; the misses counter ticks (so the
//     dashboard still sees real read traffic flowing through the wrapper)
//   * put() is silently dropped — no eviction, no table-index update
//   * snapshot adds `disabled: true` so the proxy can render "native cache
//     off" instead of misreading hits=0 as a cold cache
// Default (omitted or `false`) preserves the existing fully-functional
// behavior. The toggle is also late-bindable: re-instantiating with
// `{ disabled: ... }` against the existing singleton flips the bit.

describe('disableNativeCache', () => {
    it('default (disableNativeCache omitted) caches normally', () => {
        const cache = makeCache();
        assert.equal(cache._disabled, false);
        cache.put('SELECT * FROM users', null, [{ id: 1 }], [{ name: 'id' }]);
        const entry = cache.get('SELECT * FROM users', null);
        assert.ok(entry);
        assert.equal(cache.statsHits, 1);
        assert.equal(cache.statsMisses, 0);
    });

    it('disabled: get returns null even after a put', () => {
        NativeCache._reset();
        const cache = new NativeCache({ disabled: true });
        cache._invalidationConnected = true;
        cache.put('SELECT * FROM users', null, [{ id: 1 }], [{ name: 'id' }]);
        assert.equal(cache.get('SELECT * FROM users', null), null);
    });

    it('disabled: put is a silent no-op — cache stays empty', () => {
        NativeCache._reset();
        const cache = new NativeCache({ disabled: true });
        cache._invalidationConnected = true;
        cache.put('SELECT * FROM users', null, [{ id: 1 }], []);
        cache.put('SELECT * FROM orders', null, [{ id: 2 }], []);
        assert.equal(cache.size, 0);
        assert.equal(cache._tableIndex.size, 0);
    });

    it('disabled: misses tick, hits stay 0, evictions stay 0', () => {
        NativeCache._reset();
        const cache = new NativeCache({ disabled: true });
        cache._invalidationConnected = true;
        // Pretend to populate, then read repeatedly. Every get is a miss.
        for (let i = 0; i < 50; i++) {
            cache.put(`SELECT ${i}`, null, [{ x: i }], []);
        }
        for (let i = 0; i < 50; i++) {
            assert.equal(cache.get(`SELECT ${i}`, null), null);
        }
        assert.equal(cache.statsHits, 0);
        assert.equal(cache.statsMisses, 50);
        assert.equal(cache.statsEvictions, 0);
    });

    it('snapshot includes disabled: true when disabled', () => {
        NativeCache._reset();
        const cache = new NativeCache({ disabled: true });
        cache._invalidationConnected = true;
        cache.get('SELECT 1', null); // bumps misses
        const snap = cache._buildSnapshot();
        assert.equal(snap.disabled, true);
        assert.equal(snap.misses, 1);
        assert.equal(snap.hits, 0);
    });

    it('snapshot omits disabled when enabled (default)', () => {
        const cache = makeCache();
        const snap = cache._buildSnapshot();
        assert.equal(Object.prototype.hasOwnProperty.call(snap, 'disabled'), false);
    });

    it('re-instantiating against the singleton flips the bit', () => {
        // Models the wrap()-then-start() ordering: a wrap() call lazily
        // creates the cache singleton, then start({ disableNativeCache: true })
        // applies the flag.
        NativeCache._reset();
        const a = new NativeCache(); // first construction, default
        a._invalidationConnected = true;
        assert.equal(a._disabled, false);

        const b = new NativeCache({ disabled: true });
        assert.strictEqual(a, b, 'same singleton instance');
        assert.equal(a._disabled, true);

        // And the flip is reversible.
        const c = new NativeCache({ disabled: false });
        assert.strictEqual(a, c);
        assert.equal(a._disabled, false);
    });

    it('omitting disabled on re-entry leaves singleton state untouched', () => {
        NativeCache._reset();
        const a = new NativeCache({ disabled: true });
        a._invalidationConnected = true;
        // No `disabled` key in opts — must not reset to default false.
        const b = new NativeCache({});
        assert.strictEqual(a, b);
        assert.equal(a._disabled, true);
    });
});

// ─── Unsafe-GUC classifier (Option Y wrapper-side) ─────────────────────────
//
// Mirrors `is_unsafe_guc` in proxy `src/guc_state.rs`. A GUC is unsafe if
// it's in the short hardcoded list (search_path, role, …) OR contains a
// `.` (namespaced — `app.*`, `myapp.*`). Comparison is case-insensitive.

describe('isUnsafeGuc', () => {
    it('short-list members are unsafe', () => {
        assert.ok(isUnsafeGuc('search_path'));
        assert.ok(isUnsafeGuc('role'));
        assert.ok(isUnsafeGuc('session_authorization'));
        assert.ok(isUnsafeGuc('default_transaction_isolation'));
        assert.ok(isUnsafeGuc('default_transaction_read_only'));
        assert.ok(isUnsafeGuc('transaction_isolation'));
        assert.ok(isUnsafeGuc('row_security'));
    });

    it('classification is case-insensitive', () => {
        assert.ok(isUnsafeGuc('ROLE'));
        assert.ok(isUnsafeGuc('Search_Path'));
        assert.ok(isUnsafeGuc('SEARCH_PATH'));
    });

    it('namespaced GUCs are unsafe', () => {
        assert.ok(isUnsafeGuc('app.user_id'));
        assert.ok(isUnsafeGuc('myapp.tenant'));
        assert.ok(isUnsafeGuc('rls.account'));
        assert.ok(isUnsafeGuc('a.b.c'));
        assert.ok(isUnsafeGuc('APP.USER'));
    });

    it('safe GUCs are safe', () => {
        assert.ok(!isUnsafeGuc('timezone'));
        assert.ok(!isUnsafeGuc('application_name'));
        assert.ok(!isUnsafeGuc('statement_timeout'));
        assert.ok(!isUnsafeGuc('work_mem'));
        assert.ok(!isUnsafeGuc('client_encoding'));
        assert.ok(!isUnsafeGuc('DateStyle'));
    });

    it('non-strings and empty strings are safe (defensive)', () => {
        assert.ok(!isUnsafeGuc(''));
        assert.ok(!isUnsafeGuc(null));
        assert.ok(!isUnsafeGuc(undefined));
        assert.ok(!isUnsafeGuc(42));
    });
});

// ─── parseSetCommand: shape coverage ──────────────────────────────────────

describe('parseSetCommand', () => {
    it('SET name = value (quoted)', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET foo = 'bar'"),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
    });

    it('SET name TO value (quoted)', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET foo TO 'bar'"),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
    });

    it('SET name = unquoted-value', () => {
        assert.deepStrictEqual(
            parseSetCommand('SET foo = 42'),
            { kind: 'set', name: 'foo', value: '42' },
        );
    });

    it('SET SESSION modifier strips to plain set', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET SESSION foo = 'bar'"),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
    });

    it('SET LOCAL modifier yields set_local kind', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET LOCAL foo = 'bar'"),
            { kind: 'set_local', name: 'foo', value: 'bar' },
        );
    });

    it('RESET name', () => {
        assert.deepStrictEqual(
            parseSetCommand('RESET foo'),
            { kind: 'reset', name: 'foo' },
        );
    });

    it('RESET ALL', () => {
        assert.deepStrictEqual(parseSetCommand('RESET ALL'), { kind: 'reset_all' });
    });

    it('case insensitive on keywords', () => {
        assert.deepStrictEqual(
            parseSetCommand("set foo = 'bar'"),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
        assert.deepStrictEqual(
            parseSetCommand("Set Local foo To 'bar'"),
            { kind: 'set_local', name: 'foo', value: 'bar' },
        );
        assert.deepStrictEqual(parseSetCommand('reset all'), { kind: 'reset_all' });
    });

    it('lowercases the GUC name', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET App.User_ID = '42'"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('tolerates a single trailing semicolon', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET foo = 'bar';"),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
        assert.deepStrictEqual(
            parseSetCommand('RESET foo ;'),
            { kind: 'reset', name: 'foo' },
        );
    });

    it("SET name='value' with no spaces (glued =)", () => {
        assert.deepStrictEqual(
            parseSetCommand("SET app.user_id='42'"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('strips surrounding double quotes from name', () => {
        assert.deepStrictEqual(
            parseSetCommand('SET "App.User" = 42'),
            { kind: 'set', name: 'app.user', value: '42' },
        );
    });

    it('strips surrounding single quotes from value', () => {
        assert.deepStrictEqual(
            parseSetCommand("SET foo = 'multi word value'"),
            { kind: 'set', name: 'foo', value: 'multi word value' },
        );
    });

    it('non-SET / non-RESET returns null', () => {
        assert.strictEqual(parseSetCommand('SELECT 1'), null);
        assert.strictEqual(parseSetCommand('INSERT INTO t VALUES (1)'), null);
        assert.strictEqual(parseSetCommand('BEGIN'), null);
        assert.strictEqual(parseSetCommand(''), null);
        assert.strictEqual(parseSetCommand('   '), null);
    });

    it('RESET with extra junk is rejected', () => {
        assert.strictEqual(parseSetCommand('RESET foo bar'), null);
    });

    it('non-string input returns null', () => {
        assert.strictEqual(parseSetCommand(null), null);
        assert.strictEqual(parseSetCommand(undefined), null);
        assert.strictEqual(parseSetCommand(42), null);
    });

    // Parity with proxy/src/guc_state.rs `parse_rejects_set_time_zone_two_word_form`:
    // the legacy `SET TIME ZONE 'UTC'` form is harmless (timezone is safe)
    // and the unusual two-word GUC name doesn't fit the parser shape. Returning
    // null is correct — it tells callers "this isn't a trackable SET" so the
    // state hash isn't perturbed.
    it("legacy 'SET TIME ZONE' two-word form returns null", () => {
        assert.strictEqual(parseSetCommand("SET TIME ZONE 'UTC'"), null);
    });

    // Parity with proxy `parse_rejects_set_without_value`. SET / RESET must
    // be rejected when the value (or name in the case of RESET) is absent —
    // we never want to fold a partial token into the state.
    it('SET name = (no value) returns null', () => {
        assert.strictEqual(parseSetCommand('SET foo ='), null);
    });
    it('SET name TO (no value) returns null', () => {
        assert.strictEqual(parseSetCommand('SET foo TO'), null);
    });
    it('SET name (no separator, no value) returns null', () => {
        assert.strictEqual(parseSetCommand('SET foo'), null);
    });
    it('bare semicolon returns null', () => {
        assert.strictEqual(parseSetCommand(';'), null);
    });

    // Parity with proxy `parse_double_quoted_value` — PG-canonical use of `"`
    // around a value (rare but legal). Stripped just like single quotes.
    it('strips surrounding double quotes from value', () => {
        assert.deepStrictEqual(
            parseSetCommand('SET foo = "bar"'),
            { kind: 'set', name: 'foo', value: 'bar' },
        );
    });
});

// ─── splitStatements: respects string literals ────────────────────────────

describe('splitStatements', () => {
    it('single statement returns a one-element list', () => {
        assert.deepStrictEqual(splitStatements('SELECT 1'), ['SELECT 1']);
    });

    it('top-level semicolon splits', () => {
        assert.deepStrictEqual(
            splitStatements("SET app.user_id = '42'; SELECT 1"),
            ["SET app.user_id = '42'", 'SELECT 1'],
        );
    });

    it('semicolon inside a single-quoted string is not a split', () => {
        assert.deepStrictEqual(
            splitStatements("SELECT ';' FROM t; SELECT 2"),
            ["SELECT ';' FROM t", 'SELECT 2'],
        );
    });

    it('semicolon inside a double-quoted identifier is not a split', () => {
        assert.deepStrictEqual(
            splitStatements('SELECT "a;b" FROM t; SELECT 2'),
            ['SELECT "a;b" FROM t', 'SELECT 2'],
        );
    });

    it("doubled-quote escape is honored ('' inside '...')", () => {
        // The '' inside the string literal must NOT close the literal.
        // The trailing ; is therefore inside the string and not a split.
        assert.deepStrictEqual(
            splitStatements("SELECT 'it''s ;' FROM t"),
            ["SELECT 'it''s ;' FROM t"],
        );
    });

    it('empty / whitespace-only segments are dropped', () => {
        assert.deepStrictEqual(splitStatements(';;;'), []);
        assert.deepStrictEqual(splitStatements('SELECT 1;;'), ['SELECT 1']);
    });

    it('non-string / empty input', () => {
        assert.deepStrictEqual(splitStatements(''), []);
        assert.deepStrictEqual(splitStatements(null), []);
        assert.deepStrictEqual(splitStatements(undefined), []);
    });
});

// ─── ConnectionGucState ───────────────────────────────────────────────────
//
// Per-connection state machine. Hash is 0 at baseline, recomputed on every
// mutation that touches an unsafe GUC. SET LOCAL is a no-op (cache is
// bypassed mid-transaction anyway). Multi-statement bodies are honored
// via observeSql. Insertion order must not affect the hash (BTreeMap-style
// canonicalisation).

describe('ConnectionGucState', () => {
    it('starts at hash 0', () => {
        const s = new ConnectionGucState();
        assert.strictEqual(s.stateHash(), 0);
    });

    it('SET unsafe GUC changes the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        assert.notStrictEqual(s.stateHash(), 0);
    });

    it('SET safe GUC does NOT change the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET timezone = 'UTC'");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('SET LOCAL on an unsafe GUC is a no-op for the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET LOCAL app.user_id = '42'");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('different unsafe GUC values produce different hashes', () => {
        const a = new ConnectionGucState();
        a.observeSql("SET app.user_id = '42'");
        const b = new ConnectionGucState();
        b.observeSql("SET app.user_id = '99'");
        assert.notStrictEqual(a.stateHash(), b.stateHash());
    });

    it('insertion order does not affect the hash', () => {
        const a = new ConnectionGucState();
        a.observeSql("SET app.user_id = '42'");
        a.observeSql("SET app.tenant = 'acme'");
        const b = new ConnectionGucState();
        b.observeSql("SET app.tenant = 'acme'");
        b.observeSql("SET app.user_id = '42'");
        assert.strictEqual(a.stateHash(), b.stateHash());
    });

    it('RESET name reverts the hash to baseline (0)', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const after = s.stateHash();
        assert.notStrictEqual(after, 0);
        s.observeSql('RESET app.user_id');
        assert.strictEqual(s.stateHash(), 0);
    });

    it('RESET ALL clears all unsafe state', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        s.observeSql("SET app.tenant = 'acme'");
        assert.notStrictEqual(s.stateHash(), 0);
        s.observeSql('RESET ALL');
        assert.strictEqual(s.stateHash(), 0);
    });

    it('multi-statement body in one observeSql call', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'; SELECT 1");
        assert.notStrictEqual(s.stateHash(), 0);
    });

    it('apply() returns true on a hash-changing mutation, false otherwise', () => {
        const s = new ConnectionGucState();
        // Unsafe SET → changes hash
        assert.ok(s.apply({ kind: 'set', name: 'app.user_id', value: '42' }));
        // Safe SET → no change
        assert.ok(!s.apply({ kind: 'set', name: 'timezone', value: 'UTC' }));
        // SET LOCAL → never changes hash
        assert.ok(!s.apply({ kind: 'set_local', name: 'app.user_id', value: '99' }));
        // Re-RESET an absent name → no change
        assert.ok(!s.apply({ kind: 'reset', name: 'nope' }));
    });

    it('observeSql handles non-SET SQL without mutating state', () => {
        const s = new ConnectionGucState();
        s.observeSql('SELECT * FROM users');
        s.observeSql('INSERT INTO orders VALUES (1)');
        assert.strictEqual(s.stateHash(), 0);
    });

    // Parity with proxy `reset_safe_guc_is_noop` — a RESET on a safe GUC
    // (not in the unsafe shortlist, no `.`) must not perturb the hash, even
    // if there's already unsafe state present.
    it('RESET <safe-guc> is a no-op for the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const after = s.stateHash();
        s.observeSql('RESET timezone');
        assert.strictEqual(s.stateHash(), after);
    });

    // Parity with proxy `overwrite_unsafe_value_changes_hash` — re-SETting
    // an already-set unsafe GUC to a different value must move the hash.
    it('overwriting an unsafe GUC value changes the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const h1 = s.stateHash();
        s.observeSql("SET app.user_id = '43'");
        const h2 = s.stateHash();
        assert.notStrictEqual(h1, h2);
    });

    // RESET on a never-SET unsafe key must leave the hash unchanged. Without
    // this guard apply()'s `delete` would still recompute the hash on a
    // baseline-empty Map — wasted work, but more importantly it would still
    // be 0. This pins the cheap-no-op behavior.
    it('RESET on a never-SET unsafe key is a no-op', () => {
        const s = new ConnectionGucState();
        const ok = s.observeSql('RESET app.never_set');
        assert.strictEqual(ok, false);
        assert.strictEqual(s.stateHash(), 0);
    });

    // Parity with proxy `observe_multi_statement_with_quoted_semicolon`:
    // a `;` inside a single-quoted SET value must not split the statement
    // — the SET command should still apply with the full value.
    it('multi-statement with quoted semicolon still applies the SET', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.tenant = 'has;semicolon'; SELECT 1");
        // The hash moved because an unsafe GUC was set.
        assert.notStrictEqual(s.stateHash(), 0);
        // And the full value (semicolon included) is what got hashed —
        // verify by comparing against a fresh state set to the same value
        // as a single statement.
        const t = new ConnectionGucState();
        t.observeSql("SET app.tenant = 'has;semicolon'");
        assert.strictEqual(s.stateHash(), t.stateHash());
    });

    // SET-looking text that's actually inside a SELECT's string literal must
    // NOT shift the state hash. The fast path's split detection sees no
    // top-level `;`; parseSetCommand reads the first token (`SELECT`) and
    // bails. End-to-end safety check.
    it("'SET ...'-shaped text inside a string literal does not shift the hash", () => {
        const s = new ConnectionGucState();
        s.observeSql("SELECT 'SET app.user_id = pwn' FROM logs");
        assert.strictEqual(s.stateHash(), 0);
    });
});

// ─── Cache key state-hash gating ──────────────────────────────────────────
//
// makeKey folds the state hash into the cache key so two CachedClients
// with different unsafe-GUC state never collide on a shared singleton
// cache slot. Default 0 (no state) preserves the pre-state-hash key
// shape's distinguishability across SQL+values.

describe('makeKey state-hash inclusion', () => {
    it('default state-hash 0 is consistent with explicit 0', () => {
        assert.strictEqual(
            makeKey('SELECT 1', null),
            makeKey('SELECT 1', null, 0),
        );
    });

    it('different state hashes produce different keys for the same SQL', () => {
        const k1 = makeKey('SELECT * FROM users', null, 0);
        const k2 = makeKey('SELECT * FROM users', null, 0xdeadbeef);
        assert.notEqual(k1, k2);
    });

    it('same state hash + same SQL = same key', () => {
        assert.strictEqual(
            makeKey('SELECT 1', [1], 0xdeadbeef),
            makeKey('SELECT 1', [1], 0xdeadbeef),
        );
    });
});

describe('cache get/put state-hash gating', () => {
    it('put under hashA does not satisfy get under hashB', () => {
        const cache = makeCache();
        cache.put('SELECT * FROM accounts', null, [{ id: 'A' }], [], 0xaaaaaaaa);
        // Same SQL/values, different state hash → must miss.
        assert.equal(
            cache.get('SELECT * FROM accounts', null, 0xbbbbbbbb),
            null,
        );
        // Same hash → hits.
        assert.deepStrictEqual(
            cache.get('SELECT * FROM accounts', null, 0xaaaaaaaa).rows,
            [{ id: 'A' }],
        );
    });

    it('two separate CachedClient-style states are isolated under one cache', () => {
        // Models the actual deployment: one shared NativeCache singleton,
        // two CachedClient instances each with their own ConnectionGucState.
        const cache = makeCache();
        const a = new ConnectionGucState();
        a.observeSql("SET app.user_id = 'A'");
        const b = new ConnectionGucState();
        b.observeSql("SET app.user_id = 'B'");
        // User A populates.
        cache.put('SELECT * FROM accounts', null, [{ id: 'rows-A' }], [], a.stateHash());
        // User B must NOT see user A's rows.
        assert.equal(cache.get('SELECT * FROM accounts', null, b.stateHash()), null);
        // User A's own get hits.
        assert.deepStrictEqual(
            cache.get('SELECT * FROM accounts', null, a.stateHash()).rows,
            [{ id: 'rows-A' }],
        );
    });

    it('zero-arg get() (legacy callers) keys against state hash 0', () => {
        // Backwards compatibility: existing tests and callers that don't
        // know about state hashes get default 0, which matches
        // empty-state ConnectionGucState. Pre-state-hash behavior intact.
        const cache = makeCache();
        cache.put('SELECT 1', null, [{ x: 1 }], []); // implicit hash 0
        assert.deepStrictEqual(cache.get('SELECT 1', null).rows, [{ x: 1 }]);
        assert.deepStrictEqual(cache.get('SELECT 1', null, 0).rows, [{ x: 1 }]);
    });

    it('invalidation by table works across state-hash buckets', () => {
        // Each CachedClient has its own state hash; an INSERT routed to
        // the cache via invalidateTable must drop entries regardless of
        // which user populated them. extractTables is keyed by SQL text
        // alone, so the table index spans hashes naturally.
        const cache = makeCache();
        cache.put('SELECT * FROM orders', null, [{ id: 1 }], [], 0xaaaaaaaa);
        cache.put('SELECT * FROM orders', null, [{ id: 2 }], [], 0xbbbbbbbb);
        cache.invalidateTable('orders');
        assert.equal(cache.get('SELECT * FROM orders', null, 0xaaaaaaaa), null);
        assert.equal(cache.get('SELECT * FROM orders', null, 0xbbbbbbbb), null);
    });
});

// ─── DISCARD parsing (RLS hardening 2026-05-05) ───────────────────────────
//
// `DISCARD ALL` resets the entire session including custom GUCs — must
// clear the unsafe-GUC state map. `DISCARD PLANS / SEQUENCES / TEMP /
// TEMPORARY` don't touch GUC state and are no-ops for the hash. The
// parser tags them with explicit `kind`s so future prepared-statement
// caches can hook the same parse without reshaping it.

describe('parseSetCommand DISCARD', () => {
    it('DISCARD ALL', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD ALL'), { kind: 'discard_all' });
    });
    it('DISCARD PLANS', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD PLANS'), { kind: 'discard_plans' });
    });
    it('DISCARD SEQUENCES', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD SEQUENCES'), { kind: 'discard_other' });
    });
    it('DISCARD TEMP', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD TEMP'), { kind: 'discard_other' });
    });
    it('DISCARD TEMPORARY', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD TEMPORARY'), { kind: 'discard_other' });
    });
    it('case-insensitive (lowercase)', () => {
        assert.deepStrictEqual(parseSetCommand('discard all'), { kind: 'discard_all' });
        assert.deepStrictEqual(parseSetCommand('discard plans'), { kind: 'discard_plans' });
    });
    it('mixed case', () => {
        assert.deepStrictEqual(parseSetCommand('Discard All'), { kind: 'discard_all' });
    });
    it('DISCARD with trailing semicolon', () => {
        assert.deepStrictEqual(parseSetCommand('DISCARD ALL;'), { kind: 'discard_all' });
    });
    it('DISCARD with no arg returns null', () => {
        assert.strictEqual(parseSetCommand('DISCARD'), null);
    });
    it('DISCARD with garbage returns null', () => {
        assert.strictEqual(parseSetCommand('DISCARD UNKNOWN'), null);
    });
    it('DISCARD with too many args returns null', () => {
        assert.strictEqual(parseSetCommand('DISCARD ALL EXTRA'), null);
    });
});

describe('ConnectionGucState DISCARD', () => {
    it('DISCARD ALL clears unsafe-GUC state', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        s.observeSql("SET app.tenant = 'acme'");
        assert.notStrictEqual(s.stateHash(), 0);
        s.observeSql('DISCARD ALL');
        assert.strictEqual(s.stateHash(), 0);
    });

    it('DISCARD PLANS does NOT clear unsafe-GUC state', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const before = s.stateHash();
        s.observeSql('DISCARD PLANS');
        assert.strictEqual(s.stateHash(), before);
    });

    it('DISCARD SEQUENCES is a no-op for the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const before = s.stateHash();
        s.observeSql('DISCARD SEQUENCES');
        assert.strictEqual(s.stateHash(), before);
    });

    it('DISCARD TEMP is a no-op for the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const before = s.stateHash();
        s.observeSql('DISCARD TEMP');
        assert.strictEqual(s.stateHash(), before);
    });

    it('DISCARD TEMPORARY is a no-op for the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        const before = s.stateHash();
        s.observeSql('DISCARD TEMPORARY');
        assert.strictEqual(s.stateHash(), before);
    });

    it('DISCARD ALL on empty state is a no-op', () => {
        const s = new ConnectionGucState();
        assert.strictEqual(s.observeSql('DISCARD ALL'), false);
        assert.strictEqual(s.stateHash(), 0);
    });

    it('DISCARD ALL in multi-statement body still applies', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET app.user_id = '42'");
        assert.notStrictEqual(s.stateHash(), 0);
        // Real client pattern: pool reset followed by re-init.
        s.observeSql("DISCARD ALL; SET app.user_id = '99'");
        // After the multi-statement body: user_id is set (to '99'), so the
        // hash is non-zero, but it must equal the hash for setting just
        // user_id='99' on a fresh state — DISCARD ALL must have wiped
        // anything that was there before.
        const t = new ConnectionGucState();
        t.observeSql("SET app.user_id = '99'");
        assert.strictEqual(s.stateHash(), t.stateHash());
    });
});

// ─── SELECT set_config(...) function-form parsing ─────────────────────────
//
// Supabase's canonical RLS pattern. Functionally equivalent to SET; the
// third arg is is_local (mirrors SET LOCAL). Recognised at the statement
// level — `SELECT set_config(...)` and `SELECT pg_catalog.set_config(...)`
// both update the per-connection state hash inline.

describe('parseSetCommand set_config()', () => {
    it('basic 3-arg form (is_local=false)', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('app.user_id', '42', false)"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('is_local=true → set_local', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('app.user_id', '42', true)"),
            { kind: 'set_local', name: 'app.user_id', value: '42' },
        );
    });

    it('pg_catalog-qualified form', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT pg_catalog.set_config('app.user_id', '42', false)"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('case-insensitive function name and SELECT', () => {
        assert.deepStrictEqual(
            parseSetCommand("select SET_CONFIG('app.user_id', '42', FALSE)"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('whitespace tolerance around args', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config ( 'app.user_id' , '42' , false )"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('PG boolean spellings: t/f', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('app.x', '1', t)").kind, 'set_local',
        );
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('app.x', '1', f)").kind, 'set',
        );
    });

    it('PG boolean spellings: yes/no, on/off, 1/0', () => {
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', yes)").kind, 'set_local');
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', no)").kind, 'set');
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', on)").kind, 'set_local');
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', off)").kind, 'set');
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', 1)").kind, 'set_local');
        assert.strictEqual(parseSetCommand("SELECT set_config('a.b', '1', 0)").kind, 'set');
    });

    it('trailing semicolon tolerated', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('app.user_id', '42', false);"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('lowercases the GUC name', () => {
        assert.deepStrictEqual(
            parseSetCommand("SELECT set_config('App.User_ID', '42', false)"),
            { kind: 'set', name: 'app.user_id', value: '42' },
        );
    });

    it('rejects bare-expression name (no string literal)', () => {
        // `set_config(my_var, '42', false)` — we can't trust an
        // expression as the name without evaluating it. Skip tracking.
        assert.strictEqual(
            parseSetCommand("SELECT set_config(my_var, '42', false)"),
            null,
        );
    });

    it('rejects ambiguous is_local value', () => {
        assert.strictEqual(
            parseSetCommand("SELECT set_config('app.x', '1', maybe)"),
            null,
        );
    });

    it('rejects wrong arity', () => {
        assert.strictEqual(parseSetCommand("SELECT set_config('a', '1')"), null);
        assert.strictEqual(parseSetCommand("SELECT set_config('a', '1', false, 'extra')"), null);
        assert.strictEqual(parseSetCommand("SELECT set_config()"), null);
    });

    it('rejects set_config embedded inside a larger expression', () => {
        // `SELECT set_config(...) || ' suffix'` — only top-level
        // invocations are tracked.
        assert.strictEqual(
            parseSetCommand("SELECT set_config('app.x', '1', false) || ' tail'"),
            null,
        );
        // `SELECT (SELECT set_config(...))` — nested SELECT, not top-level.
        assert.strictEqual(
            parseSetCommand("SELECT 1 + 1 FROM dual"),
            null,
        );
    });

    it('rejects set_config followed by FROM', () => {
        assert.strictEqual(
            parseSetCommand("SELECT set_config('app.x', '1', false) FROM dual"),
            null,
        );
    });

    it('non-set_config SELECT returns null', () => {
        assert.strictEqual(parseSetCommand('SELECT 1'), null);
        assert.strictEqual(parseSetCommand('SELECT * FROM users'), null);
        assert.strictEqual(parseSetCommand("SELECT 'set_config' FROM logs"), null);
    });
});

describe('ConnectionGucState set_config()', () => {
    it('SELECT set_config on unsafe GUC moves the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SELECT set_config('app.user_id', '42', false)");
        assert.notStrictEqual(s.stateHash(), 0);
    });

    it('SELECT set_config matches SET counterpart', () => {
        const a = new ConnectionGucState();
        a.observeSql("SET app.user_id = '42'");
        const b = new ConnectionGucState();
        b.observeSql("SELECT set_config('app.user_id', '42', false)");
        assert.strictEqual(a.stateHash(), b.stateHash());
    });

    it('SELECT pg_catalog.set_config matches SET counterpart', () => {
        const a = new ConnectionGucState();
        a.observeSql("SET app.user_id = '42'");
        const b = new ConnectionGucState();
        b.observeSql("SELECT pg_catalog.set_config('app.user_id', '42', false)");
        assert.strictEqual(a.stateHash(), b.stateHash());
    });

    it('SELECT set_config(..., true) is a no-op for the hash (is_local=true)', () => {
        const s = new ConnectionGucState();
        s.observeSql("SELECT set_config('app.user_id', '42', true)");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('SELECT set_config on safe GUC does not move the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SELECT set_config('timezone', 'UTC', false)");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('multi-statement: set_config then SELECT', () => {
        const s = new ConnectionGucState();
        s.observeSql(
            "SELECT set_config('app.user_id', '42', false); SELECT 1"
        );
        assert.notStrictEqual(s.stateHash(), 0);
    });

    it('set_config with `;` inside the value literal does not split', () => {
        const s = new ConnectionGucState();
        s.observeSql("SELECT set_config('app.tenant', 'a;b', false)");
        const t = new ConnectionGucState();
        t.observeSql("SET app.tenant = 'a;b'");
        assert.strictEqual(s.stateHash(), t.stateHash());
    });
});

// ─── Classifier: explicit known-safe list ─────────────────────────────────
//
// Locale / formatting GUCs are explicitly classified as safe. They affect
// text rendering only — never which rows the user can see — and so can be
// freely SET without fragmenting the cache.

describe('isUnsafeGuc known-safe list', () => {
    it('DateStyle / IntervalStyle / TimeZone are safe', () => {
        assert.ok(!isUnsafeGuc('DateStyle'));
        assert.ok(!isUnsafeGuc('IntervalStyle'));
        assert.ok(!isUnsafeGuc('TimeZone'));
    });

    it('bytea_output is safe', () => {
        assert.ok(!isUnsafeGuc('bytea_output'));
    });

    it('lc_messages / lc_monetary / lc_numeric / lc_time are safe', () => {
        assert.ok(!isUnsafeGuc('lc_messages'));
        assert.ok(!isUnsafeGuc('lc_monetary'));
        assert.ok(!isUnsafeGuc('lc_numeric'));
        assert.ok(!isUnsafeGuc('lc_time'));
    });

    it('classification is case-insensitive (DATESTYLE, datestyle, DateStyle)', () => {
        assert.ok(!isUnsafeGuc('DATESTYLE'));
        assert.ok(!isUnsafeGuc('datestyle'));
        assert.ok(!isUnsafeGuc('DateStyle'));
        assert.ok(!isUnsafeGuc('TIMEZONE'));
        assert.ok(!isUnsafeGuc('Timezone'));
    });

    it('SET DateStyle does not move the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET DateStyle = 'ISO, MDY'");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('SET TimeZone does not move the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET TimeZone = 'UTC'");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('SET lc_messages does not move the hash', () => {
        const s = new ConnectionGucState();
        s.observeSql("SET lc_messages = 'en_US.UTF-8'");
        assert.strictEqual(s.stateHash(), 0);
    });

    it('KNOWN_SAFE_GUCS export is iterable / inspectable', () => {
        // Export contract — downstream tooling (e.g. dashboards that
        // surface "tracked GUCs" diagnostics) reads this list directly.
        assert.ok(KNOWN_SAFE_GUCS instanceof Set);
        assert.ok(KNOWN_SAFE_GUCS.has('datestyle'));
        assert.ok(KNOWN_SAFE_GUCS.has('intervalstyle'));
        assert.ok(KNOWN_SAFE_GUCS.has('timezone'));
        assert.ok(KNOWN_SAFE_GUCS.has('bytea_output'));
        assert.ok(KNOWN_SAFE_GUCS.has('lc_messages'));
        assert.ok(KNOWN_SAFE_GUCS.has('lc_monetary'));
        assert.ok(KNOWN_SAFE_GUCS.has('lc_numeric'));
        assert.ok(KNOWN_SAFE_GUCS.has('lc_time'));
    });
});
