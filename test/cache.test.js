import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'net';
import { NativeCache, makeKey, detectWrite, extractTables, DDL_SENTINEL, TX_START, TX_END } from '../cache.js';

function makeCache(opts = {}) {
    NativeCache._reset();
    if (opts.maxEntries) process.env.GOLDLAPEL_NATIVE_CACHE_SIZE = String(opts.maxEntries);
    if (opts.enabled === false) process.env.GOLDLAPEL_NATIVE_CACHE = 'false';
    const cache = new NativeCache();
    if (opts.connected !== false) cache._invalidationConnected = true;
    delete process.env.GOLDLAPEL_NATIVE_CACHE_SIZE;
    delete process.env.GOLDLAPEL_NATIVE_CACHE;
    return cache;
}

afterEach(() => NativeCache._reset());

// --- makeKey ---

describe('makeKey', () => {
    it('null values', () => {
        assert.equal(makeKey('SELECT 1', null), 'SELECT 1\0null');
    });

    it('array values', () => {
        assert.equal(makeKey('SELECT $1', [42]), 'SELECT $1\0[42]');
    });

    it('undefined values treated as null', () => {
        assert.equal(makeKey('SELECT 1', undefined), 'SELECT 1\0null');
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
