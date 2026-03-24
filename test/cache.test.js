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
