import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GoldLapel, start, _driverNotFoundError, _logLevelToVerboseFlag, _makePostgresJsAdapter } from '../index.js';

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

// ─── gl.using() — scoped conn override ─────────────────────────────────────

describe('gl.using(conn, callback)', () => {
    it('scopes conn across synchronous calls', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [{ value: '1' }], rowCount: 1 });
        const override = mockClient({ rows: [{ value: '9' }], rowCount: 1 });
        gl._defaultConn = def;

        await gl.using(override, async (g) => {
            await g.incr('counters', 'x');
            await g.incr('counters', 'y');
        });

        assert.strictEqual(def._calls.length, 0, 'default conn untouched inside using()');
        assert.ok(override._calls.length >= 2, 'override conn used');
    });

    it('scopes conn across async awaits', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        await gl.using(override, async (g) => {
            // Yield to the microtask queue to prove scope survives awaits
            await new Promise((r) => setTimeout(r, 5));
            await g.publish('events', 'hi');
        });

        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(override._calls.length, 1);
    });

    it('restores default conn after using() returns', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const override = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        await gl.using(override, async (g) => {
            await g.publish('a', '1');
        });

        await gl.publish('b', '2');

        assert.strictEqual(override._calls.length, 1);
        assert.strictEqual(def._calls.length, 1);
    });

    it('per-method { conn } wins over using() scope', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const scoped = mockClient({ rows: [], rowCount: 0 });
        const perCall = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        await gl.using(scoped, async (g) => {
            await g.publish('events', 'hi', { conn: perCall });
        });

        assert.strictEqual(def._calls.length, 0);
        assert.strictEqual(scoped._calls.length, 0);
        assert.strictEqual(perCall._calls.length, 1);
    });

    it('using() returns callback return value', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const override = mockClient({ rows: [{ value: '5' }], rowCount: 1 });
        const result = await gl.using(override, async () => 'return-val');
        assert.strictEqual(result, 'return-val');
    });

    it('using() throws when callback is not a function', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await assert.rejects(
            () => gl.using({}, null),
            TypeError,
        );
    });

    it('nested using() calls stack correctly', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const outer = mockClient({ rows: [], rowCount: 0 });
        const inner = mockClient({ rows: [], rowCount: 0 });

        await gl.using(outer, async (g) => {
            await g.publish('a', '1');
            await g.using(inner, async (g2) => {
                await g2.publish('b', '2');
            });
            await g.publish('c', '3');
        });

        assert.strictEqual(outer._calls.length, 2);  // 'a' and 'c'
        assert.strictEqual(inner._calls.length, 1);  // 'b'
    });

    it('using() scope is per-instance — does not leak across sibling GoldLapel instances', async () => {
        // Regression: the AsyncLocalStorage backing gl.using() must be an
        // instance field, not module-scoped. Otherwise a method invoked on gl2
        // from inside gl1.using(connA, ...) would incorrectly pick up connA.
        const gl1 = new GoldLapel('postgresql://localhost:5432/dbA');
        const gl2 = new GoldLapel('postgresql://localhost:5432/dbB');

        const connA = mockClient({ rows: [], rowCount: 0 });
        const defB = mockClient({ rows: [], rowCount: 0 });
        gl2._defaultConn = defB;

        await gl1.using(connA, async (g1) => {
            // Cross-instance call: gl2 should use its own default conn,
            // not gl1's scoped connA.
            await gl2.publish('events', 'from-gl2');
            // gl1's own scoped call should still use connA.
            await g1.publish('events', 'from-gl1');
        });

        assert.strictEqual(defB._calls.length, 1, 'gl2 used its own default conn');
        assert.strictEqual(connA._calls.length, 1, 'gl1 used its scoped connA');
        assert.strictEqual(
            defB._calls[0].values[1],
            'from-gl2',
            'gl2 published the gl2 message on its default conn',
        );
        assert.strictEqual(
            connA._calls[0].values[1],
            'from-gl1',
            'gl1 published the gl1 message on its scoped conn',
        );
    });
});

// ─── Symbol.asyncDispose ───────────────────────────────────────────────────

describe('Symbol.asyncDispose', () => {
    it('is defined as an async function', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(typeof gl[Symbol.asyncDispose], 'function');
    });

    it('calls stop() when disposed', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        let closed = false;
        gl._defaultConn = { end: async () => {} };
        gl._defaultClose = async () => { closed = true; };
        await gl[Symbol.asyncDispose]();
        assert.strictEqual(closed, true);
        assert.strictEqual(gl._defaultConn, null);
    });

    it('is idempotent', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await gl[Symbol.asyncDispose]();
        await gl[Symbol.asyncDispose]();
        assert.strictEqual(gl.running, false);
    });
});

// ─── start() factory ────────────────────────────────────────────────────────

describe('start() factory', () => {
    it('is exported as an async function', () => {
        assert.strictEqual(typeof start, 'function');
    });

    it('throws helpful error when binary missing', async () => {
        const orig = process.env.GOLDLAPEL_BINARY;
        process.env.GOLDLAPEL_BINARY = '/nonexistent/goldlapel';
        try {
            await assert.rejects(
                () => start('postgresql://user:pass@localhost/mydb'),
                /GOLDLAPEL_BINARY/
            );
        } finally {
            if (orig !== undefined) {
                process.env.GOLDLAPEL_BINARY = orig;
            } else {
                delete process.env.GOLDLAPEL_BINARY;
            }
        }
    });
});

// ─── Driver detection ──────────────────────────────────────────────────────

describe('driver detection', () => {
    it('_driverNotFoundError returns a helpful Error', () => {
        const err = _driverNotFoundError();
        assert.ok(err instanceof Error);
        assert.match(err.message, /No supported Postgres driver/);
        assert.match(err.message, /pg/);
        assert.match(err.message, /postgres/);
        assert.match(err.message, /vercel/);
        assert.match(err.message, /noConnect/);
    });
});

// ─── noConnect option ──────────────────────────────────────────────────────

describe('noConnect option', () => {
    it('stored on the instance', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { noConnect: true });
        assert.strictEqual(gl._noConnect, true);
    });

    it('default is false', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._noConnect, false);
    });

    it('_openDefaultConn is a no-op when noConnect is true', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { noConnect: true });
        await gl._openDefaultConn();
        assert.strictEqual(gl._defaultConn, null);
    });
});

// ─── logLevel option ───────────────────────────────────────────────────────

describe('logLevel option', () => {
    it('accepts valid log levels on the instance', () => {
        for (const lvl of ['trace', 'debug', 'info', 'warn', 'error']) {
            const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: lvl });
            assert.strictEqual(gl._logLevel, lvl);
        }
    });

    it('is undefined when not specified', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._logLevel, undefined);
    });
});

describe('_logLevelToVerboseFlag()', () => {
    it('maps trace → -vvv', () => {
        assert.strictEqual(_logLevelToVerboseFlag('trace'), '-vvv');
    });

    it('maps debug → -vv', () => {
        assert.strictEqual(_logLevelToVerboseFlag('debug'), '-vv');
    });

    it('maps info → -v', () => {
        assert.strictEqual(_logLevelToVerboseFlag('info'), '-v');
    });

    it('maps warn/warning/error → null (default level, no flag emitted)', () => {
        assert.strictEqual(_logLevelToVerboseFlag('warn'), null);
        assert.strictEqual(_logLevelToVerboseFlag('warning'), null);
        assert.strictEqual(_logLevelToVerboseFlag('error'), null);
    });

    it('returns null for undefined / null', () => {
        assert.strictEqual(_logLevelToVerboseFlag(undefined), null);
        assert.strictEqual(_logLevelToVerboseFlag(null), null);
    });

    it('is case-insensitive', () => {
        assert.strictEqual(_logLevelToVerboseFlag('DEBUG'), '-vv');
        assert.strictEqual(_logLevelToVerboseFlag('Info'), '-v');
    });

    it('throws a clear error on invalid level strings', () => {
        assert.throws(
            () => _logLevelToVerboseFlag('verbose'),
            /logLevel must be one of: trace, debug, info, warn, error/,
        );
        assert.throws(
            () => _logLevelToVerboseFlag('loud'),
            /logLevel must be one of: trace, debug, info, warn, error/,
        );
    });

    it('throws on non-string values', () => {
        assert.throws(
            () => _logLevelToVerboseFlag(5),
            /logLevel must be one of: trace, debug, info, warn, error/,
        );
    });
});

describe('GoldLapel._buildSpawnArgs()', () => {
    it('emits -vv when logLevel is "debug"', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: 'debug' });
        const args = gl._buildSpawnArgs();
        assert.ok(args.includes('-vv'), `expected -vv in args: ${args.join(' ')}`);
        assert.ok(!args.includes('--log-level'),
            `must NOT pass --log-level (unsupported by proxy): ${args.join(' ')}`);
    });

    it('emits -v when logLevel is "info"', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: 'info' });
        const args = gl._buildSpawnArgs();
        assert.ok(args.includes('-v'));
        assert.ok(!args.includes('-vv'));
    });

    it('emits -vvv when logLevel is "trace"', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: 'trace' });
        assert.ok(gl._buildSpawnArgs().includes('-vvv'));
    });

    it('emits no verbose flag for warn/error (default level)', () => {
        for (const lvl of ['warn', 'error']) {
            const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: lvl });
            const args = gl._buildSpawnArgs();
            assert.ok(!args.some(a => a.startsWith('-v')),
                `unexpected verbose flag for ${lvl}: ${args.join(' ')}`);
        }
    });

    it('emits no verbose flag when logLevel unset', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const args = gl._buildSpawnArgs();
        assert.ok(!args.some(a => a.startsWith('-v')));
    });

    it('raises on invalid logLevel', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { logLevel: 'loud' });
        assert.throws(
            () => gl._buildSpawnArgs(),
            /logLevel must be one of: trace, debug, info, warn, error/,
        );
    });
});

// ─── postgres.js driver adapter pub/sub behavior ───────────────────────────
//
// postgres.js has no `client.on('notification', ...)` event model, so
// subscribe() and docWatch() can't deliver events through it. Rather than
// silently swallow them, the adapter throws on on/off/once/removeListener
// and points users at pg.

describe('_makePostgresJsAdapter', () => {
    function fakeSql() {
        const calls = [];
        const sql = async () => [];
        sql.unsafe = async (text, args) => {
            calls.push({ text, args });
            const rows = [];
            rows.count = 0;
            rows.command = 'SELECT';
            rows.columns = [];
            return rows;
        };
        sql.end = async () => {};
        sql._calls = calls;
        return sql;
    }

    it('query() delegates to sql.unsafe and normalizes the result shape', async () => {
        const sql = fakeSql();
        const conn = _makePostgresJsAdapter(sql);
        const res = await conn.query('SELECT 1', []);
        assert.deepStrictEqual(res, {
            rows: [],
            fields: [],
            rowCount: 0,
            command: 'SELECT',
        });
        assert.strictEqual(sql._calls.length, 1);
        assert.strictEqual(sql._calls[0].text, 'SELECT 1');
    });

    it('on() throws a helpful pub/sub-not-supported error', () => {
        const conn = _makePostgresJsAdapter(fakeSql());
        assert.throws(
            () => conn.on('notification', () => {}),
            /pub\/sub .* not supported .* 'postgres' \(postgres\.js\)/s,
        );
    });

    it('off() throws with the same message', () => {
        const conn = _makePostgresJsAdapter(fakeSql());
        assert.throws(
            () => conn.off('notification', () => {}),
            /pub\/sub .* not supported/,
        );
    });

    it('once() throws with the same message', () => {
        const conn = _makePostgresJsAdapter(fakeSql());
        assert.throws(
            () => conn.once('notification', () => {}),
            /pub\/sub .* not supported/,
        );
    });

    it('removeListener() throws with the same message', () => {
        const conn = _makePostgresJsAdapter(fakeSql());
        assert.throws(
            () => conn.removeListener('notification', () => {}),
            /pub\/sub .* not supported/,
        );
    });

    it('error message points users at pg and the { conn } / using() escape hatches', () => {
        const conn = _makePostgresJsAdapter(fakeSql());
        try {
            conn.on('notification', () => {});
            assert.fail('expected throw');
        } catch (err) {
            assert.match(err.message, /pg/);
            assert.match(err.message, /\{ conn \}|gl\.using/);
        }
    });
});
