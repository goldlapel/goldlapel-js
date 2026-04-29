import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { GoldLapel, start, _driverNotFoundError, _detectDriver, _connectWithDriver, _logLevelToVerboseFlag, _makePostgresJsAdapter, _DRIVER_CANDIDATES } from '../index.js';
import * as goldlapel from '../index.js';
import goldlapelDefault from '../index.js';

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
            await g.publish('events', 'x');
            await g.publish('events', 'y');
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

    it('scope does not leak across sibling Promise.all tasks (v0.2 Tests Q7 parity)', async () => {
        // Regression: the scoped conn set by gl.using() must NOT be visible to
        // a sibling async task running concurrently via Promise.all. Node's
        // AsyncLocalStorage is the canonical primitive for per-async-context
        // state — using shared instance state (e.g. this._scopeConn = conn)
        // would produce the leak this test guards against. Ruby had this
        // bug class once in its fiber-local handling (see
        // goldlapel-ruby test_async_native.rb::test_using_scope_under_async_reactor).
        //
        // Determinism: coordinated with Promise barriers, never setTimeout.
        //   1. Task A enters gl.using(connA, ...) and signals `aInsideUsing`.
        //   2. Task B waits on `aInsideUsing`, then invokes gl.publish() —
        //      this is the critical moment when a leak would manifest.
        //   3. Task B records which mock conn it hit, then signals `bDone`.
        //   4. Task A waits on `bDone` before exiting using() — this guarantees
        //      B's observation happened with A still inside the `using()`
        //      scope, which is the scenario a leaking impl would get wrong.
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const def = mockClient({ rows: [], rowCount: 0 });
        const connA = mockClient({ rows: [], rowCount: 0 });
        gl._defaultConn = def;

        let resolveAInsideUsing;
        const aInsideUsing = new Promise((r) => { resolveAInsideUsing = r; });
        let resolveBDone;
        const bDone = new Promise((r) => { resolveBDone = r; });

        const taskA = async () => {
            await gl.using(connA, async (g) => {
                resolveAInsideUsing();  // signal: B may proceed
                await bDone;            // wait: hold the scope open until B finishes
                // A's own call must still see connA (scope intact for A).
                await g.publish('channel_a', 'from-a');
            });
        };

        const taskB = async () => {
            await aInsideUsing;  // wait: A is inside using()
            // This call runs in a sibling Promise chain — it must resolve to
            // `def`, not `connA`. Under AsyncLocalStorage, B's async context
            // was never entered via connScope.run(), so getStore() returns
            // undefined and _resolveConn falls back to _defaultConn.
            await gl.publish('channel_b', 'from-b');
            resolveBDone();
        };

        await Promise.all([taskA(), taskB()]);

        // Task A: used connA (scoped).
        assert.strictEqual(connA._calls.length, 1, 'task A used its scoped connA');
        assert.strictEqual(
            connA._calls[0].values[0],
            'channel_a',
            'task A published channel_a on connA',
        );
        // Task B: used default conn (no leak).
        assert.strictEqual(
            def._calls.length,
            1,
            'task B used the default conn (no scope leak from sibling task A)',
        );
        assert.strictEqual(
            def._calls[0].values[0],
            'channel_b',
            'task B published channel_b on the default conn',
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

    // Builds a fake `require.resolve` that only resolves names in `installed`
    // and throws for anything else — mirroring real require.resolve behavior
    // for a project that has only those packages in its node_modules.
    function fakeResolver(installed) {
        const set = new Set(installed);
        return (name) => {
            if (set.has(name)) return `/fake/node_modules/${name}/index.js`;
            const err = new Error(`Cannot find module '${name}'`);
            err.code = 'MODULE_NOT_FOUND';
            throw err;
        };
    }

    // Env-var override on _detectDriver would short-circuit the resolver path
    // under test; stash and clear it for the priority suite.
    function withCleanDriverEnv(fn) {
        return async () => {
            const orig = process.env.GOLDLAPEL_DRIVER;
            delete process.env.GOLDLAPEL_DRIVER;
            try {
                await fn();
            } finally {
                if (orig !== undefined) process.env.GOLDLAPEL_DRIVER = orig;
            }
        };
    }

    it('priority: pg wins when all three drivers are installed', withCleanDriverEnv(() => {
        const resolve = fakeResolver(['pg', 'postgres', '@vercel/postgres']);
        assert.strictEqual(_detectDriver(resolve), 'pg');
    }));

    it('priority: postgres (postgres.js) wins when pg is missing', withCleanDriverEnv(() => {
        const resolve = fakeResolver(['postgres', '@vercel/postgres']);
        assert.strictEqual(_detectDriver(resolve), 'postgres');
    }));

    it('priority: @vercel/postgres is the last-resort when only it is installed', withCleanDriverEnv(() => {
        const resolve = fakeResolver(['@vercel/postgres']);
        assert.strictEqual(_detectDriver(resolve), '@vercel/postgres');
    }));

    // Documented behavior: _detectDriver picks ONE driver up front based on
    // `require.resolve` success; _connectWithDriver only ever sees that single
    // choice. If that driver's connect() throws, the error propagates upward
    // — there is no automatic fall-through to the next candidate. This test
    // pins that contract (no-fallthrough) so any future "try each driver
    // until one connects" refactor has to explicitly update the test (and
    // docs) rather than silently changing user-visible behavior. We prove it
    // by invoking _connectWithDriver with a name that isn't one of the three
    // supported drivers — the function throws `Unsupported driver` rather
    // than attempting the next candidate.
    it('connect dispatch throws upward — no automatic fall-through to other drivers', async () => {
        await assert.rejects(
            () => _connectWithDriver('not-a-real-driver', 'postgresql://u:p@localhost/db'),
            /Unsupported driver: not-a-real-driver/,
        );
    });

    it('no drivers installed → _detectDriver returns null and _driverNotFoundError names all three alternatives', withCleanDriverEnv(() => {
        const resolve = fakeResolver([]);
        assert.strictEqual(_detectDriver(resolve), null);

        const err = _driverNotFoundError();
        // Must explicitly name each of the three supported drivers so the
        // user can copy-paste the install command that matches their stack.
        assert.match(err.message, /\bpg\b/);
        assert.match(err.message, /\bpostgres\b/);
        assert.match(err.message, /@vercel\/postgres/);
        // And at least one install hint per the README.
        assert.match(err.message, /npm install/);

        // Sanity-check _DRIVER_CANDIDATES hasn't drifted from the error text.
        assert.deepStrictEqual(_DRIVER_CANDIDATES, ['pg', 'postgres', '@vercel/postgres']);
    }));
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

// ─── silent option / startup banner ────────────────────────────────────────
//
// Banner goes to stderr (never stdout) so it doesn't corrupt piped stdout
// output from user programs. `silent: true` suppresses it entirely. `silent`
// is a wrapper-only option — it must never be forwarded to the Rust binary
// as a `--silent` CLI flag.

describe('silent option', () => {
    // Capture writes to process.stdout and process.stderr for the duration
    // of `fn`, then restore. Returns { stdout, stderr } strings. We patch
    // the streams rather than console.* because console.error ultimately
    // calls process.stderr.write — intercepting at the stream layer catches
    // both console.* and any direct stream.write usage.
    async function captureStreams(fn) {
        const origOut = process.stdout.write.bind(process.stdout);
        const origErr = process.stderr.write.bind(process.stderr);
        let stdout = '';
        let stderr = '';
        process.stdout.write = (chunk) => {
            stdout += typeof chunk === 'string' ? chunk : chunk.toString();
            return true;
        };
        process.stderr.write = (chunk) => {
            stderr += typeof chunk === 'string' ? chunk : chunk.toString();
            return true;
        };
        try {
            await fn();
        } finally {
            process.stdout.write = origOut;
            process.stderr.write = origErr;
        }
        return { stdout, stderr };
    }

    it('stored on the instance as a boolean', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { silent: true });
        assert.strictEqual(gl._silent, true);
    });

    it('default is false', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._silent, false);
    });

    it('coerces truthy/falsy values to booleans', () => {
        const gl1 = new GoldLapel('postgresql://localhost:5432/mydb', { silent: 1 });
        assert.strictEqual(gl1._silent, true);
        const gl2 = new GoldLapel('postgresql://localhost:5432/mydb', { silent: 0 });
        assert.strictEqual(gl2._silent, false);
        const gl3 = new GoldLapel('postgresql://localhost:5432/mydb', { silent: undefined });
        assert.strictEqual(gl3._silent, false);
    });

    it('_printBanner writes to stderr, not stdout', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const { stdout, stderr } = await captureStreams(() => gl._printBanner());
        assert.strictEqual(stdout, '', `banner must not leak to stdout: ${JSON.stringify(stdout)}`);
        assert.match(stderr, /goldlapel → :7932 \(proxy\)/);
    });

    it('_printBanner includes dashboard URL when dashboardPort is set', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { port: 7932, dashboardPort: 7933 });
        const { stdout, stderr } = await captureStreams(() => gl._printBanner());
        assert.strictEqual(stdout, '');
        assert.match(stderr, /:7932 \(proxy\)/);
        assert.match(stderr, /http:\/\/127\.0\.0\.1:7933 \(dashboard\)/);
    });

    it('silent: true suppresses the banner on both streams', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { silent: true });
        const { stdout, stderr } = await captureStreams(() => gl._printBanner());
        assert.strictEqual(stdout, '', `silent must suppress stdout: ${stdout}`);
        assert.strictEqual(stderr, '', `silent must suppress stderr: ${stderr}`);
    });

    it('silent: false still prints (same as default)', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { silent: false });
        const { stdout, stderr } = await captureStreams(() => gl._printBanner());
        assert.strictEqual(stdout, '');
        assert.match(stderr, /goldlapel → /);
    });

    it('silent is not forwarded to the binary argv', () => {
        // The CLI-args builder only walks `this._config` through _configToArgs,
        // and `silent` is stored as its own field (`this._silent`), never in
        // `this._config`. So no `--silent` flag can reach the Rust binary.
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { silent: true });
        const args = gl._buildSpawnArgs();
        assert.ok(!args.includes('--silent'),
            `argv must not contain --silent: ${args.join(' ')}`);
        assert.ok(!args.some(a => a === 'silent' || /--?silent/i.test(a)),
            `argv must not contain any silent flag: ${args.join(' ')}`);
    });

    it('silent in config object is rejected at construction (wrapper-only key)', () => {
        // Belt-and-suspenders: even if a user tried to pass silent via config
        // (e.g. copy-pasted from Python docs), the constructor rejects it at
        // construction time because it's not in VALID_CONFIG_KEYS — the Rust
        // binary has no --silent flag, so forwarding it would be a silent
        // no-op at best and a spawn error at worst.
        assert.throws(
            () => new GoldLapel('postgresql://localhost:5432/mydb', {
                config: { silent: true },
            }),
            /Unknown config keys: silent/,
        );
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

// ─── Default export mirrors the named export surface ───────────────────────

describe('default export symmetry', () => {
    it('default export exposes GoldLapel, start, and utilities', () => {
        // Spot-check representative names from each category
        assert.strictEqual(goldlapelDefault.GoldLapel, goldlapel.GoldLapel);
        assert.strictEqual(goldlapelDefault.start, goldlapel.start);
        assert.strictEqual(goldlapelDefault.configKeys, goldlapel.configKeys);
        assert.strictEqual(goldlapelDefault.wrap, goldlapel.wrap);
        assert.strictEqual(goldlapelDefault.NativeCache, goldlapel.NativeCache);
        assert.strictEqual(goldlapelDefault.publish, goldlapel.publish);
        assert.strictEqual(goldlapelDefault.subscribe, goldlapel.subscribe);
        assert.strictEqual(goldlapelDefault.search, goldlapel.search);
        assert.strictEqual(goldlapelDefault.docInsert, goldlapel.docInsert);
        assert.strictEqual(goldlapelDefault.zadd, goldlapel.zadd);
        assert.strictEqual(goldlapelDefault.streamAdd, goldlapel.streamAdd);
    });

    it('every named export is reachable via the default export', () => {
        const missing = [];
        for (const name of Object.keys(goldlapel)) {
            if (name === 'default') continue;
            // Private helpers prefixed with _ aren't part of the public surface
            // — they're exported for tests/advanced users and don't need to be
            // in the default export.
            if (name.startsWith('_')) continue;
            if (!(name in goldlapelDefault)) {
                missing.push(name);
            }
        }
        assert.deepStrictEqual(missing, [],
            `named exports missing from default: ${JSON.stringify(missing)}`);
    });
});
