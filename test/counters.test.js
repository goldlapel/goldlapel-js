// Unit tests for counters.js — the nested gl.counters namespace introduced
// in Phase 5 of schema-to-core (counter / zset / hash / queue / geo).
//
// These tests verify:
//   - gl.counters is a CountersAPI bound to the parent client
//   - Each verb fetches DDL patterns from the proxy and dispatches to the
//     `utils.counter*` helper with the right args
//   - The pattern cache is shared with the parent (one HTTP call per
//     (family, name) per session)
//   - SQL builders use the proxy's canonical query patterns (no in-wrapper
//     CREATE TABLE leaks, no placeholder translation)
//   - Phase-5 counter `updated_at` parity: the canonical patterns reference
//     `NOW()` on every UPDATE — wrappers don't paper over this

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    GoldLapel, CountersAPI,
    counterIncr, counterSet, counterGet, counterDelete, counterCountKeys,
} from '../index.js';

class FakeDashboard {
    constructor() {
        this.responses = [];
        this.captured = [];
        this.server = createServer((req, res) => {
            const chunks = [];
            req.on('data', (c) => chunks.push(c));
            req.on('end', () => {
                const raw = Buffer.concat(chunks).toString('utf8');
                let body;
                try { body = raw ? JSON.parse(raw) : {}; }
                catch { body = { _raw: raw }; }
                this.captured.push({
                    path: req.url,
                    headers: Object.fromEntries(
                        Object.entries(req.headers).map(([k, v]) => [k.toLowerCase(), v]),
                    ),
                    body,
                });
                const [status, resp] = this.responses.shift() || [500, { error: 'no_response' }];
                res.writeHead(status, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(resp));
            });
        });
    }
    async start() {
        await new Promise((r) => this.server.listen(0, '127.0.0.1', r));
        this.port = this.server.address().port;
    }
    async stop() {
        await new Promise((r) => this.server.close(r));
    }
}

function mockClient(queryResult) {
    const calls = [];
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], rowCount: 0 };
        },
        _calls: calls,
    };
}

function gl(dashboardPort) {
    const inst = new GoldLapel('postgresql://localhost:5432/mydb');
    inst._dashboardPort = dashboardPort;
    inst._dashboardToken = 'test-token';
    return inst;
}

const counterCreateBody = (table) => ({
    accepted: true,
    family: 'counter',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {
        // Phase 5: every UPDATE path stamps updated_at = NOW().
        incr: `INSERT INTO ${table} (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = ${table}.value + EXCLUDED.value, updated_at = NOW() RETURNING value`,
        set: `INSERT INTO ${table} (key, value, updated_at) VALUES ($1, $2, NOW()) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW() RETURNING value`,
        get: `SELECT value FROM ${table} WHERE key = $1`,
        delete: `DELETE FROM ${table} WHERE key = $1`,
        delete_all: `DELETE FROM ${table}`,
        count_keys: `SELECT COUNT(*) FROM ${table}`,
    },
});

describe('CountersAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.counters is a CountersAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.counters instanceof CountersAPI);
        assert.strictEqual(inst.counters._gl, inst);
    });

    it('Phase 5 hard cut — legacy flat methods are gone (no aliases)', () => {
        const inst = gl(dash.port);
        for (const legacy of ['incr', 'getCounter']) {
            assert.strictEqual(
                inst[legacy], undefined,
                `Phase 5 removed flat ${legacy} — use gl.counters.<verb> instead.`,
            );
        }
    });
});

describe('CountersAPI dispatches each verb to utils.counter*', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('incr fetches DDL then runs the proxy-supplied incr pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: '42' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_pageviews')]);

        const result = await inst.counters.incr('pageviews', 'home');
        assert.strictEqual(result, 42);

        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(dash.captured[0].path, '/api/ddl/counter/create');
        assert.deepEqual(dash.captured[0].body, { name: 'pageviews', schema_version: 'v1' });

        // Wire-level call uses the proxy-supplied pattern, NOT a wrapper-built
        // CREATE TABLE / INSERT pair.
        assert.strictEqual(conn._calls.length, 1);
        assert.ok(conn._calls[0].text.includes('INSERT INTO _goldlapel.counter_pageviews'));
        assert.ok(conn._calls[0].text.includes('updated_at = NOW()'));
        assert.deepEqual(conn._calls[0].values, ['home', 1]);
    });

    it('decr negates amount and reuses the incr pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: '-3' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.decr('x', 'k', 3);
        assert.strictEqual(result, -3);
        assert.deepEqual(conn._calls[0].values, ['k', -3]);
    });

    it('set runs the set pattern with [key, value]', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: '100' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.set('x', 'k', 100);
        assert.strictEqual(result, 100);
        assert.ok(conn._calls[0].text.includes('updated_at = NOW()'));
        assert.deepEqual(conn._calls[0].values, ['k', 100]);
    });

    it('get returns 0 for unknown key (Redis convention)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.get('x', 'missing');
        assert.strictEqual(result, 0);
    });

    it('delete returns true when row removed', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.delete('x', 'k');
        assert.strictEqual(result, true);
    });

    it('delete returns false when row absent', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.delete('x', 'k');
        assert.strictEqual(result, false);
    });

    it('countKeys runs count_keys pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '5' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        const result = await inst.counters.countKeys('x');
        assert.strictEqual(result, 5);
    });

    it('create eagerly materializes the table with no extra calls', async () => {
        const inst = gl(dash.port);
        const conn = mockClient();
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        await inst.counters.create('x');
        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(conn._calls.length, 0);
    });

    it('create with { unlogged: true } passes options through to the DDL POST', async () => {
        const inst = gl(dash.port);
        const conn = mockClient();
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        await inst.counters.create('x', { unlogged: true });
        assert.deepEqual(dash.captured[0].body, {
            name: 'x',
            schema_version: 'v1',
            options: { unlogged: true },
        });
    });
});

describe('CountersAPI per-session DDL cache', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('repeated calls against the same counter share one DDL fetch', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: '1' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, counterCreateBody('_goldlapel.counter_x')]);

        await inst.counters.incr('x', 'a');
        await inst.counters.incr('x', 'b');
        await inst.counters.set('x', 'c', 5);

        // One HTTP round-trip; three wire-level Postgres calls.
        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(conn._calls.length, 3);
    });
});

describe('counter utils SQL contract', () => {
    const fakePatterns = counterCreateBody('_goldlapel.counter_x');

    it('counterIncr binds [key, amount] in source order', async () => {
        const conn = mockClient({ rows: [{ value: '7' }], rowCount: 1 });
        const result = await counterIncr(conn, 'x', 'home', 5, { patterns: fakePatterns });
        assert.strictEqual(result, 7);
        assert.deepEqual(conn._calls[0].values, ['home', 5]);
        // No placeholder translation — pg binds $N natively.
        assert.ok(conn._calls[0].text.includes('$1'));
        assert.ok(conn._calls[0].text.includes('$2'));
    });

    it('counterSet uses the set pattern (not incr)', async () => {
        const conn = mockClient({ rows: [{ value: '100' }], rowCount: 1 });
        await counterSet(conn, 'x', 'k', 100, { patterns: fakePatterns });
        const sql = conn._calls[0].text;
        // The set pattern uses EXCLUDED.value (no `+`) — the incr pattern
        // would have `${table}.value +` instead.
        assert.ok(sql.includes('value = EXCLUDED.value'));
    });

    it('counterGet returns 0 for unknown key', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await counterGet(conn, 'x', 'missing', { patterns: fakePatterns });
        assert.strictEqual(result, 0);
    });

    it('counterDelete returns boolean from rowCount', async () => {
        const conn = mockClient({ rows: [], rowCount: 1 });
        assert.strictEqual(
            await counterDelete(conn, 'x', 'k', { patterns: fakePatterns }),
            true,
        );
    });

    it('counterCountKeys parses BIGINT-as-string from pg', async () => {
        const conn = mockClient({ rows: [{ count: '42' }], rowCount: 1 });
        const result = await counterCountKeys(conn, 'x', { patterns: fakePatterns });
        assert.strictEqual(result, 42);
    });

    it('throws when called without patterns (direct util misuse)', async () => {
        await assert.rejects(
            () => counterIncr(mockClient(), 'x', 'k', 1),
            /counter utils require/,
        );
    });

    it('Phase 5 incr pattern stamps updated_at on every UPDATE', () => {
        assert.ok(fakePatterns.query_patterns.incr.includes('updated_at = NOW()'));
        assert.ok(fakePatterns.query_patterns.set.includes('updated_at = NOW()'));
    });
});
