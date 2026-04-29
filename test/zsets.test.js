// Unit tests for zsets.js — the nested gl.zsets namespace introduced in
// Phase 5 of schema-to-core.
//
// Phase 5 introduced a `zset_key` column in the canonical schema so a single
// namespace table holds many sorted sets. These tests verify:
//   - `zsetKey` threads through every method as the first positional arg
//     after the namespace `name` (matching Redis ZADD signatures).
//   - Pattern selection picks `zrange_asc` vs `zrange_desc` based on the
//     `desc` flag.
//   - Range/limit translation is Redis-inclusive (start..stop inclusive).
//   - SQL builders bind in `(zset_key, member, score)` order matching the
//     proxy's `$1, $2, $3` template.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    GoldLapel, ZsetsAPI,
    zsetAdd, zsetIncrBy, zsetScore, zsetRank,
    zsetRange, zsetRangeByScore, zsetRemove, zsetCard,
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
                try { body = raw ? JSON.parse(raw) : {}; } catch { body = {}; }
                this.captured.push({ path: req.url, body });
                const [status, resp] = this.responses.shift() || [500, { error: 'no_response' }];
                res.writeHead(status, { 'Content-Type': 'application/json' });
                res.end(JSON.stringify(resp));
            });
        });
    }
    async start() { await new Promise((r) => this.server.listen(0, '127.0.0.1', r)); this.port = this.server.address().port; }
    async stop() { await new Promise((r) => this.server.close(r)); }
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

const zsetCreateBody = (table) => ({
    accepted: true,
    family: 'zset',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {
        zadd: `INSERT INTO ${table} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = EXCLUDED.score RETURNING score`,
        zincrby: `INSERT INTO ${table} (zset_key, member, score) VALUES ($1, $2, $3) ON CONFLICT (zset_key, member) DO UPDATE SET score = ${table}.score + EXCLUDED.score RETURNING score`,
        zscore: `SELECT score FROM ${table} WHERE zset_key = $1 AND member = $2`,
        zrem: `DELETE FROM ${table} WHERE zset_key = $1 AND member = $2`,
        zrange_asc: `SELECT member, score FROM ${table} WHERE zset_key = $1 ORDER BY score ASC, member ASC LIMIT $2 OFFSET $3`,
        zrange_desc: `SELECT member, score FROM ${table} WHERE zset_key = $1 ORDER BY score DESC, member DESC LIMIT $2 OFFSET $3`,
        zrangebyscore: `SELECT member, score FROM ${table} WHERE zset_key = $1 AND score >= $2 AND score <= $3 ORDER BY score ASC, member ASC LIMIT $4 OFFSET $5`,
        zrank_asc: `SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score ASC, member ASC) - 1 AS rank FROM ${table} WHERE zset_key = $1 ) ranked WHERE member = $2`,
        zrank_desc: `SELECT rank FROM ( SELECT member, ROW_NUMBER() OVER (ORDER BY score DESC, member DESC) - 1 AS rank FROM ${table} WHERE zset_key = $1 ) ranked WHERE member = $2`,
        zcard: `SELECT COUNT(*) FROM ${table} WHERE zset_key = $1`,
        delete_key: `DELETE FROM ${table} WHERE zset_key = $1`,
        delete_all: `DELETE FROM ${table}`,
    },
});

describe('ZsetsAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.zsets is a ZsetsAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.zsets instanceof ZsetsAPI);
        assert.strictEqual(inst.zsets._gl, inst);
    });

    it('Phase 5 hard cut — legacy flat zset methods are gone', () => {
        const inst = gl(dash.port);
        for (const legacy of ['zadd', 'zincrby', 'zrange', 'zrank', 'zscore', 'zrem']) {
            assert.strictEqual(inst[legacy], undefined);
        }
    });
});

describe('ZsetsAPI threads zsetKey through every method', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('add binds [zsetKey, member, score]', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ score: 100 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        const result = await inst.zsets.add('lb', 'global', 'alice', 100);
        assert.strictEqual(result, 100);
        assert.deepEqual(conn._calls[0].values, ['global', 'alice', 100]);
        assert.ok(conn._calls[0].text.includes('INSERT INTO _goldlapel.zset_lb'));
    });

    it('incrBy passes delta', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ score: 110 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        const result = await inst.zsets.incrBy('lb', 'global', 'alice', 10);
        assert.strictEqual(result, 110);
        assert.deepEqual(conn._calls[0].values, ['global', 'alice', 10]);
    });

    it('score returns null for unknown member', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        const result = await inst.zsets.score('lb', 'global', 'unknown');
        assert.strictEqual(result, null);
    });

    it('rank picks desc pattern by default', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ rank: 0 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.rank('lb', 'global', 'alice');
        assert.ok(conn._calls[0].text.includes('ORDER BY score DESC'));
    });

    it('rank picks asc pattern when desc=false', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ rank: 0 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.rank('lb', 'global', 'alice', { desc: false });
        assert.ok(conn._calls[0].text.includes('ORDER BY score ASC'));
    });

    it('range translates Redis-inclusive [start, stop] to LIMIT/OFFSET', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.range('lb', 'global', { start: 0, stop: 9 });
        // LIMIT = stop - start + 1 = 10; OFFSET = start = 0
        assert.deepEqual(conn._calls[0].values, ['global', 10, 0]);
    });

    it('range with stop=-1 maps to a large sentinel limit', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.range('lb', 'global');  // defaults: start=0, stop=-1
        assert.deepEqual(conn._calls[0].values, ['global', 10000, 0]);
    });

    it('rangeByScore binds [zsetKey, min, max, limit, offset]', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.rangeByScore('lb', 'global', 50, 200, { limit: 10, offset: 2 });
        assert.deepEqual(conn._calls[0].values, ['global', 50, 200, 10, 2]);
    });

    it('remove returns true on rowCount=1', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        assert.strictEqual(await inst.zsets.remove('lb', 'global', 'alice'), true);
    });

    it('card returns 0 for unknown key', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '0' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        assert.strictEqual(await inst.zsets.card('lb', 'unknown'), 0);
    });
});

describe('ZsetsAPI per-session DDL cache', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('many calls against the same namespace share one DDL fetch', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ score: 1 }, { count: '0' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, zsetCreateBody('_goldlapel.zset_lb')]);

        await inst.zsets.add('lb', 'k1', 'alice', 1);
        await inst.zsets.add('lb', 'k2', 'bob', 2);
        await inst.zsets.card('lb', 'k1');

        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(conn._calls.length, 3);
    });
});

describe('zset utils SQL contract', () => {
    const fakePatterns = zsetCreateBody('_goldlapel.zset_lb');

    it('zsetAdd binds in [zsetKey, member, score] order', async () => {
        const conn = mockClient({ rows: [{ score: 100 }], rowCount: 1 });
        const result = await zsetAdd(conn, 'lb', 'global', 'alice', 100, { patterns: fakePatterns });
        assert.strictEqual(result, 100);
        assert.deepEqual(conn._calls[0].values, ['global', 'alice', 100]);
    });

    it('zsetRange picks desc pattern when desc=true', async () => {
        const conn = mockClient({ rows: [{ member: 'alice', score: 100 }] });
        await zsetRange(conn, 'lb', 'global', 0, 1, true, { patterns: fakePatterns });
        assert.ok(conn._calls[0].text.includes('DESC'));
    });

    it('zsetRange picks asc pattern when desc=false', async () => {
        const conn = mockClient({ rows: [] });
        await zsetRange(conn, 'lb', 'global', 0, 5, false, { patterns: fakePatterns });
        assert.ok(conn._calls[0].text.includes('ORDER BY score ASC'));
    });

    it('zsetRange translates inclusive stop to LIMIT', async () => {
        const conn = mockClient({ rows: [] });
        await zsetRange(conn, 'lb', 'global', 0, 9, true, { patterns: fakePatterns });
        assert.deepEqual(conn._calls[0].values, ['global', 10, 0]);
    });

    it('zsetRangeByScore inclusive bounds', async () => {
        const conn = mockClient({ rows: [] });
        await zsetRangeByScore(conn, 'lb', 'global', 50, 200, 10, 2, { patterns: fakePatterns });
        assert.deepEqual(conn._calls[0].values, ['global', 50, 200, 10, 2]);
    });

    it('zsetIncrBy returns numeric score', async () => {
        const conn = mockClient({ rows: [{ score: '15.5' }], rowCount: 1 });
        const result = await zsetIncrBy(conn, 'lb', 'global', 'alice', 5, { patterns: fakePatterns });
        assert.strictEqual(result, 15.5);
    });

    it('zsetScore returns null when absent', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await zsetScore(conn, 'lb', 'global', 'missing', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('zsetRemove returns false when absent', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await zsetRemove(conn, 'lb', 'global', 'alice', { patterns: fakePatterns });
        assert.strictEqual(result, false);
    });

    it('zsetRank returns null when absent', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await zsetRank(conn, 'lb', 'global', 'absent', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('zsetCard returns 0 when empty', async () => {
        const conn = mockClient({ rows: [{ count: '0' }], rowCount: 1 });
        const result = await zsetCard(conn, 'lb', 'global', { patterns: fakePatterns });
        assert.strictEqual(result, 0);
    });

    it('throws when called without patterns', async () => {
        await assert.rejects(
            () => zsetAdd(mockClient(), 'lb', 'k', 'm', 1),
            /zset utils require/,
        );
    });
});
