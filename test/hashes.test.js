// Unit tests for hashes.js — the nested gl.hashes namespace introduced in
// Phase 5 of schema-to-core.
//
// Phase 5 flipped the hash storage shape from "JSONB blob per key" to
// "row per (hash_key, field)". These tests verify:
//   - The wrapper executes single-row UPSERT for `set`, NOT load-merge-save.
//   - `getAll` aggregates rows from the proxy into a JS object.
//   - `keys` / `values` return per-row sequences (not blob extraction).
//   - `delete` returns true/false from rowCount, not from a JSONB key probe.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    GoldLapel, HashesAPI,
    hashSet, hashGet, hashGetAll, hashKeys, hashValues,
    hashExists, hashDelete, hashLen,
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

const hashCreateBody = (table) => ({
    accepted: true,
    family: 'hash',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {
        // Phase 5: row-per-field, NOT a JSONB-blob.
        hset: `INSERT INTO ${table} (hash_key, field, value) VALUES ($1, $2, $3::jsonb) ON CONFLICT (hash_key, field) DO UPDATE SET value = EXCLUDED.value RETURNING value`,
        hget: `SELECT value FROM ${table} WHERE hash_key = $1 AND field = $2`,
        hgetall: `SELECT field, value FROM ${table} WHERE hash_key = $1 ORDER BY field`,
        hkeys: `SELECT field FROM ${table} WHERE hash_key = $1 ORDER BY field`,
        hvals: `SELECT value FROM ${table} WHERE hash_key = $1 ORDER BY field`,
        hexists: `SELECT EXISTS (SELECT 1 FROM ${table} WHERE hash_key = $1 AND field = $2)`,
        hdel: `DELETE FROM ${table} WHERE hash_key = $1 AND field = $2`,
        hlen: `SELECT COUNT(*) FROM ${table} WHERE hash_key = $1`,
        delete_key: `DELETE FROM ${table} WHERE hash_key = $1`,
        delete_all: `DELETE FROM ${table}`,
    },
});

describe('HashesAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.hashes is a HashesAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.hashes instanceof HashesAPI);
        assert.strictEqual(inst.hashes._gl, inst);
    });

    it('Phase 5 hard cut — legacy flat hash methods are gone', () => {
        const inst = gl(dash.port);
        for (const legacy of ['hset', 'hget', 'hgetall', 'hdel']) {
            assert.strictEqual(inst[legacy], undefined);
        }
    });
});

describe('HashesAPI threads hashKey through every method', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('set runs a single-row UPSERT against the proxy pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: 'alice' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_sessions')]);

        await inst.hashes.set('sessions', 'user:1', 'name', 'alice');
        // Single round-trip — no load-merge-save sequence.
        assert.strictEqual(conn._calls.length, 1);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('INSERT INTO _goldlapel.hash_sessions'));
        assert.ok(sql.includes('ON CONFLICT (hash_key, field)'));
        // value is JSON-encoded so non-string values round-trip.
        assert.deepEqual(conn._calls[0].values, ['user:1', 'name', '"alice"']);
    });

    it('set JSON-encodes structured values', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ value: { a: 1 } }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        await inst.hashes.set('x', 'k', 'data', { a: 1 });
        assert.strictEqual(conn._calls[0].values[2], '{"a":1}');
    });

    it('getAll rebuilds a JS object from row-per-field results', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [
                { field: 'email', value: 'a@x' },
                { field: 'name', value: 'alice' },
            ],
            rowCount: 2,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        const result = await inst.hashes.getAll('x', 'user:1');
        assert.deepEqual(result, { email: 'a@x', name: 'alice' });
    });

    it('getAll decodes string-encoded JSONB payloads', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ field: 'data', value: '{"k": 1}' }],
            rowCount: 1,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        const result = await inst.hashes.getAll('x', 'user:1');
        assert.deepEqual(result, { data: { k: 1 } });
    });

    it('get returns null for absent field', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        const result = await inst.hashes.get('x', 'user:1', 'missing');
        assert.strictEqual(result, null);
    });

    it('keys returns field names', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ field: 'name' }, { field: 'email' }],
            rowCount: 2,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        const result = await inst.hashes.keys('x', 'user:1');
        assert.deepEqual(result, ['name', 'email']);
    });

    it('exists returns boolean from EXISTS()', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ exists: true }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        assert.strictEqual(await inst.hashes.exists('x', 'user:1', 'name'), true);
    });

    it('delete returns true when row removed', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        assert.strictEqual(await inst.hashes.delete('x', 'user:1', 'name'), true);
    });

    it('len reads COUNT(*)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '3' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, hashCreateBody('_goldlapel.hash_x')]);

        assert.strictEqual(await inst.hashes.len('x', 'user:1'), 3);
    });
});

describe('hash utils SQL contract', () => {
    const fakePatterns = hashCreateBody('_goldlapel.hash_x');

    it('hashSet is a single round-trip (not load-merge-save)', async () => {
        const conn = mockClient({ rows: [{ value: 'alice' }], rowCount: 1 });
        await hashSet(conn, 'x', 'user:1', 'name', 'alice', { patterns: fakePatterns });
        assert.strictEqual(conn._calls.length, 1);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('INSERT INTO'));
        assert.ok(sql.includes('ON CONFLICT (hash_key, field)'));
        // Phase 5 contract: NO jsonb_build_object on the wrapper-side
        // (legacy load-merge-save artefact).
        assert.ok(!sql.includes('jsonb_build_object'));
    });

    it('hashSet JSON-encodes the value param', async () => {
        const conn = mockClient({ rows: [{ value: { a: 1 } }] });
        await hashSet(conn, 'x', 'user:1', 'data', { a: 1 }, { patterns: fakePatterns });
        const params = conn._calls[0].values;
        assert.strictEqual(params[0], 'user:1');
        assert.strictEqual(params[1], 'data');
        assert.strictEqual(params[2], '{"a":1}');
    });

    it('hashGetAll rebuilds dict from rows', async () => {
        const conn = mockClient({
            rows: [
                { field: 'email', value: 'a@x' },
                { field: 'name', value: 'alice' },
            ],
        });
        const result = await hashGetAll(conn, 'x', 'user:1', { patterns: fakePatterns });
        assert.deepEqual(result, { email: 'a@x', name: 'alice' });
    });

    it('hashGet returns null for absent', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await hashGet(conn, 'x', 'user:1', 'missing', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('hashKeys returns array of field names', async () => {
        const conn = mockClient({ rows: [{ field: 'a' }, { field: 'b' }] });
        const result = await hashKeys(conn, 'x', 'user:1', { patterns: fakePatterns });
        assert.deepEqual(result, ['a', 'b']);
    });

    it('hashValues decodes JSONB payloads', async () => {
        const conn = mockClient({ rows: [{ value: '"alice"' }, { value: 42 }] });
        const result = await hashValues(conn, 'x', 'user:1', { patterns: fakePatterns });
        assert.deepEqual(result, ['alice', 42]);
    });

    it('hashExists returns false when row is missing entirely', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await hashExists(conn, 'x', 'user:1', 'f', { patterns: fakePatterns });
        assert.strictEqual(result, false);
    });

    it('hashDelete returns boolean from rowCount (not a JSONB probe)', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        assert.strictEqual(
            await hashDelete(conn, 'x', 'user:1', 'f', { patterns: fakePatterns }),
            false,
        );
    });

    it('hashLen returns 0 when empty', async () => {
        const conn = mockClient({ rows: [{ count: '0' }] });
        assert.strictEqual(await hashLen(conn, 'x', 'user:1', { patterns: fakePatterns }), 0);
    });

    it('Phase 5 canonical pattern is row-per-field, not a blob', () => {
        const sql = fakePatterns.query_patterns.hset;
        assert.ok(sql.includes('(hash_key, field, value)'));
        assert.ok(!sql.includes('jsonb_build_object'));
    });

    it('throws when called without patterns', async () => {
        await assert.rejects(
            () => hashSet(mockClient(), 'x', 'k', 'f', 'v'),
            /hash utils require/,
        );
    });
});
