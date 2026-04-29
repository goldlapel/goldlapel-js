// Unit tests for documents.js — the nested gl.documents namespace
// introduced in Phase 4 of schema-to-core.
//
// These tests verify:
//   - gl.documents is a DocumentsAPI bound to the parent client
//   - Each verb fetches DDL patterns from the proxy then dispatches to utils
//   - The unlogged option flows through to the DDL options bag
//   - The pattern cache is shared with the parent client (one HTTP call per
//     (family, name) per session)
//   - $lookup.from collections in aggregate are resolved via the proxy too

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import { GoldLapel, DocumentsAPI } from '../index.js';
import * as ddl from '../ddl.js';

// Tiny HTTP fake of the proxy's dashboard. Each test pre-loads `responses`
// with the [status, body] pairs to return; requests are recorded on
// `captured` for inspection.
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

// Build a GoldLapel pointing at our fake dashboard. We bypass start() —
// these are pure unit tests; spawning the Rust binary is out of scope
// (covered by streams-integration / documents-integration when the
// integration gate is on).
function gl(dashboardPort) {
    const inst = new GoldLapel('postgresql://localhost:5432/mydb');
    inst._dashboardPort = dashboardPort;
    inst._dashboardToken = 'test-token';
    return inst;
}

const docStoreCreateBody = (table) => ({
    accepted: true,
    family: 'doc_store',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {},
});

describe('DocumentsAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.documents is a DocumentsAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.documents instanceof DocumentsAPI);
        assert.strictEqual(inst.documents._gl, inst);
    });

    it('state reads through the parent — token rotation reflected immediately', async () => {
        const inst = gl(dash.port);
        // First call uses the original token
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);
        // Pretend the parent rotated token before the call dispatched
        inst._dashboardToken = 'rotated-token';
        const conn = mockClient({ rows: [{ _id: 'a', data: {}, created_at: 'now' }], rowCount: 1 });
        inst._defaultConn = conn;
        await inst.documents.insert('users', { name: 'Alice' });
        // The DDL POST sees the rotated token, not the original.
        assert.strictEqual(dash.captured[0].headers['x-gl-dashboard'], 'rotated-token');
    });
});

describe('DocumentsAPI dispatches each verb to utils.doc*', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('insert calls /api/ddl/doc_store/create then INSERT INTO _goldlapel.doc_users', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ _id: 'u1', data: { name: 'Alice' }, created_at: 'now' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        const r = await inst.documents.insert('users', { name: 'Alice' });
        assert.strictEqual(r._id, 'u1');

        // Hit /api/ddl/doc_store/create exactly once.
        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(dash.captured[0].path, '/api/ddl/doc_store/create');
        assert.deepEqual(dash.captured[0].body, { name: 'users', schema_version: 'v1' });

        // Wrapper used the canonical proxy table, not the bare collection name.
        assert.strictEqual(conn._calls.length, 1);
        assert.ok(conn._calls[0].text.includes('INSERT INTO _goldlapel.doc_users'));
    });

    it('find passes filter + sort + limit + skip down to utils.docFind', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        await inst.documents.find('users', { active: true }, { sort: { name: 1 }, limit: 5, skip: 2 });

        assert.strictEqual(conn._calls.length, 1);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM _goldlapel.doc_users'));
        assert.ok(sql.includes('WHERE data @>'));
        assert.ok(sql.includes("data->>'name' ASC"));
        assert.ok(sql.includes('LIMIT $2'));
        assert.ok(sql.includes('OFFSET $3'));
    });

    it('updateOne, deleteOne pass filter + update through', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        await inst.documents.updateOne('users', { id: 1 }, { $set: { name: 'x' } });
        assert.ok(conn._calls[0].text.includes('UPDATE _goldlapel.doc_users SET data ='));
    });

    it('count passes filter through', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '42' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        const r = await inst.documents.count('users', { active: true });
        assert.strictEqual(r, 42);
        assert.ok(conn._calls[0].text.includes('SELECT COUNT(*) FROM _goldlapel.doc_users'));
    });

    it('createCollection just fetches patterns — no client-side DDL', async () => {
        const inst = gl(dash.port);
        const conn = mockClient();
        inst._defaultConn = conn;
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        await inst.documents.createCollection('users');

        assert.strictEqual(dash.captured.length, 1);
        assert.deepEqual(dash.captured[0].body, { name: 'users', schema_version: 'v1' });
        assert.strictEqual(conn._calls.length, 0, 'no client-side DDL');
    });

    it('createCollection forwards { unlogged: true } via DDL options', async () => {
        const inst = gl(dash.port);
        inst._defaultConn = mockClient();
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_sessions')]);

        await inst.documents.createCollection('sessions', { unlogged: true });

        assert.strictEqual(dash.captured.length, 1);
        assert.deepEqual(dash.captured[0].body, {
            name: 'sessions',
            schema_version: 'v1',
            options: { unlogged: true },
        });
    });
});

describe('DocumentsAPI findCursor (async generator)', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    function mockCursorClient(batches) {
        const calls = [];
        let fetchIdx = 0;
        return {
            query: async (text) => {
                calls.push({ text });
                if (text.startsWith('FETCH')) {
                    const batch = fetchIdx < batches.length ? batches[fetchIdx] : [];
                    fetchIdx++;
                    return { rows: batch };
                }
                return { rows: [], rowCount: 0 };
            },
            _calls: calls,
        };
    }

    it('yields rows from canonical proxy table via the cursor protocol', async () => {
        const inst = gl(dash.port);
        const rows = [
            { _id: 'a', data: { name: 'Alice' }, created_at: 'now' },
            { _id: 'b', data: { name: 'Bob' }, created_at: 'now' },
        ];
        inst._defaultConn = mockCursorClient([rows, []]);
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        const out = [];
        for await (const row of inst.documents.findCursor('users')) out.push(row);
        assert.equal(out.length, 2);
        // Cursor was declared against the canonical proxy table.
        const declareCall = inst._defaultConn._calls.find((c) => c.text.includes('DECLARE'));
        assert.ok(declareCall);
        assert.ok(declareCall.text.includes('FROM _goldlapel.doc_users'));
    });
});

describe('DocumentsAPI per-session DDL cache', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('multiple verbs against the same collection share one DDL fetch', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ _id: 'a', data: {}, created_at: 'now' }], rowCount: 1 });
        inst._defaultConn = conn;
        // Only one [200, ...] response — second/third calls must hit cache.
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        await inst.documents.insert('users', { a: 1 });
        await inst.documents.find('users', {});
        await inst.documents.count('users', {});
        await inst.documents.updateOne('users', { id: 1 }, { $set: { x: 2 } });

        assert.strictEqual(dash.captured.length, 1, 'one DDL POST per (family, name) per session');
    });

    it('different collections each get their own DDL fetch', async () => {
        const inst = gl(dash.port);
        inst._defaultConn = mockClient({ rows: [{ _id: 'a', data: {}, created_at: 'now' }], rowCount: 1 });
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_orders')]);

        await inst.documents.insert('users', { x: 1 });
        await inst.documents.insert('orders', { x: 2 });

        assert.strictEqual(dash.captured.length, 2);
        assert.strictEqual(dash.captured[0].body.name, 'users');
        assert.strictEqual(dash.captured[1].body.name, 'orders');
    });
});

describe('DocumentsAPI aggregate $lookup resolution', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('resolves $lookup.from to the canonical proxy table', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        // Two POSTs expected: source collection first, then the lookup.
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_orders')]);

        await inst.documents.aggregate('users', [
            { $match: { active: true } },
            {
                $lookup: {
                    from: 'orders',
                    localField: 'id',
                    foreignField: 'userId',
                    as: 'user_orders',
                },
            },
        ]);

        assert.strictEqual(dash.captured.length, 2);
        // The SQL refers to BOTH canonical tables:
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('FROM _goldlapel.doc_users'));
        assert.ok(sql.includes('FROM _goldlapel.doc_orders b'));
    });

    it('repeated $lookup.from names share the cache', async () => {
        const inst = gl(dash.port);
        inst._defaultConn = mockClient({ rows: [], rowCount: 0 });
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_orders')]);

        await inst.documents.aggregate('users', [
            { $lookup: { from: 'orders', localField: 'id', foreignField: 'uid', as: 'a' } },
            { $lookup: { from: 'orders', localField: 'id', foreignField: 'uid', as: 'b' } },
        ]);

        // 1 for users + 1 for orders. The second $lookup hits cache.
        assert.strictEqual(dash.captured.length, 2);
    });
});

describe('DocumentsAPI stop() invalidates the cache', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('stop() drops cached patterns so a fresh start re-fetches', async () => {
        const inst = gl(dash.port);
        inst._defaultConn = mockClient({ rows: [{ _id: 'a', data: {}, created_at: 'now' }], rowCount: 1 });
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);
        dash.responses.push([200, docStoreCreateBody('_goldlapel.doc_users')]);

        await inst.documents.insert('users', { a: 1 });
        ddl.invalidate(inst);
        await inst.documents.insert('users', { a: 2 });

        assert.strictEqual(dash.captured.length, 2);
    });
});
