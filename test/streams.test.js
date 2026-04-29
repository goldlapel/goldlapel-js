// Unit tests for streams.js — the nested gl.streams namespace introduced
// alongside Phase 4 of schema-to-core.
//
// (Streams DDL ownership shipped earlier in Phase 1+2; the namespace
// nesting restructure is the new piece here.)

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import { GoldLapel, StreamsAPI } from '../index.js';

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

const streamCreateBody = (table) => ({
    accepted: true,
    family: 'stream',
    schema_version: 'v1',
    tables: { main: table, groups: `${table}_groups`, pending: `${table}_pending` },
    query_patterns: {
        insert: `INSERT INTO ${table} (payload) VALUES ($1) RETURNING id, created_at`,
        create_group: `INSERT INTO ${table}_groups (name) VALUES ($1) ON CONFLICT DO NOTHING`,
        group_get_cursor: `SELECT last_delivered_id FROM ${table}_groups WHERE name = $1 FOR UPDATE`,
        read_since: `SELECT id, payload, created_at FROM ${table} WHERE id > $1 ORDER BY id LIMIT $2`,
        group_advance_cursor: `UPDATE ${table}_groups SET last_delivered_id = $1 WHERE name = $2`,
        pending_insert: `INSERT INTO ${table}_pending (message_id, group_name, consumer) VALUES ($1, $2, $3)`,
        ack: `DELETE FROM ${table}_pending WHERE group_name = $1 AND message_id = $2`,
        claim: `UPDATE ${table}_pending SET consumer = $1, claimed_at = NOW() WHERE group_name = $2 AND claimed_at < NOW() - ($3 || ' ms')::interval RETURNING message_id`,
        read_by_id: `SELECT id, payload, created_at FROM ${table} WHERE id = $1`,
    },
});

describe('StreamsAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.streams is a StreamsAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.streams instanceof StreamsAPI);
        assert.strictEqual(inst.streams._gl, inst);
    });

    it('legacy flat stream* methods are gone (hard cut, no aliases)', () => {
        const inst = gl(dash.port);
        for (const legacy of ['streamAdd', 'streamCreateGroup', 'streamRead', 'streamAck', 'streamClaim']) {
            assert.strictEqual(
                inst[legacy], undefined,
                `Legacy flat method ${legacy} should have been removed; use gl.streams.<verb> instead.`,
            );
        }
    });
});

describe('StreamsAPI dispatches each verb to utils.stream*', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('add fetches DDL then runs the proxy-supplied insert pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ id: '42' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, streamCreateBody('_goldlapel.stream_events')]);

        const id = await inst.streams.add('events', { type: 'click' });
        assert.strictEqual(id, 42);

        // DDL fetch
        assert.strictEqual(dash.captured.length, 1);
        assert.strictEqual(dash.captured[0].path, '/api/ddl/stream/create');
        assert.deepEqual(dash.captured[0].body, { name: 'events', schema_version: 'v1' });

        // Wire-level call uses proxy-supplied pattern.
        assert.strictEqual(conn._calls.length, 1);
        assert.ok(conn._calls[0].text.includes('INSERT INTO _goldlapel.stream_events'));
    });

    it('createGroup runs the create_group pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient();
        inst._defaultConn = conn;
        dash.responses.push([200, streamCreateBody('_goldlapel.stream_events')]);

        await inst.streams.createGroup('events', 'workers');
        assert.strictEqual(conn._calls.length, 1);
        assert.ok(conn._calls[0].text.includes('INSERT INTO _goldlapel.stream_events_groups'));
        assert.deepEqual(conn._calls[0].values, ['workers']);
    });

    it('ack passes group + messageId to the ack pattern', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, streamCreateBody('_goldlapel.stream_events')]);

        const removed = await inst.streams.ack('events', 'workers', 42);
        assert.strictEqual(removed, true);
        assert.deepEqual(conn._calls[0].values, ['workers', 42]);
    });
});

describe('StreamsAPI per-session DDL cache', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('repeated calls against the same stream share one DDL fetch', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ id: '1' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, streamCreateBody('_goldlapel.stream_events')]);

        await inst.streams.add('events', { i: 1 });
        await inst.streams.add('events', { i: 2 });
        await inst.streams.createGroup('events', 'workers');

        assert.strictEqual(dash.captured.length, 1);
    });
});
