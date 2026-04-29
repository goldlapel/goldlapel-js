// Unit tests for queues.js — the nested gl.queues namespace introduced in
// Phase 5 of schema-to-core.
//
// Phase 5 introduces at-least-once delivery with visibility-timeout. The
// breaking change is `dequeue` (delete-on-fetch) → `claim` (lease + ack).
// These tests verify:
//   - `enqueue` returns the assigned id from the proxy's RETURNING clause.
//   - `claim` returns `{ id, payload }` or null — explicit object shape.
//   - `ack` is a separate call, NOT bundled into claim.
//   - `abandon` / `nack` releases the claim immediately.
//   - `extend` pushes the visibility deadline.
//   - There is NO `dequeue` compat alias.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    GoldLapel, QueuesAPI,
    queueEnqueue, queueClaim, queueAck, queueAbandon,
    queueExtend, queuePeek, queueCountReady, queueCountClaimed,
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

const queueCreateBody = (table) => ({
    accepted: true,
    family: 'queue',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {
        enqueue: `INSERT INTO ${table} (payload) VALUES ($1::jsonb) RETURNING id, created_at`,
        claim: `WITH next_msg AS ( SELECT id FROM ${table} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id FOR UPDATE SKIP LOCKED LIMIT 1 ) UPDATE ${table} SET status = 'claimed', visible_at = NOW() + INTERVAL '1 millisecond' * $1 FROM next_msg WHERE ${table}.id = next_msg.id RETURNING ${table}.id, ${table}.payload, ${table}.visible_at, ${table}.created_at`,
        ack: `DELETE FROM ${table} WHERE id = $1`,
        extend: `UPDATE ${table} SET visible_at = visible_at + INTERVAL '1 millisecond' * $2 WHERE id = $1 AND status = 'claimed' RETURNING visible_at`,
        nack: `UPDATE ${table} SET status = 'ready', visible_at = NOW() WHERE id = $1 AND status = 'claimed' RETURNING id`,
        peek: `SELECT id, payload, visible_at, status, created_at FROM ${table} WHERE status = 'ready' AND visible_at <= NOW() ORDER BY visible_at, id LIMIT 1`,
        count_ready: `SELECT COUNT(*) FROM ${table} WHERE status = 'ready' AND visible_at <= NOW()`,
        count_claimed: `SELECT COUNT(*) FROM ${table} WHERE status = 'claimed'`,
        delete_all: `DELETE FROM ${table}`,
    },
});

describe('QueuesAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.queues is a QueuesAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.queues instanceof QueuesAPI);
        assert.strictEqual(inst.queues._gl, inst);
    });

    it('Phase 5 hard cut — flat enqueue/dequeue gone, no aliases anywhere', () => {
        const inst = gl(dash.port);
        // Flat methods on the client are gone.
        assert.strictEqual(inst.enqueue, undefined);
        assert.strictEqual(inst.dequeue, undefined);
        // And critically, NO `dequeue` alias on the queues namespace —
        // master-plan rule. claim+ack must be explicit.
        assert.strictEqual(inst.queues.dequeue, undefined,
            'No dequeue alias on QueuesAPI — claim+ack is explicit by design.');
    });
});

describe('QueuesAPI claim+ack flow', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('enqueue returns the assigned id', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ id: '42', created_at: '2026' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const id = await inst.queues.enqueue('jobs', { work: 'foo' });
        assert.strictEqual(id, 42);
        assert.deepEqual(conn._calls[0].values, ['{"work":"foo"}']);
    });

    it('claim returns { id, payload } object', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ id: '7', payload: { x: 1 }, visible_at: 'vat', created_at: 'cat' }],
            rowCount: 1,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const msg = await inst.queues.claim('jobs', 60000);
        assert.deepEqual(msg, { id: 7, payload: { x: 1 } });
        assert.deepEqual(conn._calls[0].values, [60000]);
    });

    it('claim decodes string-encoded JSONB payload', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ id: '7', payload: '{"x":1}', visible_at: null, created_at: null }],
            rowCount: 1,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const msg = await inst.queues.claim('jobs');
        assert.deepEqual(msg, { id: 7, payload: { x: 1 } });
    });

    it('claim returns null when queue is empty', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const msg = await inst.queues.claim('jobs');
        assert.strictEqual(msg, null);
    });

    it('claim runs ONE statement (no DELETE — lease, not delete-on-fetch)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ id: '7', payload: {}, visible_at: null, created_at: null }],
            rowCount: 1,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        await inst.queues.claim('jobs');
        assert.strictEqual(conn._calls.length, 1);
        const sql = conn._calls[0].text;
        // Phase 5 claim is an UPDATE — never a DELETE (would lose work
        // on consumer crash).
        assert.ok(!sql.toUpperCase().includes('DELETE'));
    });

    it('ack returns true when row removed', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        assert.strictEqual(await inst.queues.ack('jobs', 42), true);
        assert.deepEqual(conn._calls[0].values, [42]);
    });

    it('ack returns false when id unknown', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        assert.strictEqual(await inst.queues.ack('jobs', 999), false);
    });

    it('abandon uses the nack pattern (status=ready)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ id: '42' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const released = await inst.queues.abandon('jobs', 42);
        assert.strictEqual(released, true);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes("status = 'ready'"));
    });

    it('extend binds [messageId, additionalMs] (proxy SQL: $1=id, $2=additional_ms)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ visible_at: '2026' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const result = await inst.queues.extend('jobs', 42, 5000);
        assert.strictEqual(result, '2026');
        // Source-of-truth: the proxy SQL uses `WHERE id = $1` and
        // `INTERVAL ... * $2`. With pg's native $N binding, params[0]=id,
        // params[1]=additionalMs.
        assert.deepEqual(conn._calls[0].values, [42, 5000]);
    });

    it('peek returns shaped object', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({
            rows: [{ id: '42', payload: { work: 'foo' }, visible_at: 'vat', status: 'ready', created_at: 'cat' }],
            rowCount: 1,
        });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        const result = await inst.queues.peek('jobs');
        assert.deepEqual(result, {
            id: 42,
            payload: { work: 'foo' },
            visible_at: 'vat',
            status: 'ready',
            created_at: 'cat',
        });
    });

    it('countReady reads COUNT(*)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '5' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, queueCreateBody('_goldlapel.queue_jobs')]);

        assert.strictEqual(await inst.queues.countReady('jobs'), 5);
    });
});

describe('queue utils SQL contract', () => {
    const fakePatterns = queueCreateBody('_goldlapel.queue_jobs');

    it('queueEnqueue runs INSERT … RETURNING id', async () => {
        const conn = mockClient({ rows: [{ id: '99', created_at: '2026' }], rowCount: 1 });
        const id = await queueEnqueue(conn, 'jobs', { x: 1 }, { patterns: fakePatterns });
        assert.strictEqual(id, 99);
        assert.deepEqual(conn._calls[0].values, ['{"x":1}']);
    });

    it('queueClaim returns { id, payload } object', async () => {
        const conn = mockClient({
            rows: [{ id: '7', payload: { x: 1 }, visible_at: null, created_at: null }],
            rowCount: 1,
        });
        const result = await queueClaim(conn, 'jobs', 30000, { patterns: fakePatterns });
        assert.deepEqual(result, { id: 7, payload: { x: 1 } });
    });

    it('queueClaim returns null when empty', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await queueClaim(conn, 'jobs', 30000, { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('queueAck returns true when deleted, false when absent', async () => {
        let conn = mockClient({ rows: [], rowCount: 1 });
        assert.strictEqual(
            await queueAck(conn, 'jobs', 42, { patterns: fakePatterns }),
            true,
        );
        conn = mockClient({ rows: [], rowCount: 0 });
        assert.strictEqual(
            await queueAck(conn, 'jobs', 999, { patterns: fakePatterns }),
            false,
        );
    });

    it('queueAbandon → uses nack pattern → status=ready', async () => {
        const conn = mockClient({ rows: [{ id: '42' }], rowCount: 1 });
        const released = await queueAbandon(conn, 'jobs', 42, { patterns: fakePatterns });
        assert.strictEqual(released, true);
        assert.ok(conn._calls[0].text.includes("status = 'ready'"));
    });

    it('queueExtend returns the new visible_at', async () => {
        const conn = mockClient({ rows: [{ visible_at: '2026-05-01' }], rowCount: 1 });
        const result = await queueExtend(conn, 'jobs', 42, 5000, { patterns: fakePatterns });
        assert.strictEqual(result, '2026-05-01');
        assert.deepEqual(conn._calls[0].values, [42, 5000]);
    });

    it('queuePeek returns null when nothing is ready', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await queuePeek(conn, 'jobs', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('queueCountReady reads BIGINT count from pg', async () => {
        const conn = mockClient({ rows: [{ count: '100' }], rowCount: 1 });
        assert.strictEqual(
            await queueCountReady(conn, 'jobs', { patterns: fakePatterns }),
            100,
        );
    });

    it('queueCountClaimed reads BIGINT count from pg', async () => {
        const conn = mockClient({ rows: [{ count: '7' }], rowCount: 1 });
        assert.strictEqual(
            await queueCountClaimed(conn, 'jobs', { patterns: fakePatterns }),
            7,
        );
    });

    it('Phase 5: claim is a single UPDATE (no DELETE)', () => {
        const sql = fakePatterns.query_patterns.claim;
        assert.ok(!sql.toUpperCase().includes('DELETE'),
            'Phase 5 claim must be UPDATE, not DELETE — DELETE = at-most-once = lossy.');
    });

    it('throws when called without patterns', async () => {
        await assert.rejects(
            () => queueEnqueue(mockClient(), 'jobs', {}),
            /queue utils require/,
        );
    });
});
