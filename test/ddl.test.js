// Unit tests for ddl.js — the DDL API client + per-session cache.
//
// Spins up a tiny HTTP server per test to capture requests and return canned
// responses. Mirrors the shape of tests/test_ddl.py in goldlapel-python.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    fetchPatterns,
    tokenFromEnvOrFile,
    supportedVersion,
    invalidate,
    _internals,
} from '../ddl.js';

class FakeServer {
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

describe('tokenFromEnvOrFile', () => {
    let savedEnv;
    beforeEach(() => { savedEnv = process.env.GOLDLAPEL_DASHBOARD_TOKEN; });
    afterEach(() => {
        if (savedEnv === undefined) delete process.env.GOLDLAPEL_DASHBOARD_TOKEN;
        else process.env.GOLDLAPEL_DASHBOARD_TOKEN = savedEnv;
    });

    it('returns env when set', () => {
        process.env.GOLDLAPEL_DASHBOARD_TOKEN = 'env-token';
        assert.equal(tokenFromEnvOrFile(), 'env-token');
    });

    it('trims whitespace from env', () => {
        process.env.GOLDLAPEL_DASHBOARD_TOKEN = '  trimmed  ';
        assert.equal(tokenFromEnvOrFile(), 'trimmed');
    });

    it('returns null when env is empty', () => {
        process.env.GOLDLAPEL_DASHBOARD_TOKEN = '';
        // Could be null if no file exists, or file contents — caller tolerates.
        const tok = tokenFromEnvOrFile();
        // Can be null (no file) or a real file-backed string. Both fine; what
        // we're asserting is that the empty env doesn't short-circuit to ''.
        assert.notEqual(tok, '');
    });
});

describe('supportedVersion', () => {
    it('stream is v1', () => {
        assert.equal(supportedVersion('stream'), 'v1');
    });
});

describe('fetchPatterns', () => {
    let srv;
    beforeEach(async () => { srv = new FakeServer(); await srv.start(); });
    afterEach(async () => { await srv.stop(); });

    it('happy path — hits /api/ddl/stream/create with correct headers + body', async () => {
        srv.responses.push([200, {
            accepted: true,
            family: 'stream',
            schema_version: 'v1',
            tables: { main: '_goldlapel.stream_events' },
            query_patterns: { insert: 'INSERT ...' },
        }]);
        const owner = {};
        const r = await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        assert.equal(r.tables.main, '_goldlapel.stream_events');
        assert.equal(r.query_patterns.insert, 'INSERT ...');

        assert.equal(srv.captured.length, 1);
        const cap = srv.captured[0];
        assert.equal(cap.path, '/api/ddl/stream/create');
        assert.equal(cap.headers['x-gl-dashboard'], 'tok');
        assert.deepEqual(cap.body, { name: 'events', schema_version: 'v1' });
    });

    it('cache hit — second call does not re-POST', async () => {
        srv.responses.push([200, {
            tables: { main: '_goldlapel.stream_events' },
            query_patterns: { insert: 'X' },
        }]);
        const owner = {};
        const r1 = await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        const r2 = await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        assert.equal(r1, r2);
        assert.equal(srv.captured.length, 1);
    });

    it('different names are cached independently', async () => {
        for (const n of ['events', 'orders']) {
            srv.responses.push([200, {
                tables: { main: `_goldlapel.stream_${n}` },
                query_patterns: { insert: `INSERT ${n}` },
            }]);
        }
        const owner = {};
        await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        await fetchPatterns(owner, 'stream', 'orders', srv.port, 'tok');
        assert.equal(srv.captured.length, 2);
    });

    it('different owners have isolated caches', async () => {
        for (let i = 0; i < 2; i++) {
            srv.responses.push([200, {
                tables: { main: '_goldlapel.stream_events' },
                query_patterns: { insert: 'X' },
            }]);
        }
        const a = {};
        const b = {};
        await fetchPatterns(a, 'stream', 'events', srv.port, 'tok');
        await fetchPatterns(b, 'stream', 'events', srv.port, 'tok');
        assert.equal(srv.captured.length, 2);
    });

    it('409 version_mismatch throws actionable error', async () => {
        srv.responses.push([409, {
            error: 'version_mismatch',
            detail: 'wrapper requested v1; proxy speaks v2 — upgrade proxy',
            requested: 'v1', canonical: 'v2',
        }]);
        await assert.rejects(
            () => fetchPatterns({}, 'stream', 'events', srv.port, 'tok'),
            /schema version mismatch/,
        );
    });

    it('403 throws token-specific error', async () => {
        srv.responses.push([403, { error: 'forbidden' }]);
        await assert.rejects(
            () => fetchPatterns({}, 'stream', 'events', srv.port, 'tok'),
            /dashboard token/,
        );
    });

    it('missing token throws before any HTTP', async () => {
        await assert.rejects(
            () => fetchPatterns({}, 'stream', 'events', 9999, null),
            /No dashboard token/,
        );
    });

    it('missing port throws before any HTTP', async () => {
        await assert.rejects(
            () => fetchPatterns({}, 'stream', 'events', null, 'tok'),
            /No dashboard port/,
        );
    });

    it('server unreachable produces actionable error', async () => {
        await assert.rejects(
            () => fetchPatterns({}, 'stream', 'events', 1, 'tok'),
            /dashboard not reachable/,
        );
    });

    it('invalidate drops cache', async () => {
        for (let i = 0; i < 2; i++) {
            srv.responses.push([200, {
                tables: { main: '_goldlapel.stream_events' },
                query_patterns: { insert: 'X' },
            }]);
        }
        const owner = {};
        await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        invalidate(owner);
        await fetchPatterns(owner, 'stream', 'events', srv.port, 'tok');
        assert.equal(srv.captured.length, 2);
    });
});
