// End-to-end streams integration test — proxy-owned DDL (Phase 2).
//
// Gated on GOLDLAPEL_INTEGRATION=1 + GOLDLAPEL_TEST_UPSTREAM — the
// standardized integration-test convention shared across all Gold Lapel
// wrappers. See test/_integration-gate.js.
//
// Set:
//   GOLDLAPEL_INTEGRATION=1
//   GOLDLAPEL_TEST_UPSTREAM=postgresql://postgres@localhost:5432/postgres
//   GOLDLAPEL_BINARY=/path/to/goldlapel
//
// Without GOLDLAPEL_INTEGRATION=1, the whole file is skipped so ordinary
// `npm test` runs remain hermetic.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

import { integrationGate } from './_integration-gate.js';

const gate = integrationGate();

if (gate.failReason) {
    // Half-configured CI — fail loudly to prevent false-green.
    describe('streams integration (misconfigured)', () => {
        it('fails when GOLDLAPEL_INTEGRATION=1 but GOLDLAPEL_TEST_UPSTREAM missing', () => {
            throw new Error(gate.failReason);
        });
    });
} else if (!gate.shouldRun) {
    describe('streams integration (skipped)', { skip: gate.skipReason }, () => {
        it('skipped', () => {});
    });
} else {

const PG_URL = gate.upstream;

describe('stream DDL ownership (proxy-owned tables)', () => {
    let goldlapel, gl, pg, streamName;

    before(async () => {
        goldlapel = await import('../index.js');
        // Use a high port to avoid collisions with default installs.
        const port = 7700 + (Date.now() % 100);
        gl = await goldlapel.start(PG_URL, { port });
    });

    after(async () => {
        if (gl) await gl.stop();
    });

    it('stream_add creates _goldlapel.stream_<name> table', async () => {
        streamName = `gl_int_stream_${Date.now()}`;
        await gl.streamAdd(streamName, { type: 'click' });

        // Open a direct pg connection to verify table layout.
        const { Client } = await import('pg');
        pg = new Client({ connectionString: PG_URL });
        await pg.connect();
        try {
            const r = await pg.query(
                `SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = '_goldlapel' AND table_name = $1`,
                [`stream_${streamName}`],
            );
            assert.equal(Number(r.rows[0].count), 1, 'expected _goldlapel.stream_<name>');

            // And nothing in public.
            const r2 = await pg.query(
                `SELECT COUNT(*) FROM information_schema.tables
                 WHERE table_schema = 'public' AND table_name = $1`,
                [streamName],
            );
            assert.equal(Number(r2.rows[0].count), 0, 'no public.<name> — proxy owns DDL');
        } finally {
            await pg.end();
        }
    });

    it('schema_meta row recorded', async () => {
        const { Client } = await import('pg');
        const c = new Client({ connectionString: PG_URL });
        await c.connect();
        try {
            const r = await c.query(
                `SELECT family, name, schema_version FROM _goldlapel.schema_meta
                 WHERE family = 'stream' AND name = $1`,
                [streamName],
            );
            assert.equal(r.rows.length, 1);
            assert.equal(r.rows[0].family, 'stream');
            assert.equal(r.rows[0].name, streamName);
            assert.equal(r.rows[0].schema_version, 'v1');
        } finally {
            await c.end();
        }
    });

    it('DDL API HTTP call happens once per stream', async () => {
        const ddl = await import('../ddl.js');
        const real = ddl._internals.post;
        let count = 0;
        ddl._internals.post = async (...args) => { count++; return real(...args); };

        try {
            const freshName = `gl_int_stream_count_${Date.now()}`;
            await gl.streamAdd(freshName, { i: 1 });
            assert.equal(count, 1, 'first call posts once');
            await gl.streamAdd(freshName, { i: 2 });
            await gl.streamAdd(freshName, { i: 3 });
            assert.equal(count, 1, 'subsequent calls use cache — no new POST');
        } finally {
            ddl._internals.post = real;
        }
    });
});

describe('stream round-trip', () => {
    let gl, name;

    before(async () => {
        const goldlapel = await import('../index.js');
        const port = 7800 + (Date.now() % 100);
        gl = await goldlapel.start(PG_URL, { port });
        name = `gl_int_rt_${Date.now()}`;
    });

    after(async () => { if (gl) await gl.stop(); });

    it('add and read round-trips via proxy patterns', async () => {
        await gl.streamCreateGroup(name, 'workers');
        const id1 = await gl.streamAdd(name, { i: 1 });
        const id2 = await gl.streamAdd(name, { i: 2 });
        assert.ok(id2 > id1);
        const messages = await gl.streamRead(name, 'workers', 'c', 10);
        assert.equal(messages.length, 2);
        assert.deepEqual(messages[0].payload, { i: 1 });
        assert.deepEqual(messages[1].payload, { i: 2 });
    });

    it('ack removes pending', async () => {
        const ackName = `${name}_ack`;
        await gl.streamCreateGroup(ackName, 'workers');
        const id = await gl.streamAdd(ackName, { i: 1 });
        await gl.streamRead(ackName, 'workers', 'c', 10);
        const first = await gl.streamAck(ackName, 'workers', id);
        assert.equal(first, true);
        const second = await gl.streamAck(ackName, 'workers', id);
        assert.equal(second, false);
    });

    it('claim reassigns idle pending (min_idle_ms=0)', async () => {
        const claimName = `${name}_claim`;
        await gl.streamCreateGroup(claimName, 'workers');
        await gl.streamAdd(claimName, { i: 1 });
        await gl.streamRead(claimName, 'workers', 'consumer-a', 10);
        const claimed = await gl.streamClaim(claimName, 'workers', 'consumer-b', 0);
        assert.equal(claimed.length, 1);
        assert.deepEqual(claimed[0].payload, { i: 1 });
    });
});

} // if SHOULD_RUN
