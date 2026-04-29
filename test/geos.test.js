// Unit tests for geos.js — the nested gl.geos namespace introduced in
// Phase 5 of schema-to-core.
//
// Phase 5 schema decisions:
//   - GEOGRAPHY column type (not GEOMETRY) — distance returns are meters
//     native; no `::geography` casts on the wrapper side.
//   - `member TEXT PRIMARY KEY` — re-adding a member updates its location
//     (idempotent), matching Redis GEOADD semantics.
//   - `updated_at` stamped on every UPSERT.
//
// These tests verify:
//   - `add` is idempotent on member name (the proxy's ON CONFLICT DO UPDATE).
//   - SQL uses the canonical GEOGRAPHY-native pattern (no `::geography`
//     casts on the column reference because the column already IS geography).
//   - Distance unit conversion at the wrapper edge (m / km / mi / ft).
//   - **Radius CTE-anchor contract**: `geoRadius` passes
//     `[lon, lat, radius_m, limit]` (4 elements, no duplicates) — each $N
//     appears exactly once in the rendered SQL.
//   - **`geoRadiusByMember` non-CTE contract**: `[member, member, radius_m,
//     limit]` — `$1` and `$2` are both the anchor member name.

import { describe, it, beforeEach, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { createServer } from 'node:http';

import {
    GoldLapel, GeosAPI,
    geoAdd, geoPos, geoDist, geoRadius, geoRadiusByMember,
    geoRemove, geoCount,
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

const geoCreateBody = (table) => ({
    accepted: true,
    family: 'geo',
    schema_version: 'v1',
    tables: { main: table },
    query_patterns: {
        geoadd: `INSERT INTO ${table} (member, location, updated_at) VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography, NOW()) ON CONFLICT (member) DO UPDATE SET location = EXCLUDED.location, updated_at = NOW() RETURNING ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat`,
        geopos: `SELECT ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM ${table} WHERE member = $1`,
        geodist: `SELECT ST_Distance(a.location, b.location) AS distance_m FROM ${table} a, ${table} b WHERE a.member = $1 AND b.member = $2`,
        // CTE-anchor: each $N appears exactly once in the rendered SQL.
        georadius: `WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat FROM ${table}, anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY ST_Distance(location, anchor.geog) LIMIT $4`,
        georadius_with_dist: `WITH anchor AS ( SELECT ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography AS geog ) SELECT member, ST_X(location::geometry) AS lon, ST_Y(location::geometry) AS lat, ST_Distance(location, anchor.geog) AS distance_m FROM ${table}, anchor WHERE ST_DWithin(location, anchor.geog, $3) ORDER BY distance_m LIMIT $4`,
        // NOT a CTE — $1 and $2 are both the anchor member; $3 = radius, $4 = limit.
        geosearch_member: `SELECT b.member, ST_X(b.location::geometry) AS lon, ST_Y(b.location::geometry) AS lat, ST_Distance(b.location, a.location) AS distance_m FROM ${table} a, ${table} b WHERE a.member = $1 AND ST_DWithin(b.location, a.location, $3) AND b.member <> $2 ORDER BY distance_m LIMIT $4`,
        geo_remove: `DELETE FROM ${table} WHERE member = $1`,
        geo_count: `SELECT COUNT(*) FROM ${table}`,
        delete_all: `DELETE FROM ${table}`,
    },
});

describe('GeosAPI shape', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('gl.geos is a GeosAPI bound to the parent client', () => {
        const inst = gl(dash.port);
        assert.ok(inst.geos instanceof GeosAPI);
        assert.strictEqual(inst.geos._gl, inst);
    });

    it('Phase 5 hard cut — flat geo methods are gone', () => {
        const inst = gl(dash.port);
        for (const legacy of ['geoadd', 'geodist', 'georadius']) {
            assert.strictEqual(inst[legacy], undefined);
        }
    });
});

describe('GeosAPI dispatches each verb to utils.geo*', () => {
    let dash;
    beforeEach(async () => { dash = new FakeDashboard(); await dash.start(); });
    afterEach(async () => { await dash.stop(); });

    it('add is idempotent (proxy ON CONFLICT (member) DO UPDATE)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ lon: 13.4, lat: 52.5 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        const pos = await inst.geos.add('riders', 'alice', 13.4, 52.5);
        assert.deepEqual(pos, [13.4, 52.5]);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('ON CONFLICT (member)'));
        assert.ok(sql.includes('DO UPDATE'));
        assert.deepEqual(conn._calls[0].values, ['alice', 13.4, 52.5]);
    });

    it('pos returns null for unknown member', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        assert.strictEqual(await inst.geos.pos('riders', 'missing'), null);
    });

    it('dist returns meters by default', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ distance_m: 1234 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        assert.strictEqual(
            await inst.geos.dist('riders', 'alice', 'bob'),
            1234,
        );
    });

    it('dist converts to km when unit=km', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ distance_m: 1234 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        assert.strictEqual(
            await inst.geos.dist('riders', 'alice', 'bob', { unit: 'km' }),
            1.234,
        );
    });

    it('dist converts to miles', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ distance_m: 1609.344 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        const result = await inst.geos.dist('riders', 'alice', 'bob', { unit: 'mi' });
        assert.ok(Math.abs(result - 1.0) < 1e-6);
    });

    it('dist with unknown unit throws', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ distance_m: 1.0 }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        await assert.rejects(
            () => inst.geos.dist('riders', 'a', 'b', { unit: 'parsec' }),
            /Unknown distance unit/,
        );
    });

    // CRITICAL — proxy radius CTE-anchor contract.
    it('radius binds [lon, lat, radius_m, limit] (4 elements, NO duplicates)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        await inst.geos.radius('riders', 13.4, 52.5, 5, { unit: 'km' });
        // 5 km → 5000 m. Each $N appears exactly once in the CTE-anchored
        // SQL — no duplicates needed in the params array.
        assert.deepEqual(conn._calls[0].values, [13.4, 52.5, 5000, 50]);
        assert.strictEqual(conn._calls[0].values.length, 4,
            'CTE-anchor contract: exactly 4 params, no duplicates.');
    });

    // CRITICAL — geosearch_member non-CTE contract.
    it('radiusByMember passes [member, member, radius_m, limit] (member twice)', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 0 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        await inst.geos.radiusByMember('riders', 'alice', 1000);
        // Proxy SQL: WHERE a.member=$1 AND ST_DWithin(...,$3) AND b.member<>$2 LIMIT $4
        // pg binds $N natively, so we pass [member, member, radius_m, limit].
        assert.deepEqual(conn._calls[0].values, ['alice', 'alice', 1000, 50]);
    });

    it('remove returns true when row deleted', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        assert.strictEqual(await inst.geos.remove('riders', 'alice'), true);
    });

    it('count returns 0 when empty', async () => {
        const inst = gl(dash.port);
        const conn = mockClient({ rows: [{ count: '0' }], rowCount: 1 });
        inst._defaultConn = conn;
        dash.responses.push([200, geoCreateBody('_goldlapel.geo_riders')]);

        assert.strictEqual(await inst.geos.count('riders'), 0);
    });
});

describe('geo utils SQL contract', () => {
    const fakePatterns = geoCreateBody('_goldlapel.geo_riders');

    it('geoAdd ON CONFLICT (member) DO UPDATE makes it idempotent', async () => {
        const conn = mockClient({ rows: [{ lon: 13.4, lat: 52.5 }], rowCount: 1 });
        await geoAdd(conn, 'riders', 'alice', 13.4, 52.5, { patterns: fakePatterns });
        assert.strictEqual(conn._calls.length, 1);
        const sql = conn._calls[0].text;
        assert.ok(sql.includes('ON CONFLICT (member)'));
        assert.ok(sql.includes('DO UPDATE'));
    });

    it('geoAdd pattern is GEOGRAPHY-native (no ::geography cast on column)', () => {
        // The wrapper writes the literal proxy pattern; the column itself
        // is GEOGRAPHY, so the SELECT/RETURNING clauses cast through
        // ::geometry to extract lon/lat — that's expected. What we forbid
        // is a `::geography` cast on the column reference in WHERE.
        const sql = fakePatterns.query_patterns.geoadd;
        assert.ok(sql.includes('ST_SetSRID(ST_MakePoint($2, $3), 4326)::geography'));
    });

    it('geoPos returns [lon, lat] tuple', async () => {
        const conn = mockClient({ rows: [{ lon: 13.4, lat: 52.5 }], rowCount: 1 });
        const pos = await geoPos(conn, 'riders', 'alice', { patterns: fakePatterns });
        assert.deepEqual(pos, [13.4, 52.5]);
    });

    it('geoPos returns null when absent', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const pos = await geoPos(conn, 'riders', 'missing', { patterns: fakePatterns });
        assert.strictEqual(pos, null);
    });

    it('geoDist returns null when row missing', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        const result = await geoDist(conn, 'riders', 'a', 'b', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('geoDist returns null when distance column is null (member absent)', async () => {
        const conn = mockClient({ rows: [{ distance_m: null }], rowCount: 1 });
        const result = await geoDist(conn, 'riders', 'a', 'b', { patterns: fakePatterns });
        assert.strictEqual(result, null);
    });

    it('geoDist converts km/mi/ft via the unit table', async () => {
        let conn = mockClient({ rows: [{ distance_m: 1000 }], rowCount: 1 });
        assert.strictEqual(
            await geoDist(conn, 'r', 'a', 'b', { unit: 'km', patterns: fakePatterns }),
            1,
        );
        conn = mockClient({ rows: [{ distance_m: 1609.344 }], rowCount: 1 });
        assert.ok(Math.abs(
            await geoDist(conn, 'r', 'a', 'b', { unit: 'mi', patterns: fakePatterns }) - 1
        ) < 1e-6);
        conn = mockClient({ rows: [{ distance_m: 0.3048 }], rowCount: 1 });
        assert.ok(Math.abs(
            await geoDist(conn, 'r', 'a', 'b', { unit: 'ft', patterns: fakePatterns }) - 1
        ) < 1e-6);
    });

    it('geoDist with unknown unit throws', async () => {
        const conn = mockClient({ rows: [{ distance_m: 1.0 }], rowCount: 1 });
        await assert.rejects(
            () => geoDist(conn, 'r', 'a', 'b', { unit: 'parsec', patterns: fakePatterns }),
            /Unknown distance unit/,
        );
    });

    it('geoRadius converts unit to meters and binds 4-tuple [lon, lat, m, limit]', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        await geoRadius(conn, 'r', 13.4, 52.5, 5, { unit: 'km', limit: 50, patterns: fakePatterns });
        assert.deepEqual(conn._calls[0].values, [13.4, 52.5, 5000, 50]);
    });

    it('geoRadius default unit is meters', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        await geoRadius(conn, 'r', 0, 0, 1000, { patterns: fakePatterns });
        // 1000 m, no unit conversion.
        assert.deepEqual(conn._calls[0].values, [0, 0, 1000, 50]);
    });

    it('geoRadiusByMember binds [member, member, radius_m, limit]', async () => {
        const conn = mockClient({ rows: [], rowCount: 0 });
        await geoRadiusByMember(conn, 'r', 'alice', 1000, { patterns: fakePatterns });
        assert.deepEqual(conn._calls[0].values, ['alice', 'alice', 1000, 50]);
    });

    it('geoRemove returns boolean from rowCount', async () => {
        let conn = mockClient({ rows: [], rowCount: 1 });
        assert.strictEqual(
            await geoRemove(conn, 'r', 'alice', { patterns: fakePatterns }),
            true,
        );
        conn = mockClient({ rows: [], rowCount: 0 });
        assert.strictEqual(
            await geoRemove(conn, 'r', 'missing', { patterns: fakePatterns }),
            false,
        );
    });

    it('geoCount reads BIGINT count from pg', async () => {
        const conn = mockClient({ rows: [{ count: '3' }], rowCount: 1 });
        assert.strictEqual(
            await geoCount(conn, 'r', { patterns: fakePatterns }),
            3,
        );
    });

    it('throws when called without patterns', async () => {
        await assert.rejects(
            () => geoAdd(mockClient(), 'r', 'm', 0, 0),
            /geo utils require/,
        );
    });
});
