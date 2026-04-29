// Geo namespace API — `gl.geos.<verb>(...)`.
//
// Phase 5 of schema-to-core. The proxy's v1 geo schema uses GEOGRAPHY (not
// GEOMETRY), `member TEXT PRIMARY KEY` (not `BIGSERIAL` + `name`), and a
// GIST index on the location column. `geos.add` is idempotent on the member
// name — re-adding a member updates its location.
//
// Distance unit: methods accept `{ unit: 'm' | 'km' | 'mi' | 'ft' }`. The
// proxy column is meters-native (GEOGRAPHY default); the wrapper converts at
// the edge.
//
// Radius patterns use a CTE-anchor: each `$N` appears exactly once in
// `georadius` / `georadiusWithDist` so `pg`'s native `$N` binding works
// without duplicate-expansion. `geosearchMember` does NOT use a CTE: `$1`
// and `$2` are both the anchor member name (passed twice once each), `$3` is
// the radius in meters, `$4` is the limit.

import {
    validateIdentifier,
    geoAdd, geoPos, geoDist, geoRadius, geoRadiusByMember,
    geoRemove, geoCount,
} from './utils.js';

export class GeosAPI {
    constructor(gl) {
        this._gl = gl;
    }

    async _patterns(name, { unlogged = false } = {}) {
        validateIdentifier(name);
        const gl = this._gl;
        const ddl = await import('./ddl.js');
        const token = gl._dashboardToken || ddl.tokenFromEnvOrFile();
        const options = unlogged ? { unlogged: true } : null;
        return ddl.fetchPatterns(
            gl, 'geo', name, gl._dashboardPort, token,
            { options },
        );
    }

    async create(name, { unlogged = false } = {}) {
        await this._patterns(name, { unlogged });
    }

    // Set-or-update a member's lon/lat. Idempotent on the member name (PK).
    async add(name, member, lon, lat, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return geoAdd(conn, name, member, lon, lat, { patterns });
    }

    async pos(name, member, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return geoPos(conn, name, member, { patterns });
    }

    async dist(name, memberA, memberB, { unit = 'm', conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        return geoDist(conn, name, memberA, memberB, { unit, patterns });
    }

    // Members within `radius` of (lon, lat). Returns an array of objects
    // with `member`, `lon`, `lat`, `distance_m`.
    async radius(name, lon, lat, radius, { unit = 'm', limit = 50, conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        return geoRadius(conn, name, lon, lat, radius, { unit, limit, patterns });
    }

    // Members within `radius` of `member`'s location.
    async radiusByMember(name, member, radius, { unit = 'm', limit = 50, conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        return geoRadiusByMember(conn, name, member, radius, { unit, limit, patterns });
    }

    async remove(name, member, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return geoRemove(conn, name, member, { patterns });
    }

    async count(name, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return geoCount(conn, name, { patterns });
    }
}
