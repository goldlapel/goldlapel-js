// Sorted-set (zset) namespace API — `gl.zsets.<verb>(...)`.
//
// Phase 5 of schema-to-core. The proxy's v1 zset schema introduces a
// `zset_key` column so a single namespace table holds many sorted sets —
// matching Redis's mental model. Every method below threads `zsetKey` as the
// first positional arg after the namespace `name`.

import {
    validateIdentifier,
    zsetAdd, zsetIncrBy, zsetScore, zsetRank,
    zsetRange, zsetRangeByScore, zsetRemove, zsetCard,
} from './utils.js';

export class ZsetsAPI {
    // The zsets sub-API — accessible as `gl.zsets`.
    //
    // Method shape: `gl.zsets.<verb>(name, zsetKey, ...)`. `name` is the
    // namespace (one Postgres table); `zsetKey` partitions multiple sorted
    // sets within that namespace.
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
            gl, 'zset', name, gl._dashboardPort, token,
            { options },
        );
    }

    async create(name, { unlogged = false } = {}) {
        await this._patterns(name, { unlogged });
    }

    async add(name, zsetKey, member, score, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return zsetAdd(conn, name, zsetKey, member, score, { patterns });
    }

    async incrBy(name, zsetKey, member, delta = 1, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return zsetIncrBy(conn, name, zsetKey, member, delta, { patterns });
    }

    async score(name, zsetKey, member, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return zsetScore(conn, name, zsetKey, member, { patterns });
    }

    async rank(name, zsetKey, member, { desc = true, conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        return zsetRank(conn, name, zsetKey, member, { desc, patterns });
    }

    // Members by rank within `zsetKey`. Inclusive `start`/`stop` Redis-style;
    // `stop = -1` is a sentinel meaning "to the end" — mapped to a large
    // limit since the proxy's pattern is LIMIT/OFFSET-based. Callers wanting
    // the entire set should page explicitly via rangeByScore.
    async range(name, zsetKey, { start = 0, stop = -1, desc = true, conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        const effectiveStop = (stop === undefined || stop === null || stop === -1)
            ? 9999
            : stop;
        return zsetRange(conn, name, zsetKey, start, effectiveStop, desc, { patterns });
    }

    async rangeByScore(name, zsetKey, minScore, maxScore, { limit = 100, offset = 0, conn: connOpt } = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(connOpt);
        return zsetRangeByScore(
            conn, name, zsetKey, minScore, maxScore, limit, offset,
            { patterns },
        );
    }

    async remove(name, zsetKey, member, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return zsetRemove(conn, name, zsetKey, member, { patterns });
    }

    async card(name, zsetKey, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return zsetCard(conn, name, zsetKey, { patterns });
    }
}
