// Counters namespace API — `gl.counters.<verb>(...)`.
//
// Phase 5 of schema-to-core: the proxy owns counter DDL. Each call here:
//
//   1. Calls `/api/ddl/counter/create` (idempotent) to materialize the
//      canonical `_goldlapel.counter_<name>` table and pull its query
//      patterns.
//   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance for
//      the session's lifetime (one HTTP round-trip per (family, name) per
//      session).
//   3. Hands the patterns off to the `utils.counter*` helpers, which run the
//      proxy's canonical SQL verbatim — `pg` binds `$N` natively, so no
//      placeholder translation is needed.
//
// Mirrors `documents.DocumentsAPI` exactly — the canonical schema-to-core
// sub-API shape for the JS wrapper.

import {
    validateIdentifier,
    counterIncr, counterDecr, counterSet, counterGet, counterDelete,
    counterCountKeys,
} from './utils.js';

export class CountersAPI {
    // The counters sub-API — accessible as `gl.counters`.
    //
    // Every method takes the namespace `name` as the first positional arg;
    // the per-key value-mutation surface follows. State (dashboard token,
    // dashboard port, internal connection, DDL pattern cache) is shared via
    // the parent GoldLapel reference held in `this._gl`.
    constructor(gl) {
        this._gl = gl;
    }

    // Fetch (and cache) canonical counter DDL + query patterns from the
    // proxy. Cache lives on the parent GoldLapel instance — see ddl.js.
    async _patterns(name, { unlogged = false } = {}) {
        validateIdentifier(name);
        const gl = this._gl;
        const ddl = await import('./ddl.js');
        const token = gl._dashboardToken || ddl.tokenFromEnvOrFile();
        const options = unlogged ? { unlogged: true } : null;
        return ddl.fetchPatterns(
            gl, 'counter', name, gl._dashboardPort, token,
            { options },
        );
    }

    // Eagerly materialize the counter table. Other methods will also
    // materialize on first use, so calling this is optional — provided for
    // callers that want explicit setup at startup time.
    async create(name, { unlogged = false } = {}) {
        await this._patterns(name, { unlogged });
    }

    async incr(name, key, amount = 1, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterIncr(conn, name, key, amount, { patterns });
    }

    async decr(name, key, amount = 1, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterDecr(conn, name, key, amount, { patterns });
    }

    async set(name, key, value, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterSet(conn, name, key, value, { patterns });
    }

    async get(name, key, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterGet(conn, name, key, { patterns });
    }

    async delete(name, key, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterDelete(conn, name, key, { patterns });
    }

    async countKeys(name, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return counterCountKeys(conn, name, { patterns });
    }
}
