// Hash namespace API — `gl.hashes.<verb>(...)`.
//
// Phase 5 of schema-to-core. The proxy's v1 hash schema is row-per-field
// (`hash_key`, `field`, `value`) — NOT the legacy JSONB-blob-per-key shape.
// Every method threads `hashKey` as the first positional arg after the
// namespace `name`. Values are JSON-encoded so callers can store arbitrary
// structured payloads (objects, arrays, numbers, strings, …).

import {
    validateIdentifier,
    hashSet, hashGet, hashGetAll, hashKeys, hashValues,
    hashExists, hashDelete, hashLen,
} from './utils.js';

export class HashesAPI {
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
            gl, 'hash', name, gl._dashboardPort, token,
            { options },
        );
    }

    async create(name, { unlogged = false } = {}) {
        await this._patterns(name, { unlogged });
    }

    async set(name, hashKey, field, value, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashSet(conn, name, hashKey, field, value, { patterns });
    }

    async get(name, hashKey, field, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashGet(conn, name, hashKey, field, { patterns });
    }

    async getAll(name, hashKey, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashGetAll(conn, name, hashKey, { patterns });
    }

    async keys(name, hashKey, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashKeys(conn, name, hashKey, { patterns });
    }

    async values(name, hashKey, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashValues(conn, name, hashKey, { patterns });
    }

    async exists(name, hashKey, field, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashExists(conn, name, hashKey, field, { patterns });
    }

    async delete(name, hashKey, field, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashDelete(conn, name, hashKey, field, { patterns });
    }

    async len(name, hashKey, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return hashLen(conn, name, hashKey, { patterns });
    }
}
