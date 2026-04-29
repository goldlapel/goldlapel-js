// Streams namespace API — `gl.streams.<verb>(...)`.
//
// Wraps the wire-level stream methods in a sub-API instance held on the
// parent GoldLapel client. The instance shares all state (license, dashboard
// token, http session, conn) by reference back to the parent — no
// duplication.
//
// This is the canonical sub-API shape for the schema-to-core wrapper rollout.
// Other namespaces (cache, search, queues, counters, hashes, zsets, geo,
// auth, …) stay flat for now; they migrate to nested form one-at-a-time as
// their own schema-to-core phase fires.

import {
    validateIdentifier,
    streamAdd, streamCreateGroup, streamRead, streamAck, streamClaim,
} from './utils.js';

export class StreamsAPI {
    // The streams sub-API — accessible as `gl.streams`.
    //
    // All methods take the stream name as the first positional argument;
    // remaining args mirror the legacy `gl.stream<Verb>` signatures. State
    // (dashboard token, dashboard port, internal connection, DDL pattern
    // cache) is shared via the parent GoldLapel reference held in `this._gl`.
    constructor(gl) {
        // Hold a back-reference to the parent client. We never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `this._gl` so a config change on the parent (e.g. proxy restart
        // with a new dashboard token) is reflected immediately on the next
        // call.
        this._gl = gl;
    }

    // Fetch (and cache) canonical stream DDL + query patterns from the
    // proxy. Cache lives on the parent GoldLapel instance — see ddl.js.
    async _patterns(stream) {
        validateIdentifier(stream);
        const gl = this._gl;
        const ddl = await import('./ddl.js');
        const token = gl._dashboardToken || ddl.tokenFromEnvOrFile();
        // Cache owner is the parent client so describe-once-per-session works
        // even if the user holds onto a `gl.streams` reference across calls.
        return ddl.fetchPatterns(gl, 'stream', stream, gl._dashboardPort, token);
    }

    async add(stream, payload, opts = {}) {
        const patterns = await this._patterns(stream);
        const conn = this._gl._resolveConn(opts.conn);
        return streamAdd(conn, stream, payload, { patterns });
    }

    async createGroup(stream, group, opts = {}) {
        const patterns = await this._patterns(stream);
        const conn = this._gl._resolveConn(opts.conn);
        return streamCreateGroup(conn, stream, group, { patterns });
    }

    async read(stream, group, consumer, count = 1, opts = {}) {
        const patterns = await this._patterns(stream);
        const conn = this._gl._resolveConn(opts.conn);
        return streamRead(conn, stream, group, consumer, count, { patterns });
    }

    async ack(stream, group, messageId, opts = {}) {
        const patterns = await this._patterns(stream);
        const conn = this._gl._resolveConn(opts.conn);
        return streamAck(conn, stream, group, messageId, { patterns });
    }

    async claim(stream, group, consumer, minIdleMs = 60000, opts = {}) {
        const patterns = await this._patterns(stream);
        const conn = this._gl._resolveConn(opts.conn);
        return streamClaim(conn, stream, group, consumer, minIdleMs, { patterns });
    }
}
