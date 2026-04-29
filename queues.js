// Queue namespace API — `gl.queues.<verb>(...)`.
//
// Phase 5 of schema-to-core. The proxy's v1 queue schema is at-least-once
// with visibility-timeout — NOT the legacy fire-and-forget shape. The
// breaking change:
//
//   Before:  payload = await gl.dequeue('jobs')           // delete-on-fetch, may lose work
//   After :  const msg = await gl.queues.claim('jobs')    // lease the row
//            if (msg) {
//                // ... handle msg.payload ...
//                await gl.queues.ack('jobs', msg.id)      // commit; missing ack → redelivery
//            }
//
// `claim` returns `{ id, payload }` or `null`. The caller MUST `ack(id)` to
// commit, or `abandon(id)` to release the lease immediately. A consumer that
// crashes leaves the lease standing; the message becomes ready again after
// `visibilityTimeoutMs` and is redelivered to the next claim.
//
// No `dequeue` shim — the master plan rejected a claim+ack alias because the
// resulting at-most-once semantic silently re-introduces the lossy behaviour
// the new schema was designed to fix.

import {
    validateIdentifier,
    queueEnqueue, queueClaim, queueAck, queueAbandon,
    queueExtend, queuePeek, queueCountReady, queueCountClaimed,
} from './utils.js';

export class QueuesAPI {
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
            gl, 'queue', name, gl._dashboardPort, token,
            { options },
        );
    }

    async create(name, { unlogged = false } = {}) {
        await this._patterns(name, { unlogged });
    }

    async enqueue(name, payload, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueEnqueue(conn, name, payload, { patterns });
    }

    // Lease the next ready message; returns `{ id, payload }` or null.
    async claim(name, visibilityTimeoutMs = 30000, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueClaim(conn, name, visibilityTimeoutMs, { patterns });
    }

    async ack(name, messageId, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueAck(conn, name, messageId, { patterns });
    }

    // Release a claim immediately so the message is redelivered without
    // waiting for the visibility timeout. Equivalent to a queue NACK.
    async abandon(name, messageId, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueAbandon(conn, name, messageId, { patterns });
    }

    // Push the visibility deadline forward by `additionalMs`.
    async extend(name, messageId, additionalMs, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueExtend(conn, name, messageId, additionalMs, { patterns });
    }

    // Look at the next-ready message without claiming.
    async peek(name, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queuePeek(conn, name, { patterns });
    }

    async countReady(name, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueCountReady(conn, name, { patterns });
    }

    async countClaimed(name, opts = {}) {
        const patterns = await this._patterns(name);
        const conn = this._gl._resolveConn(opts.conn);
        return queueCountClaimed(conn, name, { patterns });
    }
}
