// Documents namespace API — `gl.documents.<verb>(...)`.
//
// Wraps the doc-store methods in a sub-API instance held on the parent
// GoldLapel client. The instance shares all state (license, dashboard token,
// http session, conn) by reference back to the parent — no duplication.
//
// The proxy owns doc-store DDL (Phase 4 of schema-to-core). Each call here:
//
//   1. Calls `/api/ddl/doc_store/create` (idempotent) to materialize the
//      canonical `_goldlapel.doc_<name>` table and pull its query patterns.
//   2. Caches `(tables, query_patterns)` on the parent GoldLapel instance for
//      the session's lifetime (one HTTP round-trip per (family, name) per
//      session).
//   3. Hands the patterns off to the existing `utils.doc*` functions so they
//      execute against the canonical table name instead of CREATE-ing their
//      own.
//
// Sub-API class shape mirrors `streams.StreamsAPI` — this is the canonical
// pattern for the wrapper rollout. Other namespaces (cache, search, queues,
// counters, hashes, zsets, geo, auth, …) stay flat for now; they migrate to
// nested form one-at-a-time as their own schema-to-core phase fires.

import {
    validateIdentifier,
    docInsert, docInsertMany, docFind, docFindCursor, docFindOne,
    docUpdate, docUpdateOne, docDelete, docDeleteOne,
    docFindOneAndUpdate, docFindOneAndDelete,
    docDistinct,
    docCount, docCreateIndex, docAggregate,
    docWatch, docUnwatch,
    docCreateTtlIndex, docRemoveTtlIndex,
    docCreateCapped, docRemoveCap,
} from './utils.js';

export class DocumentsAPI {
    // The documents sub-API — accessible as `gl.documents`.
    //
    // All methods take the collection name as the first positional argument;
    // remaining args mirror the legacy `gl.doc<Verb>` signatures. State
    // (dashboard token, dashboard port, internal connection, DDL pattern
    // cache) is shared via the parent GoldLapel reference held in `this._gl`.
    constructor(gl) {
        // Hold a back-reference to the parent client. Never copy lifecycle
        // state (token, port, conn) onto this instance — always read through
        // `this._gl` so a config change on the parent (e.g. proxy restart
        // with a new dashboard token) is reflected immediately on the next
        // call.
        this._gl = gl;
    }

    // Fetch (and cache) canonical doc-store DDL + query patterns from the
    // proxy. Cache lives on the parent GoldLapel instance.
    //
    // `unlogged` is a creation-time option; passed only on the first call
    // for a given (family, name) since proxy `CREATE TABLE IF NOT EXISTS`
    // makes subsequent calls no-op DDL-wise. If a caller flips `unlogged`
    // across calls in the same session, the table's storage type is
    // whatever it was on first create — wrappers don't migrate it.
    async _patterns(collection, { unlogged = false } = {}) {
        validateIdentifier(collection);
        const gl = this._gl;
        const ddl = await import('./ddl.js');
        const token = gl._dashboardToken || ddl.tokenFromEnvOrFile();
        const options = unlogged ? { unlogged: true } : null;
        return ddl.fetchPatterns(
            gl, 'doc_store', collection, gl._dashboardPort, token,
            { options },
        );
    }

    // -- Collection lifecycle -----------------------------------------------

    // Eagerly materialize the doc-store table. Other methods will also
    // materialize on first use, so calling this is optional — provided for
    // callers that want explicit setup at startup time.
    async createCollection(collection, { unlogged = false } = {}) {
        await this._patterns(collection, { unlogged });
    }

    // -- CRUD ---------------------------------------------------------------

    async insert(collection, document, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docInsert(conn, collection, document, { patterns });
    }

    async insertMany(collection, documents, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docInsertMany(conn, collection, documents, { patterns });
    }

    async find(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const { conn: connOpt, ...rest } = opts;
        const conn = this._gl._resolveConn(connOpt);
        return docFind(conn, collection, filter, { ...rest, patterns });
    }

    async *findCursor(collection, filter = null, opts = {}) {
        const patterns = await this._patterns(collection);
        const { conn: connOpt, ...rest } = opts;
        const conn = this._gl._resolveConn(connOpt);
        yield* docFindCursor(conn, collection, filter, { ...rest, patterns });
    }

    async findOne(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docFindOne(conn, collection, filter, { patterns });
    }

    async update(collection, filter, update, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docUpdate(conn, collection, filter, update, { patterns });
    }

    async updateOne(collection, filter, update, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docUpdateOne(conn, collection, filter, update, { patterns });
    }

    async delete(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docDelete(conn, collection, filter, { patterns });
    }

    async deleteOne(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docDeleteOne(conn, collection, filter, { patterns });
    }

    async findOneAndUpdate(collection, filter, update, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docFindOneAndUpdate(conn, collection, filter, update, { patterns });
    }

    async findOneAndDelete(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docFindOneAndDelete(conn, collection, filter, { patterns });
    }

    async distinct(collection, field, filter = null, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docDistinct(conn, collection, field, filter, { patterns });
    }

    async count(collection, filter, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docCount(conn, collection, filter, { patterns });
    }

    async createIndex(collection, keys, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docCreateIndex(conn, collection, keys, { patterns });
    }

    // Run a Mongo-style aggregation pipeline.
    //
    // $lookup.from references are resolved to their canonical proxy tables
    // (`_goldlapel.doc_<name>`) — each unique `from` collection triggers an
    // idempotent describe/create against the proxy and is cached for the
    // session.
    async aggregate(collection, pipeline, opts = {}) {
        const patterns = await this._patterns(collection);
        // Walk the pipeline once to find every $lookup.from collection, fetch
        // patterns for each (cached after first call), and pass the resolved
        // map down to docAggregate.
        const lookupTables = {};
        for (const stage of pipeline) {
            if (stage && typeof stage === 'object' && '$lookup' in stage) {
                const spec = stage.$lookup;
                if (spec && typeof spec === 'object' && 'from' in spec) {
                    const fromName = spec.from;
                    if (!(fromName in lookupTables)) {
                        const lp = await this._patterns(fromName);
                        lookupTables[fromName] = lp.tables.main;
                    }
                }
            }
        }
        const conn = this._gl._resolveConn(opts.conn);
        return docAggregate(conn, collection, pipeline, { patterns, lookupTables });
    }

    // -- Watch / TTL / capped ----------------------------------------------

    async watch(collection, callback, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docWatch(conn, collection, callback, { patterns });
    }

    async unwatch(collection, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docUnwatch(conn, collection, { patterns });
    }

    async createTtlIndex(collection, expireAfterSeconds, opts = {}) {
        const patterns = await this._patterns(collection);
        const { conn: connOpt, ...rest } = opts;
        const conn = this._gl._resolveConn(connOpt);
        return docCreateTtlIndex(conn, collection, expireAfterSeconds, { ...rest, patterns });
    }

    async removeTtlIndex(collection, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docRemoveTtlIndex(conn, collection, { patterns });
    }

    async createCapped(collection, maxDocuments, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docCreateCapped(conn, collection, maxDocuments, { patterns });
    }

    async removeCap(collection, opts = {}) {
        const patterns = await this._patterns(collection);
        const conn = this._gl._resolveConn(opts.conn);
        return docRemoveCap(conn, collection, { patterns });
    }
}
