import {
    NativeCache, detectWrite, DDL_SENTINEL, TX_START, TX_END,
    ConnectionGucState, splitStatements,
} from './cache.js';

// Postgres response `command` strings that signal a session-state /
// control-flow command rather than a cacheable read. Caching their
// `{rows: [], fields: []}` reply bloats the cache with no-row entries
// that never serve real data and triggers needless eviction pressure.
// Truthy-array check (`result.rows && result.fields`) misses this in JS
// because `[] && []` is truthy.
const NON_CACHEABLE_COMMANDS = new Set([
    'SET', 'RESET', 'LISTEN', 'UNLISTEN', 'NOTIFY',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
]);

// Multi-statement Q-message write detection. Reuses splitStatements so
// `SET app.user_id = '42'; INSERT INTO orders VALUES (1)` (a single Q
// message) doesn't slip past detectWrite's first-token check. Returns:
//   - DDL_SENTINEL  if any segment is DDL (caller invalidateAll's)
//   - Set<string>   of table names if any segments are table-scoped writes
//   - null          if no segment is a write
// Single-statement bodies skip the splitter entirely (hot path).
function detectWritesMulti(sql) {
    if (typeof sql !== 'string' || sql.length === 0) return null;
    // Fast path: no inner `;` → single-statement, original detectWrite.
    const trimmed = sql.trimEnd().replace(/;+$/, '');
    if (!trimmed.includes(';')) {
        const t = detectWrite(sql);
        if (t === null) return null;
        if (t === DDL_SENTINEL) return DDL_SENTINEL;
        return new Set([t]);
    }
    const tables = new Set();
    for (const seg of splitStatements(sql)) {
        const t = detectWrite(seg);
        if (t === DDL_SENTINEL) return DDL_SENTINEL;
        if (t !== null) tables.add(t);
    }
    return tables.size > 0 ? tables : null;
}

let _cache = null;

function _detectInvalidationPort() {
    // Default: proxy port + 2
    return 7934;
}

export function wrap(client, invalidationPort) {
    if (!_cache) {
        _cache = new NativeCache();
    }
    if (invalidationPort == null) {
        invalidationPort = _detectInvalidationPort();
    }
    if (!_cache._socket) {
        _cache.connectInvalidation(invalidationPort);
    }
    const cached = new CachedClient(client, _cache);
    return new Proxy(cached, {
        get(target, prop) {
            if (prop in target) {
                const val = target[prop];
                return typeof val === 'function' ? val.bind(target) : val;
            }
            const val = client[prop];
            return typeof val === 'function' ? val.bind(client) : val;
        }
    });
}

class CachedClient {
    constructor(realClient, cache) {
        this._real = realClient;
        this._cache = cache;
        this._inTransaction = false;
        // Per-connection unsafe-GUC state hash. Mirrors the proxy's
        // per-connection `ConnectionGucState` (commit 3e02359). Folded
        // into the native cache key on every read/write so two
        // CachedClients with different unsafe-GUC state never collide on
        // a shared singleton cache (the wrapper has one cache singleton
        // per process; CachedClient is per-conn).
        this._gucState = new ConnectionGucState();
    }

    async query(textOrConfig, values) {
        let sql, params;
        if (typeof textOrConfig === 'object' && textOrConfig !== null) {
            sql = textOrConfig.text;
            params = textOrConfig.values;
        } else {
            sql = textOrConfig;
            params = values;
        }

        // GUC state observation. Runs on every query before any
        // transaction / write / read branching: a `SET app.user_id =
        // '42'` shifts the hash, so the very next SELECT must key
        // against the new hash. Cheap on the hot path — fast-path
        // single-statement queries skip the splitter entirely.
        if (typeof sql === 'string' && sql.length > 0) {
            this._gucState.observeSql(sql);
        }

        // Multi-statement-aware write detection. Runs BEFORE transaction
        // tracking so a single Q message like `BEGIN; INSERT INTO orders
        // VALUES (1); COMMIT` still surfaces the INSERT for invalidation
        // — TX_START's first-token check would otherwise swallow the
        // whole body as a transaction-boundary marker and the INSERT
        // would never invalidate the `orders` cache slot. Same gap fixed
        // for `SET app.tenant = 'x'; INSERT INTO t ...` (SET hides the
        // INSERT from detectWrite's single-token shape).
        const writeTables = detectWritesMulti(sql);
        if (writeTables !== null) {
            if (writeTables === DDL_SENTINEL) {
                this._cache.invalidateAll();
            } else {
                for (const t of writeTables) this._cache.invalidateTable(t);
            }
        }

        // Transaction tracking
        if (TX_START.test(sql)) {
            this._inTransaction = true;
            return this._real.query(textOrConfig, values);
        }
        if (TX_END.test(sql)) {
            this._inTransaction = false;
            return this._real.query(textOrConfig, values);
        }

        // If this was a single-statement write, we've already invalidated
        // and now just dispatch to the real client without going through
        // the read path. Multi-statement bodies that contain writes
        // alongside SELECTs are uncacheable (extractTables on the joined
        // SQL would index against the wrong slot), so they also exit here.
        if (writeTables !== null) {
            return this._real.query(textOrConfig, values);
        }

        // Inside transaction: bypass cache
        if (this._inTransaction) {
            return this._real.query(textOrConfig, values);
        }

        // Read path: check native cache, gated on the per-connection
        // unsafe-GUC state hash so user A's RLS-scoped rows can never
        // be served to user B from a shared cache slot.
        const stateHash = this._gucState.stateHash();
        const entry = this._cache.get(sql, params, stateHash);
        if (entry !== null) {
            return {
                rows: entry.rows,
                fields: entry.fields,
                rowCount: entry.rows.length,
                command: 'SELECT',
            };
        }

        // Cache miss: execute for real
        const result = await this._real.query(textOrConfig, values);

        // Cache the result only if it's a real read response. Same state
        // hash gating as the get() above — the entry is keyed to the
        // connection state at the time of the response.
        //
        // Skip session-state command replies. `SET foo = 'bar'` returns
        // `{rows: [], fields: [], command: 'SET'}` — `[] && []` is truthy
        // in JS so the old check let these through, bloating the cache
        // with no-row entries that never serve real data and triggering
        // needless eviction pressure on session-heavy workloads.
        // Empty-result SELECTs (zero rows from `WHERE id = -1`) are
        // intentionally still cached — the proxy does the same.
        if (
            result.rows && result.fields
            && !NON_CACHEABLE_COMMANDS.has(result.command)
        ) {
            this._cache.put(sql, params, result.rows, result.fields, stateHash);
        }

        return result;
    }

    async connect() {
        return this._real.connect();
    }

    async end() {
        return this._real.end();
    }

    on(event, handler) {
        return this._real.on(event, handler);
    }

    off(event, handler) {
        return this._real.off(event, handler);
    }

    once(event, handler) {
        return this._real.once(event, handler);
    }
}

export { CachedClient };
