import {
    NativeCache, detectWrite, DDL_SENTINEL, TX_START, TX_END,
    ConnectionGucState,
} from './cache.js';

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

        // Transaction tracking
        if (TX_START.test(sql)) {
            this._inTransaction = true;
            return this._real.query(textOrConfig, values);
        }
        if (TX_END.test(sql)) {
            this._inTransaction = false;
            return this._real.query(textOrConfig, values);
        }

        // Write detection + self-invalidation
        const writeTable = detectWrite(sql);
        if (writeTable) {
            if (writeTable === DDL_SENTINEL) {
                this._cache.invalidateAll();
            } else {
                this._cache.invalidateTable(writeTable);
            }
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

        // Cache the result if it has rows. Same state hash gating as the
        // get() above — the entry is keyed to the connection state at the
        // time of the response.
        if (result.rows && result.fields) {
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
