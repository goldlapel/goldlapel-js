import { createConnection } from 'net';
import { existsSync, readFileSync } from 'fs';
import { platform } from 'os';
import { randomUUID } from 'crypto';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const DDL_SENTINEL = '__ddl__';

// --- L1 telemetry tuning ---
//
// Demand-driven model (2026-05-03 — port of the Python pattern, see
// goldlapel-python/docs/wrapper-telemetry-pattern.md): the wrapper has
// NO background timer. Cache counters increment on cache ops (free);
// state-change events are emitted synchronously when a relevant counter
// crosses a threshold; snapshot replies are sent only when the proxy
// asks via `?:<request>` over the existing invalidation socket.
//
// Eviction-rate sliding window. cache_full fires when ≥ EVICT_RATE_HIGH
// of the last EVICT_RATE_WINDOW cache writes (puts) caused an eviction;
// cache_recovered fires when the rate falls back below EVICT_RATE_LOW.
// Hysteresis (50% / 10%) avoids flapping at the boundary.
const EVICT_RATE_WINDOW = 200;
const EVICT_RATE_HIGH = 0.5; // 50% of recent puts evicted → cache_full
const EVICT_RATE_LOW = 0.1;  // ≤ 10% → cache_recovered

const __dirname = dirname(fileURLToPath(import.meta.url));

// Wrapper version, read once at module init from package.json. Mirrors
// the `application_name` marker logic in index.js but is duplicated
// here so the cache module stays standalone (no circular import).
function _readWrapperVersion() {
    try {
        const raw = readFileSync(join(__dirname, 'package.json'), 'utf8');
        const pkg = JSON.parse(raw);
        return pkg.version || 'unknown';
    } catch {
        return 'unknown';
    }
}

const TX_START = /^\s*(BEGIN|START\s+TRANSACTION)\b/i;
const TX_END = /^\s*(COMMIT|ROLLBACK|END)\b/i;

const TABLE_PATTERN = /\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/gi;

const SQL_KEYWORDS = new Set([
    'select', 'from', 'where', 'and', 'or', 'not', 'in', 'exists',
    'between', 'like', 'is', 'null', 'true', 'false', 'as', 'on',
    'left', 'right', 'inner', 'outer', 'cross', 'full', 'natural',
    'group', 'order', 'having', 'limit', 'offset', 'union', 'intersect',
    'except', 'all', 'distinct', 'lateral', 'values',
]);

function makeKey(sql, values) {
    try {
        return sql + '\0' + JSON.stringify(values ?? null);
    } catch {
        return null;
    }
}

function detectWrite(sql) {
    const trimmed = sql.trim();
    const tokens = trimmed.split(/\s+/);
    if (!tokens.length) return null;
    const first = tokens[0].toUpperCase();

    if (first === 'INSERT') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'INTO') return null;
        return bareTable(tokens[2]);
    } else if (first === 'UPDATE') {
        if (tokens.length < 2) return null;
        return bareTable(tokens[1]);
    } else if (first === 'DELETE') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'FROM') return null;
        return bareTable(tokens[2]);
    } else if (first === 'TRUNCATE') {
        if (tokens.length < 2) return null;
        if (tokens[1].toUpperCase() === 'TABLE') {
            if (tokens.length < 3) return null;
            return bareTable(tokens[2]);
        }
        return bareTable(tokens[1]);
    } else if (first === 'CREATE' || first === 'ALTER' || first === 'DROP' || first === 'REFRESH' || first === 'DO' || first === 'CALL') {
        return DDL_SENTINEL;
    } else if (first === 'MERGE') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'INTO') return null;
        return bareTable(tokens[2]);
    } else if (first === 'SELECT') {
        let sawInto = false;
        let intoTarget = null;
        for (let i = 1; i < tokens.length; i++) {
            const upper = tokens[i].toUpperCase();
            if (upper === 'INTO' && !sawInto) {
                sawInto = true;
                continue;
            }
            if (sawInto && intoTarget === null) {
                if (upper === 'TEMPORARY' || upper === 'TEMP' || upper === 'UNLOGGED') {
                    continue;
                }
                intoTarget = tokens[i];
                continue;
            }
            if (sawInto && intoTarget !== null && upper === 'FROM') {
                return DDL_SENTINEL;
            }
            if (upper === 'FROM') {
                return null;
            }
        }
        return null;
    } else if (first === 'COPY') {
        if (tokens.length < 2) return null;
        const raw = tokens[1];
        if (raw.startsWith('(')) return null;
        const tablePart = raw.split('(')[0];
        for (let i = 2; i < tokens.length; i++) {
            const upper = tokens[i].toUpperCase();
            if (upper === 'FROM') return bareTable(tablePart);
            if (upper === 'TO') return null;
        }
        return null;
    } else if (first === 'WITH') {
        const restUpper = trimmed.slice(tokens[0].length).toUpperCase();
        for (const token of restUpper.split(/\s+/)) {
            const word = token.replace(/^\(+/, '');
            if (word === 'INSERT' || word === 'UPDATE' || word === 'DELETE') {
                return DDL_SENTINEL;
            }
        }
        return null;
    }

    return null;
}

function bareTable(raw) {
    let table = raw.split('(')[0];
    const parts = table.split('.');
    table = parts[parts.length - 1];
    return table.toLowerCase();
}

function extractTables(sql) {
    const tables = new Set();
    TABLE_PATTERN.lastIndex = 0;
    let match;
    while ((match = TABLE_PATTERN.exec(sql)) !== null) {
        const table = match[2].toLowerCase();
        if (!SQL_KEYWORDS.has(table)) {
            tables.add(table);
        }
    }
    return tables;
}

let _instance = null;
// Process-level guard so test-driven `_reset()` cycles don't accumulate
// dozens of `exit`/`SIGINT`/`SIGTERM` listeners and trip Node's default
// MaxListeners warning. We install the hooks exactly once per process.
let _exitHooksInstalledGlobal = false;

class NativeCache {
    constructor(opts = {}) {
        if (_instance) {
            // Re-entry against the existing singleton. Only `disabled` is
            // applicable here — other state (capacity, env-driven enable
            // flag, sockets) is fixed at first construction. This lets
            // `GoldLapel({ disableL1: true })` flip the bit on a cache
            // that a `wrap()` call already lazily created.
            if (opts && Object.prototype.hasOwnProperty.call(opts, 'disabled')) {
                _instance._disabled = !!opts.disabled;
            }
            return _instance;
        }
        this._cache = new Map();
        this._tableIndex = new Map();
        this._maxEntries = parseInt(process.env.GOLDLAPEL_NATIVE_CACHE_SIZE || '32768', 10);
        this._enabled = (process.env.GOLDLAPEL_NATIVE_CACHE || 'true').toLowerCase() !== 'false';
        // Explicit L1 toggle — distinct from `_enabled` (which gates the
        // whole module via env var). When `_disabled` is true the cache
        // acts as a no-op pass-through: get() always misses (and bumps
        // the misses counter so dashboards still see traffic), put() is
        // a no-op, no eviction occurs. Invalidation socket still
        // connects so telemetry continues to flow. Default false; flipped
        // via `goldlapel.start(url, { disableL1: true })`.
        this._disabled = !!opts.disabled;
        this._invalidationConnected = false;
        this._socket = null;
        this._reconnectTimer = null;
        this._reconnectAttempt = 0;
        this._invalidationPort = 0;
        this._buf = '';
        this.statsHits = 0;
        this.statsMisses = 0;
        this.statsInvalidations = 0;
        // L1 telemetry (2026-05-03). Eviction counter — was missing
        // before; bumped in `_evictOne`. Configurable opt-out: set
        // GOLDLAPEL_REPORT_STATS=false to disable all snapshot replies
        // and state-change emissions (cache continues to function;
        // invalidation listener still runs, only telemetry output is
        // suppressed).
        this.statsEvictions = 0;
        this._reportStats = (
            (process.env.GOLDLAPEL_REPORT_STATS || 'true').toLowerCase() !== 'false'
        );
        // Stable wrapper identity for the lifetime of the process. Lets
        // the proxy aggregate per wrapper across reconnects.
        this._wrapperId = randomUUID();
        this._wrapperLang = 'javascript';
        this._wrapperVersion = _readWrapperVersion();
        // Sliding window for eviction-rate state-change detection. A
        // bounded ring buffer; updates are O(1) amortised. We keep a
        // running sum (`_recentEvictionsSum`) updated on every record
        // so the threshold check is O(1) — the hot path is each put.
        // 1 = evicted on this put, 0 = inserted without eviction.
        this._recentEvictions = [];
        this._recentEvictionsIdx = 0;
        this._recentEvictionsSum = 0;
        // Latched state — only emit a state-change event when the state
        // transitions. Without latching the wrapper would re-emit every
        // put once the rate crossed the threshold.
        this._stateCacheFull = false;
        _instance = this;
    }

    get connected() { return this._invalidationConnected; }
    get enabled() { return this._enabled; }
    get size() { return this._cache.size; }

    get(sql, values) {
        if (!this._enabled || !this._invalidationConnected) return null;
        // L1 disabled: every get is a miss. We still bump the miss
        // counter so the dashboard sees real read traffic flowing
        // through the wrapper; hits stay 0 (no entries are ever
        // stored), evictions stay 0.
        if (this._disabled) {
            this.statsMisses++;
            return null;
        }
        const key = makeKey(sql, values);
        if (key === null) return null;
        const entry = this._cache.get(key);
        if (entry !== undefined) {
            // LRU: delete and re-insert to move to end
            this._cache.delete(key);
            this._cache.set(key, entry);
            this.statsHits++;
            return entry;
        }
        this.statsMisses++;
        return null;
    }

    put(sql, values, rows, fields) {
        if (!this._enabled || !this._invalidationConnected) return;
        // L1 disabled: drop the write silently. No eviction, no table
        // index update — the cache stays empty for the lifetime of the
        // process.
        if (this._disabled) return;
        const key = makeKey(sql, values);
        if (key === null) return;
        const tables = extractTables(sql);
        let evicted = 0;
        if (this._cache.has(key)) {
            this._cache.delete(key);
        } else if (this._cache.size >= this._maxEntries) {
            this._evictOne();
            evicted = 1;
        }
        this._cache.set(key, { rows, fields, tables });
        for (const table of tables) {
            let keys = this._tableIndex.get(table);
            if (!keys) {
                keys = new Set();
                this._tableIndex.set(table, keys);
            }
            keys.add(key);
        }
        this._recordEviction(evicted);
        // Eviction-rate threshold check. Emits `cache_full` /
        // `cache_recovered` on transition; no-op otherwise. Cheap
        // (constant-time sum over a bounded ring) so it's safe on the
        // hot path.
        this._maybeEmitEvictionRateStateChange();
    }

    invalidateTable(table) {
        table = table.toLowerCase();
        const keys = this._tableIndex.get(table);
        if (!keys) return;
        this._tableIndex.delete(table);
        for (const key of keys) {
            const entry = this._cache.get(key);
            this._cache.delete(key);
            if (entry) {
                for (const otherTable of entry.tables) {
                    if (otherTable !== table) {
                        const otherKeys = this._tableIndex.get(otherTable);
                        if (otherKeys) {
                            otherKeys.delete(key);
                            if (otherKeys.size === 0) this._tableIndex.delete(otherTable);
                        }
                    }
                }
            }
        }
        this.statsInvalidations += keys.size;
    }

    invalidateAll() {
        const count = this._cache.size;
        this._cache.clear();
        this._tableIndex.clear();
        this.statsInvalidations += count;
    }

    connectInvalidation(port) {
        if (this._socket) return;
        this._invalidationPort = port;
        this._reconnectAttempt = 0;
        this._installExitHooks();
        this._tryConnect();
    }

    stopInvalidation() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        if (this._socket) {
            // Emit `wrapper_disconnected` BEFORE tearing down the socket
            // so the proxy gets a clean farewell line. Best-effort: if
            // the underlying FD is already torn down on the kernel side
            // the write is a no-op (we swallow EPIPE in `_sendLine`).
            // The at-exit hook is the same emit, but if the user calls
            // `gl.stop()` explicitly we want the goodbye to happen now,
            // not at process exit (the socket will already be gone).
            try { this.emitWrapperDisconnected(); } catch {}
            this._socket.destroy();
            this._socket = null;
        }
        this._invalidationConnected = false;
    }

    _tryConnect() {
        const port = this._invalidationPort;
        const sockPath = `/tmp/goldlapel-${port}.sock`;

        let socket;
        if (platform() !== 'win32' && existsSync(sockPath)) {
            socket = createConnection({ path: sockPath });
        } else {
            socket = createConnection({ host: '127.0.0.1', port });
        }

        socket.setEncoding('utf8');

        socket.on('connect', () => {
            this._socket = socket;
            this._invalidationConnected = true;
            this._reconnectAttempt = 0;
            this._buf = '';
            // Announce ourselves so the proxy can register the wrapper
            // identity and its language/version. Stash the socket FIRST
            // (above) so `_emitStateChange` finds it.
            this._emitStateChange('wrapper_connected');
        });

        socket.on('data', (data) => {
            this._buf += data;
            let idx;
            while ((idx = this._buf.indexOf('\n')) !== -1) {
                const line = this._buf.slice(0, idx);
                this._buf = this._buf.slice(idx + 1);
                this._processSignal(line);
            }
        });

        socket.on('close', () => {
            if (this._invalidationConnected) {
                this._invalidationConnected = false;
                this.invalidateAll();
            }
            this._socket = null;
            this._scheduleReconnect();
        });

        socket.on('error', () => {
            // error fires before close — just let close handle cleanup
        });
    }

    _processSignal(line) {
        // Backwards-compat: unknown prefixes are silently ignored. Older
        // proxies sent only `I:` and `P:` (keepalive); newer proxies
        // route additional request types via `?:`. Forward-compat: this
        // dispatcher accepts any well-formed prefix and routes by type.
        if (line.startsWith('I:')) {
            const table = line.slice(2).trim();
            if (table === '*') {
                this.invalidateAll();
            } else {
                this.invalidateTable(table);
            }
        } else if (line.startsWith('?:')) {
            // Snapshot request from the proxy. Reply with R:<json>.
            this._processRequest(line.slice(2));
        }
        // P: keepalive — ignored. Anything else also ignored.
    }

    _evictOne() {
        const oldest = this._cache.keys().next().value;
        if (oldest === undefined) return;
        const entry = this._cache.get(oldest);
        this._cache.delete(oldest);
        if (entry) {
            for (const table of entry.tables) {
                const keys = this._tableIndex.get(table);
                if (keys) {
                    keys.delete(oldest);
                    if (keys.size === 0) this._tableIndex.delete(table);
                }
            }
        }
        this.statsEvictions++;
    }

    // ─── L1 telemetry: sliding window ──────────────────────────────────────

    _recordEviction(evicted) {
        // Bounded ring — once at capacity, overwrites oldest in O(1).
        // Records the put() outcome (1 evicted, 0 inserted) and keeps
        // the running window sum in sync so the threshold check stays
        // O(1) on the hot path.
        if (this._recentEvictions.length < EVICT_RATE_WINDOW) {
            this._recentEvictions.push(evicted);
            this._recentEvictionsSum += evicted;
        } else {
            const old = this._recentEvictions[this._recentEvictionsIdx];
            this._recentEvictions[this._recentEvictionsIdx] = evicted;
            this._recentEvictionsSum += evicted - old;
            this._recentEvictionsIdx = (this._recentEvictionsIdx + 1) % EVICT_RATE_WINDOW;
        }
    }

    // ─── L1 telemetry: snapshot + emit ─────────────────────────────────────

    _buildSnapshot() {
        // The proxy computes deltas across snapshots; we just expose the
        // raw counters. Counter reads in JS are atomic (single-threaded),
        // so no lock is needed to keep the snapshot internally
        // consistent (no `await` between reads either).
        const snap = {
            wrapper_id: this._wrapperId,
            lang: this._wrapperLang,
            version: this._wrapperVersion,
            hits: this.statsHits,
            misses: this.statsMisses,
            evictions: this.statsEvictions,
            invalidations: this.statsInvalidations,
            current_size_entries: this._cache.size,
            capacity_entries: this._maxEntries,
        };
        // Surface the explicit-disable bit so the dashboard can render
        // "L1 off" rather than misreading hits=0 as a cold cache. Only
        // emit the field when truthy to keep the on-the-wire snapshot
        // shape identical for the common (enabled) case.
        if (this._disabled) snap.l1_disabled = true;
        return snap;
    }

    _sendLine(line) {
        // Best-effort line write. Socket errors are swallowed: the
        // 'close' / 'error' handlers will rebuild the connection. We
        // never throw from the emit path because emissions happen on
        // the cache hot path (put → cache_full) and synchronously from
        // process exit hooks (wrapper_disconnected).
        //
        // Concurrency: Node is single-threaded for JS execution, so two
        // emits cannot happen "at the same time" — each `socket.write()`
        // call enqueues its bytes atomically into the socket's outgoing
        // stream. No send-lock is needed (unlike the Python wrapper,
        // where the recv thread and the calling thread can race).
        if (!this._reportStats) return;
        const sock = this._socket;
        if (!sock || sock.destroyed) return;
        const data = line.endsWith('\n') ? line : line + '\n';
        try {
            sock.write(data);
        } catch {
            // Connection dead — close handler will rebuild on next
            // event-loop tick. Don't try to repair from here.
        }
    }

    _emitStateChange(state) {
        if (!this._reportStats) return;
        const payload = this._buildSnapshot();
        payload.state = state;
        payload.ts_ms = Date.now();
        let line;
        try {
            line = 'S:' + JSON.stringify(payload);
        } catch {
            return;
        }
        this._sendLine(line);
    }

    _emitResponse(snapshot) {
        if (!this._reportStats) return;
        const payload = snapshot ?? this._buildSnapshot();
        if (payload.ts_ms === undefined) payload.ts_ms = Date.now();
        let line;
        try {
            line = 'R:' + JSON.stringify(payload);
        } catch {
            return;
        }
        this._sendLine(line);
    }

    _maybeEmitEvictionRateStateChange() {
        // Hysteresis-guarded: crossing HIGH emits cache_full, falling
        // back below LOW emits cache_recovered, rates between the two
        // leave the latched state unchanged (no flapping). Need a full
        // window before reporting — a single eviction in 3 puts is
        // noise, not signal.
        const n = this._recentEvictions.length;
        if (n < EVICT_RATE_WINDOW) return;
        const rate = this._recentEvictionsSum / n;
        let emit = null;
        if (!this._stateCacheFull && rate >= EVICT_RATE_HIGH) {
            this._stateCacheFull = true;
            emit = 'cache_full';
        } else if (this._stateCacheFull && rate <= EVICT_RATE_LOW) {
            this._stateCacheFull = false;
            emit = 'cache_recovered';
        }
        if (emit !== null) {
            this._emitStateChange(emit);
        }
    }

    _processRequest(raw) {
        // Today the only request is `snapshot`. Future request types
        // can extend this without breaking older proxies — they'd just
        // ignore unknown R: replies, but only the proxy that sent ?:<x>
        // expects a reply, so the contract is local to the request.
        const body = raw ? raw.trim() : '';
        if (body === '' || body === 'snapshot') {
            this._emitResponse();
        }
    }

    emitWrapperDisconnected() {
        // Called from at-exit hooks before the process tears down.
        // Best-effort; the socket may already be torn down (the proxy
        // will time us out anyway in that case).
        this._emitStateChange('wrapper_disconnected');
    }

    _installExitHooks() {
        // Process-level guard — we only ever install one set of
        // listeners per process. Test cycles that reset the singleton
        // would otherwise accumulate one set per `new NativeCache()`
        // and trip Node's MaxListeners warning around the 10th cycle.
        if (_exitHooksInstalledGlobal) return;
        _exitHooksInstalledGlobal = true;

        // The hooks always re-resolve `_instance` so a reset between
        // install and exit fires the *current* singleton's emit (or
        // a no-op if nothing is wired right now).
        const onExit = () => {
            try { _instance && _instance.emitWrapperDisconnected(); } catch {}
        };

        // SIGINT / SIGTERM: run our emit, then let Node's default
        // disposition exit the process. We only force-exit if no
        // other handler is registered — otherwise we'd hijack the
        // user's intentional signal handling.
        const onSignal = (sig, code) => {
            try { _instance && _instance.emitWrapperDisconnected(); } catch {}
            if (process.listenerCount(sig) <= 1) {
                process.exit(code);
            }
        };

        process.on('exit', onExit);
        process.on('SIGINT', () => onSignal('SIGINT', 130));
        process.on('SIGTERM', () => onSignal('SIGTERM', 143));
    }

    // ─── Reconnect ─────────────────────────────────────────────────────────

    _scheduleReconnect() {
        if (this._reconnectTimer) return;
        const delay = Math.min(2 ** this._reconnectAttempt, 15) * 1000;
        this._reconnectAttempt++;
        this._reconnectTimer = setTimeout(() => {
            this._reconnectTimer = null;
            this._tryConnect();
        }, delay);
        // Don't keep process alive just for reconnection
        if (this._reconnectTimer.unref) this._reconnectTimer.unref();
    }

    static _reset() {
        if (_instance) {
            _instance.stopInvalidation();
            _instance = null;
        }
    }
}

export {
    NativeCache, makeKey, detectWrite, extractTables,
    DDL_SENTINEL, TX_START, TX_END,
    EVICT_RATE_WINDOW, EVICT_RATE_HIGH, EVICT_RATE_LOW,
};
