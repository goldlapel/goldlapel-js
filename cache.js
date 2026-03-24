import { createConnection } from 'net';
import { existsSync } from 'fs';
import { platform } from 'os';

const DDL_SENTINEL = '__ddl__';

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
    } else if (first === 'CREATE' || first === 'ALTER' || first === 'DROP') {
        return DDL_SENTINEL;
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

class NativeCache {
    constructor() {
        if (_instance) return _instance;
        this._cache = new Map();
        this._tableIndex = new Map();
        this._maxEntries = parseInt(process.env.GOLDLAPEL_NATIVE_CACHE_SIZE || '32768', 10);
        this._enabled = (process.env.GOLDLAPEL_NATIVE_CACHE || 'true').toLowerCase() !== 'false';
        this._invalidationConnected = false;
        this._socket = null;
        this._reconnectTimer = null;
        this._reconnectAttempt = 0;
        this._invalidationPort = 0;
        this._buf = '';
        this.statsHits = 0;
        this.statsMisses = 0;
        this.statsInvalidations = 0;
        _instance = this;
    }

    get connected() { return this._invalidationConnected; }
    get enabled() { return this._enabled; }
    get size() { return this._cache.size; }

    get(sql, values) {
        if (!this._enabled || !this._invalidationConnected) return null;
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
        const key = makeKey(sql, values);
        if (key === null) return;
        const tables = extractTables(sql);
        if (this._cache.has(key)) {
            this._cache.delete(key);
        } else if (this._cache.size >= this._maxEntries) {
            this._evictOne();
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
        this._tryConnect();
    }

    stopInvalidation() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        if (this._socket) {
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
        if (line.startsWith('I:')) {
            const table = line.slice(2).trim();
            if (table === '*') {
                this.invalidateAll();
            } else {
                this.invalidateTable(table);
            }
        }
        // P: keepalive — ignore
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
    }

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

export { NativeCache, makeKey, detectWrite, extractTables, DDL_SENTINEL, TX_START, TX_END };
