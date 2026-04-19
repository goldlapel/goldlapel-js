import { spawn, execFileSync } from 'child_process';
import { AsyncLocalStorage } from 'node:async_hooks';
import { NativeCache } from './cache.js';
import { createConnection } from 'net';
import { existsSync } from 'fs';
import { join, dirname } from 'path';
import { platform, arch } from 'os';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import {
    publish, subscribe, enqueue, dequeue,
    incr, getCounter,
    zadd, zincrby, zrange, zrank, zscore, zrem,
    geoadd, georadius, geodist,
    hset, hget, hgetall, hdel,
    countDistinct,
    script,
    streamAdd, streamCreateGroup, streamRead, streamAck, streamClaim,
    search, searchFuzzy, searchPhonetic, similar, suggest,
    facets, aggregate, createSearchConfig,
    percolateAdd, percolate, percolateDelete,
    analyze, explainScore,
    docCreateCollection,
    docInsert, docInsertMany, docFind, docFindCursor, docFindOne,
    docUpdate, docUpdateOne, docDelete, docDeleteOne,
    docFindOneAndUpdate, docFindOneAndDelete,
    docDistinct,
    docCount, docCreateIndex, docAggregate,
    docWatch, docUnwatch,
    docCreateTtlIndex, docRemoveTtlIndex,
    docCreateCapped, docRemoveCap,
} from './utils.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const DEFAULT_PORT = 7932;
const DEFAULT_DASHBOARD_PORT = 7933;
const STARTUP_TIMEOUT = 10000;
const STARTUP_POLL_INTERVAL = 50;

const VALID_CONFIG_KEYS = new Set([
    'mode', 'minPatternCount', 'refreshIntervalSecs', 'patternTtlSecs',
    'maxTablesPerView', 'maxColumnsPerView', 'deepPaginationThreshold',
    'reportIntervalSecs', 'resultCacheSize', 'batchCacheSize',
    'batchCacheTtlSecs', 'poolSize', 'poolTimeoutSecs',
    'poolMode', 'mgmtIdleTimeout', 'fallback', 'readAfterWriteSecs',
    'n1Threshold', 'n1WindowMs', 'n1CrossThreshold',
    'tlsCert', 'tlsKey', 'tlsClientCa', 'config', 'dashboardPort',
    'disableMatviews', 'disableConsolidation', 'disableBtreeIndexes',
    'disableTrigramIndexes', 'disableExpressionIndexes',
    'disablePartialIndexes', 'disableRewrite', 'disablePreparedCache',
    'disableResultCache', 'disablePool',
    'disableN1', 'disableN1CrossConnection', 'disableShadowMode',
    'enableCoalescing', 'replica', 'excludeTables',
    'invalidationPort',
]);

const BOOLEAN_KEYS = new Set([
    'disableMatviews', 'disableConsolidation', 'disableBtreeIndexes',
    'disableTrigramIndexes', 'disableExpressionIndexes',
    'disablePartialIndexes', 'disableRewrite', 'disablePreparedCache',
    'disableResultCache', 'disablePool',
    'disableN1', 'disableN1CrossConnection', 'disableShadowMode',
    'enableCoalescing',
]);

const LIST_KEYS = new Set([
    'replica', 'excludeTables',
]);

const VALID_LOG_LEVELS = new Set(['trace', 'debug', 'info', 'warn', 'error']);

export function configKeys() {
    return new Set(VALID_CONFIG_KEYS);
}

export function _configToArgs(config) {
    if (!config || Object.keys(config).length === 0) return [];

    const unknown = Object.keys(config).filter(k => !VALID_CONFIG_KEYS.has(k));
    if (unknown.length > 0) {
        throw new Error(`Unknown config keys: ${unknown.sort().join(', ')}`);
    }

    const args = [];
    for (const [key, value] of Object.entries(config)) {
        const flag = '--' + key.replace(/[A-Z]/g, m => '-' + m.toLowerCase());

        if (BOOLEAN_KEYS.has(key)) {
            if (typeof value !== 'boolean') {
                throw new TypeError(
                    `Config key '${key}' expects a boolean, got ${typeof value}`
                );
            }
            if (value) args.push(flag);
        } else if (LIST_KEYS.has(key)) {
            const items = typeof value === 'string' ? [value] : value;
            if (!Array.isArray(items)) {
                throw new TypeError(
                    `Config key '${key}' expects an array or string, got ${typeof value}`
                );
            }
            for (const item of items) {
                args.push(flag, String(item));
            }
        } else {
            args.push(flag, String(value));
        }
    }

    return args;
}

export function _isMusl() {
    const machine = arch();
    const linkerArch = machine === 'x64' ? 'x86_64' : machine === 'arm64' ? 'aarch64' : machine;
    return existsSync(`/lib/ld-musl-${linkerArch}.so.1`);
}

export function _findBinary() {
    // 1. Explicit override via env var
    const envPath = process.env.GOLDLAPEL_BINARY;
    if (envPath) {
        if (existsSync(envPath)) return envPath;
        throw new Error(`GOLDLAPEL_BINARY points to ${envPath} but file not found`);
    }

    // 2. Bundled binary (inside the installed package)
    const sys = platform();
    const machine = arch();
    const archName = machine === 'x64' ? 'x86_64' : machine === 'arm64' ? 'aarch64' : machine;

    const isWindows = sys === 'win32';
    let binaryName;
    if (sys === 'linux') {
        binaryName = `goldlapel-linux-${archName}${_isMusl() ? '-musl' : ''}`;
    } else if (sys === 'darwin') {
        binaryName = `goldlapel-darwin-${archName}`;
    } else if (isWindows) {
        binaryName = `goldlapel-windows-${archName}.exe`;
    } else {
        binaryName = `goldlapel-${sys}-${archName}`;
    }

    const bundled = join(__dirname, 'bin', binaryName);
    if (existsSync(bundled)) return bundled;

    // 3. Platform-specific npm package (@goldlapel/linux-x64, etc.)
    const npmPlatform = sys === 'darwin' ? 'darwin' : isWindows ? 'win' : 'linux';
    let npmPkgName = `@goldlapel/${npmPlatform}-${machine}`;
    if (sys === 'linux' && _isMusl()) npmPkgName += '-musl';
    const npmBinaryName = isWindows ? 'goldlapel.exe' : 'goldlapel';
    try {
        const pkgDir = dirname(require.resolve(`${npmPkgName}/package.json`));
        const npmBinary = join(pkgDir, npmBinaryName);
        if (existsSync(npmBinary)) return npmBinary;
    } catch {}

    // 4. On PATH
    try {
        const whichCmd = isWindows ? 'where' : 'which';
        const onPath = execFileSync(whichCmd, [isWindows ? 'goldlapel.exe' : 'goldlapel'], { encoding: 'utf8' }).trim().split('\n')[0].trim();
        if (onPath && existsSync(onPath)) return onPath;
    } catch {}

    throw new Error(
        'Gold Lapel binary not found. Set GOLDLAPEL_BINARY env var, ' +
        "install the platform-specific package, or ensure 'goldlapel' is on PATH."
    );
}

export function _makeProxyUrl(upstream, port) {
    // Build a proxy URL: replace host with localhost and set the proxy port.
    // Uses regex instead of URL class to avoid decoding percent-encoded characters
    // in passwords (e.g. %40 for @), which would corrupt the URL on reconstruction.

    // Split userinfo from host at the LAST @ (passwords may contain literal @).
    // This two-step approach avoids backtracking issues where (?:.*@)? is optional
    // and the regex engine drops the @ anchor, misinterpreting password digits as a port.
    const schemeMatch = upstream.match(/^(postgres(?:ql)?:\/\/)(.*)/);
    if (schemeMatch) {
        const scheme = schemeMatch[1];
        const rest = schemeMatch[2];

        // The authority ends at the first / ? or # — only look for @ within it
        const authEnd = rest.search(/[/?#]/);
        const authority = authEnd === -1 ? rest : rest.slice(0, authEnd);
        const pathEtc = authEnd === -1 ? '' : rest.slice(authEnd);

        // Find the last @ within the authority to split userinfo from host
        const atIdx = authority.lastIndexOf('@');
        let userinfo, hostPart;
        if (atIdx !== -1) {
            userinfo = authority.slice(0, atIdx + 1);  // includes trailing @
            hostPart = authority.slice(atIdx + 1);
        } else {
            userinfo = '';
            hostPart = authority;
        }

        // Replace host[:port] with localhost:proxyPort
        return `${scheme}${userinfo}localhost:${port}${pathEtc}`;
    }

    // bare host:port (only if not a URL — guard against splitting on scheme colons)
    if (!upstream.includes('://') && upstream.includes(':')) {
        return `localhost:${port}`;
    }

    // bare host
    return `localhost:${port}`;
}

export function _waitForPort(host, port, timeout) {
    return new Promise((resolve) => {
        const deadline = Date.now() + timeout;

        function attempt() {
            if (Date.now() >= deadline) {
                resolve(false);
                return;
            }

            const sock = createConnection({ host, port }, () => {
                sock.destroy();
                resolve(true);
            });

            sock.on('error', () => {
                sock.destroy();
                setTimeout(attempt, STARTUP_POLL_INTERVAL);
            });

            sock.setTimeout(500, () => {
                sock.destroy();
                setTimeout(attempt, STARTUP_POLL_INTERVAL);
            });
        }

        attempt();
    });
}

// ─── Driver auto-detection ─────────────────────────────────────────────────
//
// At import time we attempt to find a Postgres driver in the user's project.
// Order: pg → postgres (postgres.js) → @vercel/postgres. The first one found
// wins. Detection is eager so we can fail fast if none is installed, but the
// actual module is loaded lazily (on the first start() call) to keep import
// cheap and avoid loading native addons for users who only want wrapper
// methods with their own conn.

const DRIVER_CANDIDATES = ['pg', 'postgres', '@vercel/postgres'];

export function _detectDriver() {
    // Allow override via env var for tests
    if (process.env.GOLDLAPEL_DRIVER === 'none') return null;
    if (process.env.GOLDLAPEL_DRIVER) {
        return process.env.GOLDLAPEL_DRIVER;
    }

    for (const name of DRIVER_CANDIDATES) {
        try {
            require.resolve(name);
            return name;
        } catch {}
    }
    return null;
}

let _driverName = _detectDriver();

export function _driverNotFoundError() {
    return new Error(
        'No supported Postgres driver found. Install one of: ' +
        DRIVER_CANDIDATES.join(', ') +
        ". For example: `npm install pg`. If you only need the proxy URL " +
        'without an internal connection, pass `{ noConnect: true }` to start().'
    );
}

export async function _connectWithDriver(driverName, url) {
    // Returns an object with { conn, close } where conn has a .query(text, values)
    // method that resolves to { rows, fields, rowCount, command } shape.
    if (driverName === 'pg') {
        const pg = await import('pg');
        const Client = pg.default?.Client ?? pg.Client;
        const client = new Client({ connectionString: url });
        await client.connect();
        return { conn: client, close: () => client.end() };
    }
    if (driverName === 'postgres') {
        // postgres.js — template-tag style. Wrap to the shape utils expect.
        const mod = await import('postgres');
        const postgres = mod.default ?? mod;
        const sql = postgres(url);
        const conn = {
            _sql: sql,
            async query(text, values) {
                const args = values == null ? [] : values;
                // sql.unsafe(text, args) runs parameterized; returns a rows-like array
                const rows = await sql.unsafe(text, args);
                return {
                    rows: Array.from(rows),
                    fields: rows.columns ?? [],
                    rowCount: rows.count ?? rows.length ?? 0,
                    command: rows.command ?? '',
                };
            },
            on() {}, off() {}, once() {},
        };
        return { conn, close: () => sql.end() };
    }
    if (driverName === '@vercel/postgres') {
        const { createClient } = await import('@vercel/postgres');
        const client = createClient({ connectionString: url });
        await client.connect();
        const conn = {
            _client: client,
            async query(text, values) {
                return client.query(text, values);
            },
            on(ev, h) { return client.on?.(ev, h); },
            off(ev, h) { return client.off?.(ev, h); },
            once(ev, h) { return client.once?.(ev, h); },
        };
        return { conn, close: () => client.end() };
    }
    throw new Error(`Unsupported driver: ${driverName}`);
}

// ─── GoldLapel instance ────────────────────────────────────────────────────

const _connScope = new AsyncLocalStorage();
const _liveInstances = new Set();
let _cleanupRegistered = false;

function _cleanup() {
    for (const inst of _liveInstances) {
        try { inst.stop(); } catch {}
    }
    _liveInstances.clear();
}

export class GoldLapel {
    constructor(upstream, {
        port, dashboardPort, logLevel, config, extraArgs, noConnect,
    } = {}) {
        this._upstream = upstream;
        this._port = port ?? DEFAULT_PORT;
        this._dashboardPort = dashboardPort !== undefined
            ? Number(dashboardPort)
            : (config && config.dashboardPort !== undefined
                ? Number(config.dashboardPort)
                : DEFAULT_DASHBOARD_PORT);
        this._logLevel = logLevel;
        this._config = config || {};
        this._extraArgs = extraArgs || [];
        this._noConnect = !!noConnect;
        this._process = null;
        this._proxyUrl = null;
        this._defaultConn = null;
        this._defaultClose = null;
        this._stopped = false;
    }

    async _spawn() {
        if (this._process && this._process.exitCode === null) {
            return;
        }

        if (this._logLevel !== undefined && !VALID_LOG_LEVELS.has(this._logLevel)) {
            throw new Error(
                `Invalid logLevel '${this._logLevel}'. Must be one of: ` +
                [...VALID_LOG_LEVELS].join(', ')
            );
        }

        const binary = _findBinary();
        const args = [
            '--upstream', this._upstream,
            '--proxy-port', String(this._port),
            ..._configToArgs(this._config),
            ...this._extraArgs,
        ];
        if (this._logLevel) {
            args.push('--log-level', this._logLevel);
        }

        const env = { ...process.env };
        if (!env.GOLDLAPEL_CLIENT) env.GOLDLAPEL_CLIENT = 'node';
        this._process = spawn(binary, args, {
            stdio: ['ignore', 'ignore', 'pipe'],
            env,
        });

        let stderr = '';
        const onData = (chunk) => { stderr += chunk; };
        this._process.stderr.on('data', onData);

        this._process.on('error', (err) => { stderr += err.message; });

        const ready = await Promise.race([
            _waitForPort('127.0.0.1', this._port, STARTUP_TIMEOUT),
            new Promise((resolve) => {
                this._process.on('exit', () => resolve(false));
            }),
        ]);
        if (!ready) {
            this._process.stderr.removeListener('data', onData);
            this._process.kill();
            throw new Error(
                `Gold Lapel failed to start on port ${this._port} ` +
                `within ${STARTUP_TIMEOUT / 1000}s.\nstderr: ${stderr}`
            );
        }

        this._process.stderr.removeListener('data', onData);

        this._proxyUrl = _makeProxyUrl(this._upstream, this._port);
    }

    async _openDefaultConn() {
        if (this._noConnect) return;
        if (this._defaultConn) return;

        const driver = _driverName;
        if (!driver) {
            throw _driverNotFoundError();
        }
        const { conn, close } = await _connectWithDriver(driver, this._proxyUrl);
        this._defaultConn = conn;
        this._defaultClose = close;
    }

    async _printBanner() {
        if (this._dashboardPort) {
            console.log(`goldlapel → :${this._port} (proxy) | http://127.0.0.1:${this._dashboardPort} (dashboard)`);
        } else {
            console.log(`goldlapel → :${this._port} (proxy)`);
        }
    }

    async stop() {
        if (this._stopped) return;
        this._stopped = true;

        _liveInstances.delete(this);

        const close = this._defaultClose;
        this._defaultConn = null;
        this._defaultClose = null;
        if (close) {
            try { await close(); } catch {}
        }

        const proc = this._process;
        this._process = null;
        this._proxyUrl = null;
        if (proc && proc.exitCode === null) {
            proc.kill('SIGTERM');
            setTimeout(() => {
                if (proc.exitCode === null) {
                    proc.kill('SIGKILL');
                }
            }, 5000);
        }
    }

    async [Symbol.asyncDispose]() {
        return this.stop();
    }

    // ─── Connection resolution ─────────────────────────────────────────────

    _resolveConn(override) {
        // Priority: explicit `{ conn }` > scoped `using()` conn > default internal conn.
        if (override !== undefined && override !== null) return override;
        const scoped = _connScope.getStore();
        if (scoped) return scoped;
        if (!this._defaultConn) {
            throw new Error(
                'Not connected. Either call start() on this GoldLapel instance, ' +
                'provide a conn via { conn } on this call, or use gl.using(conn, cb).'
            );
        }
        return this._defaultConn;
    }

    // Scoped connection override. The callback receives this instance; any
    // wrapper method invoked from within the callback (including across awaits)
    // will use the supplied `conn` unless it passes its own `{ conn }` override.
    async using(conn, callback) {
        if (typeof callback !== 'function') {
            throw new TypeError('gl.using(conn, callback): callback must be a function');
        }
        return _connScope.run(conn, () => callback(this));
    }

    get url() {
        return this._proxyUrl;
    }

    get running() {
        return this._process !== null && this._process.exitCode === null;
    }

    get dashboardUrl() {
        if (this._dashboardPort && this._process && this._process.exitCode === null) {
            return `http://127.0.0.1:${this._dashboardPort}`;
        }
        return null;
    }

    // ─── Wrapper methods ───────────────────────────────────────────────────
    //
    // Every method accepts an optional trailing `{ conn }` option that
    // overrides the default connection for that call. If absent, the
    // connection is resolved from the `using()` scope or the internal default.

    // Document store
    async docCreateCollection(...args) { return _call(this, docCreateCollection, args); }
    async docInsert(...args) { return _call(this, docInsert, args); }
    async docInsertMany(...args) { return _call(this, docInsertMany, args); }
    async docFind(...args) { return _call(this, docFind, args); }
    async *docFindCursor(...args) {
        const { conn, rest } = _splitArgs(this, args);
        yield* docFindCursor(conn, ...rest);
    }
    async docFindOne(...args) { return _call(this, docFindOne, args); }
    async docUpdate(...args) { return _call(this, docUpdate, args); }
    async docUpdateOne(...args) { return _call(this, docUpdateOne, args); }
    async docDelete(...args) { return _call(this, docDelete, args); }
    async docDeleteOne(...args) { return _call(this, docDeleteOne, args); }
    async docFindOneAndUpdate(...args) { return _call(this, docFindOneAndUpdate, args); }
    async docFindOneAndDelete(...args) { return _call(this, docFindOneAndDelete, args); }
    async docDistinct(...args) { return _call(this, docDistinct, args); }
    async docCount(...args) { return _call(this, docCount, args); }
    async docCreateIndex(...args) { return _call(this, docCreateIndex, args); }
    async docAggregate(...args) { return _call(this, docAggregate, args); }
    async docWatch(...args) { return _call(this, docWatch, args); }
    async docUnwatch(...args) { return _call(this, docUnwatch, args); }
    async docCreateTtlIndex(...args) { return _call(this, docCreateTtlIndex, args); }
    async docRemoveTtlIndex(...args) { return _call(this, docRemoveTtlIndex, args); }
    async docCreateCapped(...args) { return _call(this, docCreateCapped, args); }
    async docRemoveCap(...args) { return _call(this, docRemoveCap, args); }

    // Search
    async search(...args) { return _call(this, search, args); }
    async searchFuzzy(...args) { return _call(this, searchFuzzy, args); }
    async searchPhonetic(...args) { return _call(this, searchPhonetic, args); }
    async similar(...args) { return _call(this, similar, args); }
    async suggest(...args) { return _call(this, suggest, args); }
    async facets(...args) { return _call(this, facets, args); }
    async aggregate(...args) { return _call(this, aggregate, args); }
    async createSearchConfig(...args) { return _call(this, createSearchConfig, args); }

    // Percolation
    async percolateAdd(...args) { return _call(this, percolateAdd, args); }
    async percolate(...args) { return _call(this, percolate, args); }
    async percolateDelete(...args) { return _call(this, percolateDelete, args); }

    // Analysis
    async analyze(...args) { return _call(this, analyze, args); }
    async explainScore(...args) { return _call(this, explainScore, args); }

    // Pub/Sub & Queues
    async publish(...args) { return _call(this, publish, args); }
    async subscribe(...args) { return _call(this, subscribe, args); }
    async enqueue(...args) { return _call(this, enqueue, args); }
    async dequeue(...args) { return _call(this, dequeue, args); }

    // Counters
    async incr(...args) { return _call(this, incr, args); }
    async getCounter(...args) { return _call(this, getCounter, args); }

    // Hash maps
    async hset(...args) { return _call(this, hset, args); }
    async hget(...args) { return _call(this, hget, args); }
    async hgetall(...args) { return _call(this, hgetall, args); }
    async hdel(...args) { return _call(this, hdel, args); }

    // Sorted sets
    async zadd(...args) { return _call(this, zadd, args); }
    async zincrby(...args) { return _call(this, zincrby, args); }
    async zrange(...args) { return _call(this, zrange, args); }
    async zrank(...args) { return _call(this, zrank, args); }
    async zscore(...args) { return _call(this, zscore, args); }
    async zrem(...args) { return _call(this, zrem, args); }

    // Geo
    async geoadd(...args) { return _call(this, geoadd, args); }
    async georadius(...args) { return _call(this, georadius, args); }
    async geodist(...args) { return _call(this, geodist, args); }

    // Misc
    async countDistinct(...args) { return _call(this, countDistinct, args); }
    async script(...args) { return _call(this, script, args); }

    // Streams
    async streamAdd(...args) { return _call(this, streamAdd, args); }
    async streamCreateGroup(...args) { return _call(this, streamCreateGroup, args); }
    async streamRead(...args) { return _call(this, streamRead, args); }
    async streamAck(...args) { return _call(this, streamAck, args); }
    async streamClaim(...args) { return _call(this, streamClaim, args); }
}

// Splits out an optional `conn` from the trailing options arg.
//
// If the last argument is a plain object that contains a `conn` property, we
// pull it out and pass the remaining keys through to the underlying method.
// If `conn` is the only key, the entire options object is dropped so the
// method's own default options kick in.
//
// This lets callers write any of:
//   gl.docInsert('t', doc)                          // no override
//   gl.docInsert('t', doc, { conn: client })        // pure override
//   gl.search('t', 'c', 'q', { limit: 10 })         // normal options
//   gl.search('t', 'c', 'q', { limit: 10, conn: c}) // options + override
function _splitArgs(gl, args) {
    let override;
    let rest = args;
    if (args.length > 0) {
        const last = args[args.length - 1];
        if (
            last !== null &&
            typeof last === 'object' &&
            !Array.isArray(last) &&
            Object.prototype.hasOwnProperty.call(last, 'conn')
        ) {
            override = last.conn;
            const otherKeys = Object.keys(last).filter((k) => k !== 'conn');
            if (otherKeys.length === 0) {
                rest = args.slice(0, -1);
            } else {
                const trimmed = {};
                for (const k of otherKeys) trimmed[k] = last[k];
                rest = [...args.slice(0, -1), trimmed];
            }
        }
    }
    const conn = gl._resolveConn(override);
    return { conn, rest };
}

function _call(gl, fn, args) {
    const { conn, rest } = _splitArgs(gl, args);
    return fn(conn, ...rest);
}

// ─── Factory ───────────────────────────────────────────────────────────────

/**
 * Start a Gold Lapel proxy and return an instance for use with wrapper methods.
 *
 * ```js
 * import * as goldlapel from 'goldlapel';
 * const gl = await goldlapel.start('postgresql://user:pass@db/mydb');
 * const rows = await gl.search('articles', 'body', 'postgres tuning');
 * await gl.stop();
 * ```
 *
 * @param {string} upstream  Postgres connection string (upstream database).
 * @param {object} [opts]
 * @param {number} [opts.port=7932]  Proxy listen port.
 * @param {number} [opts.dashboardPort=7933]  Dashboard port. 0 disables.
 * @param {'trace'|'debug'|'info'|'warn'|'error'} [opts.logLevel]  Binary log level.
 * @param {object} [opts.config]  camelCase → CLI flags (see configKeys()).
 * @param {string[]} [opts.extraArgs]  Raw CLI flags passed to the binary.
 * @param {boolean} [opts.noConnect]  Skip opening the internal driver connection.
 * @returns {Promise<GoldLapel>}
 */
export async function start(upstream, opts = {}) {
    const gl = new GoldLapel(upstream, opts);
    _liveInstances.add(gl);
    if (!_cleanupRegistered) {
        process.on('exit', _cleanup);
        _cleanupRegistered = true;
    }

    try {
        await gl._spawn();
        await gl._openDefaultConn();
        await gl._printBanner();
    } catch (err) {
        try { await gl.stop(); } catch {}
        throw err;
    }

    return gl;
}

// ─── Module-level exports ──────────────────────────────────────────────────

export { wrap } from './wrap.js';
export { NativeCache } from './cache.js';
export {
    publish, subscribe, enqueue, dequeue,
    incr, getCounter,
    zadd, zincrby, zrange, zrank, zscore, zrem,
    geoadd, georadius, geodist,
    hset, hget, hgetall, hdel,
    countDistinct,
    script,
    streamAdd, streamCreateGroup, streamRead, streamAck, streamClaim,
    search, searchFuzzy, searchPhonetic, similar, suggest,
    facets, aggregate, createSearchConfig,
    percolateAdd, percolate, percolateDelete,
    analyze, explainScore,
    docCreateCollection,
    docInsert, docInsertMany, docFind, docFindCursor, docFindOne,
    docUpdate, docUpdateOne, docDelete, docDeleteOne,
    docFindOneAndUpdate, docFindOneAndDelete,
    docDistinct,
    docCount, docCreateIndex, docAggregate,
    docWatch, docUnwatch,
    docCreateTtlIndex, docRemoveTtlIndex,
    docCreateCapped, docRemoveCap,
} from './utils.js';

export default { GoldLapel, start, configKeys, _configToArgs };
