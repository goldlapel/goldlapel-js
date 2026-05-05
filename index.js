import { spawn, execFileSync } from 'child_process';
import { AsyncLocalStorage } from 'node:async_hooks';
import { NativeCache } from './cache.js';
import { wrap } from './wrap.js';
import { createConnection } from 'net';
import { existsSync } from 'fs';
import { join, dirname } from 'path';
import { platform, arch } from 'os';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';
import {
    publish, subscribe,
    countDistinct,
    script,
    // Phase 5 family helpers — exported for power users / direct use with
    // `{ patterns }` from the namespace classes. Most callers should use the
    // namespaces (gl.counters / gl.zsets / gl.hashes / gl.queues / gl.geos).
    counterIncr, counterDecr, counterSet, counterGet, counterDelete, counterCountKeys,
    zsetAdd, zsetIncrBy, zsetScore, zsetRank, zsetRange, zsetRangeByScore, zsetRemove, zsetCard,
    hashSet, hashGet, hashGetAll, hashKeys, hashValues, hashExists, hashDelete, hashLen,
    queueEnqueue, queueClaim, queueAck, queueAbandon, queueExtend, queuePeek,
    queueCountReady, queueCountClaimed,
    geoAdd, geoPos, geoDist, geoRadius, geoRadiusByMember, geoRemove, geoCount,
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
import { DocumentsAPI } from './documents.js';
import { StreamsAPI } from './streams.js';
import { CountersAPI } from './counters.js';
import { ZsetsAPI } from './zsets.js';
import { HashesAPI } from './hashes.js';
import { QueuesAPI } from './queues.js';
import { GeosAPI } from './geos.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const DEFAULT_PROXY_PORT = 7932;
const STARTUP_TIMEOUT = 10000;
const STARTUP_POLL_INTERVAL = 50;

// Wrapper version, read from package.json. CI rewrites the version in
// package.json from the git tag at publish time; local dev installs read
// "0.0.0". Used to build the application_name marker on PG connections so
// the proxy can classify wrapper-vs-raw traffic and gate L2 result cache.
function _wrapperVersion() {
    try {
        const pkg = require(join(__dirname, 'package.json'));
        return pkg.version || '0.0.0';
    } catch {
        return '0.0.0';
    }
}

export function _applicationNameMarker() {
    return `goldlapel:js:${_wrapperVersion()}`;
}

// Keys that are valid inside the structured `config` map. Top-level concepts
// (proxyPort, dashboardPort, invalidationPort, logLevel, mode, license,
// client, configFile, plus the four headline disable flags —
// disableMatviews, disableProxyCache, disableSqloptimize,
// disableAutoIndexes) are exposed as their own options on GoldLapel's
// constructor and are NOT accepted here — passing them through `config`
// raises at argv build time.
const VALID_CONFIG_KEYS = new Set([
    'minPatternCount', 'refreshIntervalSecs', 'patternTtlSecs',
    'maxTablesPerView', 'maxColumnsPerView', 'deepPaginationThreshold',
    'reportIntervalSecs', 'proxyCacheSize', 'batchCacheSize',
    'batchCacheTtlSecs', 'poolSize', 'poolTimeoutSecs',
    'poolMode', 'mgmtIdleTimeout', 'fallback', 'readAfterWriteSecs',
    'n1Threshold', 'n1WindowMs', 'n1CrossThreshold',
    'tlsCert', 'tlsKey', 'tlsClientCa',
    'disableConsolidation', 'disableBtreeIndexes',
    'disableTrigramIndexes', 'disableExpressionIndexes',
    'disablePartialIndexes', 'disableRewrite', 'disableRewritePreparedCache',
    'disablePool',
    'disableN1', 'disableN1CrossConnection', 'disableShadowMode',
    'enableCoalescing', 'replica', 'excludeTables',
]);

const BOOLEAN_KEYS = new Set([
    'disableConsolidation', 'disableBtreeIndexes',
    'disableTrigramIndexes', 'disableExpressionIndexes',
    'disablePartialIndexes', 'disableRewrite', 'disableRewritePreparedCache',
    'disablePool',
    'disableN1', 'disableN1CrossConnection', 'disableShadowMode',
    'enableCoalescing',
]);

const LIST_KEYS = new Set([
    'replica', 'excludeTables',
]);

// logLevel is exposed as an ergonomic string ("trace"|"debug"|"info"|"warn"|
// "error"), but the proxy binary's actual verbosity flag is count-based
// (`-v`/`-vv`/`-vvv`). Translate at the wrapper boundary so users don't have
// to know the underlying flag shape. "warn"/"error" map to the default level
// (no flag emitted).
const VALID_LOG_LEVELS = new Set(['trace', 'debug', 'info', 'warn', 'warning', 'error']);

export function _logLevelToVerboseFlag(level) {
    if (level === undefined || level === null) return null;
    if (typeof level !== 'string') {
        throw new Error(
            `logLevel must be one of: trace, debug, info, warn, error (got ${typeof level})`
        );
    }
    const normalized = level.toLowerCase();
    if (!VALID_LOG_LEVELS.has(normalized)) {
        throw new Error(
            `logLevel must be one of: trace, debug, info, warn, error (got '${level}')`
        );
    }
    switch (normalized) {
        case 'trace': return '-vvv';
        case 'debug': return '-vv';
        case 'info': return '-v';
        // warn/warning/error map to the default level — no flag emitted.
        default: return null;
    }
}

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

// Append `application_name=goldlapel:js:<version>` to `url` unless it already
// has one (or PGAPPNAME is set in the env). The marker tells the proxy this
// is wrapper traffic, so it can skip L2 result cache (the wrapper already has
// its own L1). Idempotent and override-respecting.
function _injectApplicationName(url) {
    if (/[?&]application_name=/.test(url)) return url;
    if (process.env.PGAPPNAME) return url;
    const sep = url.includes('?') ? '&' : '?';
    return `${url}${sep}application_name=${_applicationNameMarker()}`;
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

        // Replace host[:port] with localhost:proxyPort and tag as wrapper traffic
        return _injectApplicationName(`${scheme}${userinfo}localhost:${port}${pathEtc}`);
    }

    // bare host:port (only if not a URL — guard against splitting on scheme colons).
    // Bare-host form skips the marker — atypical caller path.
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

export const _DRIVER_CANDIDATES = ['pg', 'postgres', '@vercel/postgres'];

// `resolve` defaults to the module-scoped `require.resolve`; tests pass a fake
// resolver to simulate various driver-installed subsets without touching the
// real filesystem. The fake must throw like the real `require.resolve` does
// for missing packages.
export function _detectDriver(resolve = require.resolve) {
    // Allow override via env var for tests
    if (process.env.GOLDLAPEL_DRIVER === 'none') return null;
    if (process.env.GOLDLAPEL_DRIVER) {
        return process.env.GOLDLAPEL_DRIVER;
    }

    for (const name of _DRIVER_CANDIDATES) {
        try {
            resolve(name);
            return name;
        } catch {}
    }
    return null;
}

let _driverName = _detectDriver();

export function _driverNotFoundError() {
    // Gold Lapel ships with no bundled driver — users pick one via npm's
    // peerDependencies + peerDependenciesMeta optional pattern. If start()
    // is called with zero drivers installed, name all three options with
    // exact install commands so the fix is copy-pasteable.
    return new Error(
        'No supported Postgres driver found. Gold Lapel needs one of ' +
        "'pg', 'postgres' (postgres.js), or '@vercel/postgres' installed " +
        'alongside it. Install one:\n' +
        '  npm install pg                 # node-postgres (recommended)\n' +
        '  npm install postgres           # postgres.js\n' +
        '  npm install @vercel/postgres   # Vercel Postgres / Neon\n' +
        'If you only need the proxy URL without an internal connection, ' +
        'pass `{ noConnect: true }` to start().'
    );
}

// Builds the adapter object that wraps a postgres.js `sql` instance into the
// shape utils.js expects (a `.query(text, values)` method). Exported so tests
// can exercise the pub/sub-not-supported branch without installing postgres.js.
//
// postgres.js has no LISTEN/NOTIFY event model compatible with pg's
// `client.on('notification', ...)` interface. Rather than silently swallow
// subscribe()/docWatch() callbacks (user sets up pub/sub, then wonders why
// no events ever fire), `on`/`off`/`once`/`removeListener` throw loud and
// point at pg.
export function _makePostgresJsAdapter(sql) {
    const pubsubNotSupported = () => {
        throw new Error(
            "Gold Lapel pub/sub (subscribe, docWatch) is not supported on " +
            "the 'postgres' (postgres.js) driver — postgres.js does not " +
            "expose the node-postgres 'notification' event model. Install " +
            "'pg' (node-postgres) as your internal driver (auto-detected " +
            "if present) or pass an explicit pg client via { conn } / " +
            "gl.using(conn, ...)."
        );
    };
    return {
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
        on: pubsubNotSupported,
        off: pubsubNotSupported,
        once: pubsubNotSupported,
        removeListener: pubsubNotSupported,
    };
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
        const conn = _makePostgresJsAdapter(sql);
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
        proxyPort, dashboardPort, invalidationPort, logLevel, mode, license,
        client, configFile, config, extraArgs, noConnect, silent,
        mesh, meshTag, disableNativeCache,
        // Headline strategy disables — promoted out of `config` to
        // first-class top-level options. Each maps 1:1 to a proxy CLI
        // flag (`--disable-matviews`, `--disable-proxy-cache`,
        // `--disable-sqloptimize`, `--disable-auto-indexes`). Atomic
        // break — passing them inside `config` is rejected.
        disableMatviews, disableProxyCache, disableSqloptimize, disableAutoIndexes,
    } = {}) {
        this._upstream = upstream;
        this._proxyPort = proxyPort ?? DEFAULT_PROXY_PORT;
        // Dashboard / invalidation ports default to proxyPort + 1 / + 2 when
        // unset (matches what the Rust binary binds when no --dashboard-port /
        // --invalidation-port is passed). A user-supplied value (including 0
        // for "disable dashboard") overrides the derivation.
        this._dashboardPortSet = dashboardPort !== undefined;
        this._dashboardPort = dashboardPort !== undefined
            ? Number(dashboardPort)
            : this._proxyPort + 1;
        this._invalidationPortSet = invalidationPort !== undefined;
        this._invalidationPort = invalidationPort !== undefined
            ? Number(invalidationPort)
            : this._proxyPort + 2;
        this._logLevel = logLevel;
        this._mode = mode;
        this._license = license;
        this._client = client;
        this._configFile = configFile;
        this._config = config || {};
        this._extraArgs = extraArgs || [];
        this._noConnect = !!noConnect;
        // `silent` is a wrapper-only option (suppresses the startup banner).
        // Deliberately a top-level option, NOT a key inside `config`, so it
        // can never leak to the Rust binary as a `--silent` CLI flag — the
        // argv builder only ever walks `this._config` through _configToArgs,
        // which enforces VALID_CONFIG_KEYS.
        this._silent = !!silent;
        // Mesh membership (startup intent — HQ enforces license).
        this._mesh = !!mesh;
        this._meshTag = meshTag ? String(meshTag) : null;
        // Headline strategy disables. Each is a first-class top-level
        // option that maps 1:1 to a proxy CLI flag (Model B pivot —
        // wrappers no longer opt traffic into the proxy's result cache;
        // the proxy decides per-connection based on the application_name
        // marker). Default false; emitted as `--disable-X` when true.
        this._disableMatviews = !!disableMatviews;
        this._disableProxyCache = !!disableProxyCache;
        this._disableSqloptimize = !!disableSqloptimize;
        this._disableAutoIndexes = !!disableAutoIndexes;
        // Toggle the wrapper's in-process native cache off without losing
        // the tuned `cacheSize`. When `true`, NativeCache acts as a no-op
        // pass-through (get always misses, put is a no-op) — the
        // invalidation socket still connects so telemetry continues to
        // flow. The previous workaround was `cacheSize: 0`, which forced
        // customers to discard their tuned size to flip the layer; the
        // explicit option lets them keep the size and still toggle.
        // Applied to the NativeCache singleton at construction time so a
        // later `wrap(client)` call sees the right state immediately.
        this._disableNativeCache = !!disableNativeCache;
        // Push `disabled` into the cache singleton now (creates the
        // singleton if `wrap()` hasn't yet). This keeps the toggle
        // effective regardless of whether the user calls wrap() before
        // or after start(), and without forcing GL to mutate cache state
        // later from spawn paths that aren't always exercised in tests.
        new NativeCache({ disabled: this._disableNativeCache });
        // Validate structured-config keys eagerly so a test that constructs
        // without spawning still catches bad keys.
        const unknown = Object.keys(this._config).filter(k => !VALID_CONFIG_KEYS.has(k));
        if (unknown.length > 0) {
            throw new Error(`Unknown config keys: ${unknown.sort().join(', ')}`);
        }
        this._process = null;
        this._proxyUrl = null;
        this._defaultConn = null;
        this._defaultClose = null;
        this._stopped = false;
        // Dashboard token — provisioned on _spawn for internally-launched
        // proxies, or resolved via env/file on first DDL call for external ones.
        this._dashboardToken = null;
        // Per-instance scope for gl.using(). Module-scoped storage would leak
        // the scoped conn across sibling GoldLapel instances in the same process.
        this._connScope = new AsyncLocalStorage();

        // Nested namespaces — canonical schema-to-core sub-API instances.
        // Each holds a back-reference to this client for shared state
        // (license, dashboard token, http session, conn, DDL pattern cache).
        //
        // As of Phase 5 the Redis-compat helper families (counter / zset /
        // hash / queue / geo) are nested too, alongside streams (Phase 1+2)
        // and documents (Phase 4). Search / cache / auth remain flat —
        // they'll migrate when their own schema-to-core phase fires.
        this.documents = new DocumentsAPI(this);
        this.streams = new StreamsAPI(this);
        this.counters = new CountersAPI(this);
        this.zsets = new ZsetsAPI(this);
        this.hashes = new HashesAPI(this);
        this.queues = new QueuesAPI(this);
        this.geos = new GeosAPI(this);
    }

    // Builds the argv passed to the proxy binary. Pure — no side effects —
    // so tests can assert the translated flags without spawning a process.
    // Throws if logLevel is invalid (via _logLevelToVerboseFlag).
    _buildSpawnArgs() {
        const verboseFlag = _logLevelToVerboseFlag(this._logLevel);
        const args = [
            '--upstream', this._upstream,
            '--proxy-port', String(this._proxyPort),
        ];
        // Top-level options (promoted out of the config map) emit their own
        // CLI flags before the tuning-knob config map. Each is suppressed
        // when the user hasn't set it, so the Rust binary applies its own
        // defaults.
        if (this._dashboardPortSet) {
            args.push('--dashboard-port', String(this._dashboardPort));
        }
        if (this._invalidationPortSet) {
            args.push('--invalidation-port', String(this._invalidationPort));
        }
        if (verboseFlag) {
            args.push(verboseFlag);
        }
        if (this._mode) {
            args.push('--mode', this._mode);
        }
        if (this._license) {
            args.push('--license', this._license);
        }
        if (this._client) {
            args.push('--client', this._client);
        }
        if (this._configFile) {
            args.push('--config', this._configFile);
        }
        if (this._mesh) {
            args.push('--mesh');
        }
        if (this._meshTag) {
            args.push('--mesh-tag', this._meshTag);
        }
        // Headline strategy disables — emitted as their own top-level CLI
        // flags. Suppressed when the user hasn't set them so the Rust
        // binary applies its own defaults.
        if (this._disableMatviews) {
            args.push('--disable-matviews');
        }
        if (this._disableProxyCache) {
            args.push('--disable-proxy-cache');
        }
        if (this._disableSqloptimize) {
            args.push('--disable-sqloptimize');
        }
        if (this._disableAutoIndexes) {
            args.push('--disable-auto-indexes');
        }
        args.push(..._configToArgs(this._config));
        args.push(...this._extraArgs);
        return args;
    }

    async _spawn() {
        if (this._process && this._process.exitCode === null) {
            return;
        }

        const args = this._buildSpawnArgs();
        const binary = _findBinary();

        const env = { ...process.env };
        // GOLDLAPEL_CLIENT env var is only set when the user hasn't opted
        // in via the top-level `client` option (which emits --client and
        // takes precedence over the env var).
        if (!this._client && !env.GOLDLAPEL_CLIENT) env.GOLDLAPEL_CLIENT = 'node';
        // Provision a session-scoped dashboard token for /api/ddl/* calls.
        // Pre-set env wins (user may already have their own token).
        if (env.GOLDLAPEL_DASHBOARD_TOKEN) {
            this._dashboardToken = env.GOLDLAPEL_DASHBOARD_TOKEN;
        } else {
            const { randomBytes } = await import('crypto');
            this._dashboardToken = randomBytes(32).toString('hex');
            env.GOLDLAPEL_DASHBOARD_TOKEN = this._dashboardToken;
        }
        this._process = spawn(binary, args, {
            stdio: ['ignore', 'ignore', 'pipe'],
            env,
        });

        let stderr = '';
        const onData = (chunk) => { stderr += chunk; };
        this._process.stderr.on('data', onData);

        this._process.on('error', (err) => { stderr += err.message; });

        const ready = await Promise.race([
            _waitForPort('127.0.0.1', this._proxyPort, STARTUP_TIMEOUT),
            new Promise((resolve) => {
                this._process.on('exit', () => resolve(false));
            }),
        ]);
        if (!ready) {
            this._process.stderr.removeListener('data', onData);
            this._process.kill();
            throw new Error(
                `Gold Lapel failed to start on port ${this._proxyPort} ` +
                `within ${STARTUP_TIMEOUT / 1000}s.\nstderr: ${stderr}`
            );
        }

        this._process.stderr.removeListener('data', onData);

        this._proxyUrl = _makeProxyUrl(this._upstream, this._proxyPort);
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
        // Banner goes to stderr, never stdout. Library code writing to stdout
        // corrupts programs that pipe gl output (e.g. a CLI tool piping the
        // wrapper's own output). `silent: true` on start() suppresses the
        // banner entirely.
        if (this._silent) return;
        const banner = this._dashboardPort
            ? `goldlapel → :${this._proxyPort} (proxy) | http://127.0.0.1:${this._dashboardPort} (dashboard)`
            : `goldlapel → :${this._proxyPort} (proxy)`;
        console.error(banner);
    }

    async stop() {
        if (this._stopped) return;
        this._stopped = true;

        _liveInstances.delete(this);

        // Drop any cached DDL patterns — they're tied to the proxy we're
        // about to kill.
        try {
            const ddl = await import('./ddl.js');
            ddl.invalidate(this);
        } catch {}

        const close = this._defaultClose;
        this._defaultConn = null;
        this._defaultClose = null;
        if (close) {
            try { await close(); } catch {}
        }

        const proc = this._process;
        this._process = null;
        this._proxyUrl = null;
        this._dashboardToken = null;
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
        const scoped = this._connScope.getStore();
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
        return this._connScope.run(conn, () => callback(this));
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

    get dashboardPort() {
        return this._dashboardPort;
    }

    get dashboardToken() {
        return this._dashboardToken;
    }

    // ─── Wrapper methods ───────────────────────────────────────────────────
    //
    // Every method accepts an optional trailing `{ conn }` option that
    // overrides the default connection for that call. If absent, the
    // connection is resolved from the `using()` scope or the internal default.

    // Document store:      gl.documents.<verb>(...).  See documents.js.
    // Streams:              gl.streams.<verb>(...).    See streams.js.
    // Phase 5 Redis-compat: gl.counters / gl.zsets / gl.hashes /
    //                       gl.queues / gl.geos.       See {counters,zsets,
    //                       hashes,queues,geos}.js.
    //
    // The legacy flat helpers (incr, hset, zadd, enqueue, geoadd, …) are
    // gone — Phase 5 is a hard cut, no aliases. Use the namespaces above.

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

    // Pub/Sub
    async publish(...args) { return _call(this, publish, args); }
    async subscribe(...args) { return _call(this, subscribe, args); }

    // Misc
    async countDistinct(...args) { return _call(this, countDistinct, args); }
    async script(...args) { return _call(this, script, args); }
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
 * @param {number} [opts.proxyPort=7932]  Proxy listen port.
 * @param {number} [opts.dashboardPort]  Dashboard port. Defaults to `proxyPort + 1` (7933 when `proxyPort` is the 7932 default). `0` disables.
 * @param {number} [opts.invalidationPort]  Cache-invalidation port. Defaults to `proxyPort + 2`.
 * @param {'trace'|'debug'|'info'|'warn'|'error'} [opts.logLevel]  Binary log level.
 * @param {string} [opts.mode]  Operating mode (`waiter`, `consideration`, etc).
 * @param {string} [opts.license]  Path to the license file.
 * @param {string} [opts.client]  Client identifier; sets `GOLDLAPEL_CLIENT` for telemetry tagging.
 * @param {string} [opts.configFile]  Path to a TOML config file for the Rust binary (`--config`).
 * @param {object} [opts.config]  Structured tuning knobs — camelCase keys → CLI flags (see configKeys()). Top-level concepts listed above are NOT accepted here.
 * @param {string[]} [opts.extraArgs]  Raw CLI flags passed to the binary.
 * @param {boolean} [opts.noConnect]  Skip opening the internal driver connection.
 * @param {boolean} [opts.silent]  Suppress the one-line startup banner (wrapper-only; never forwarded to the binary).
 * @param {boolean} [opts.mesh]  Opt into the mesh at startup. HQ enforces the license; denial is non-fatal — proxy runs without clustering.
 * @param {string}  [opts.meshTag]  Mesh tag — instances sharing a tag cluster together.
 * @param {boolean} [opts.disableMatviews=false]  Disable matview optimization on the proxy (`--disable-matviews`). Headline kill switch for the matview strategy.
 * @param {boolean} [opts.disableProxyCache=false]  Disable the proxy's result cache entirely (`--disable-proxy-cache`). Highest-precedence kill switch — overrides finer-grained cache toggles.
 * @param {boolean} [opts.disableSqloptimize=false]  Disable the SQL rewrite / optimization pipeline on the proxy (`--disable-sqloptimize`). Per-kind disables in `config` still apply on top.
 * @param {boolean} [opts.disableAutoIndexes=false]  Disable automatic index recommendations / creation on the proxy (`--disable-auto-indexes`).
 * @param {boolean} [opts.disableNativeCache=false]  Disable the wrapper's in-process native cache without losing the tuned `cacheSize`. When `true`, gets always miss and puts are no-ops; the invalidation socket still connects so telemetry continues to flow. Use to A/B the native cache layer (e.g. measure end-to-end latency with and without it) while keeping your size config intact.
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
export { DocumentsAPI } from './documents.js';
export { StreamsAPI } from './streams.js';
export { CountersAPI } from './counters.js';
export { ZsetsAPI } from './zsets.js';
export { HashesAPI } from './hashes.js';
export { QueuesAPI } from './queues.js';
export { GeosAPI } from './geos.js';
export {
    publish, subscribe,
    countDistinct,
    script,
    counterIncr, counterDecr, counterSet, counterGet, counterDelete, counterCountKeys,
    zsetAdd, zsetIncrBy, zsetScore, zsetRank, zsetRange, zsetRangeByScore, zsetRemove, zsetCard,
    hashSet, hashGet, hashGetAll, hashKeys, hashValues, hashExists, hashDelete, hashLen,
    queueEnqueue, queueClaim, queueAck, queueAbandon, queueExtend, queuePeek,
    queueCountReady, queueCountClaimed,
    geoAdd, geoPos, geoDist, geoRadius, geoRadiusByMember, geoRemove, geoCount,
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

// Default export mirrors the named surface so both styles work identically:
//   import goldlapel from 'goldlapel';
//   import * as goldlapel from 'goldlapel';
// Every name available as a named export is also reachable via the default.
export default {
    GoldLapel, start, configKeys, _configToArgs, _logLevelToVerboseFlag,
    wrap, NativeCache,
    DocumentsAPI, StreamsAPI,
    CountersAPI, ZsetsAPI, HashesAPI, QueuesAPI, GeosAPI,
    publish, subscribe,
    countDistinct,
    script,
    counterIncr, counterDecr, counterSet, counterGet, counterDelete, counterCountKeys,
    zsetAdd, zsetIncrBy, zsetScore, zsetRank, zsetRange, zsetRangeByScore, zsetRemove, zsetCard,
    hashSet, hashGet, hashGetAll, hashKeys, hashValues, hashExists, hashDelete, hashLen,
    queueEnqueue, queueClaim, queueAck, queueAbandon, queueExtend, queuePeek,
    queueCountReady, queueCountClaimed,
    geoAdd, geoPos, geoDist, geoRadius, geoRadiusByMember, geoRemove, geoCount,
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
};
