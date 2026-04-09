import { spawn, execFileSync } from 'child_process';
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

export class GoldLapel {
    constructor(upstream, { port, config, extraArgs } = {}) {
        this._upstream = upstream;
        this._port = port ?? DEFAULT_PORT;
        this._dashboardPort = config && config.dashboardPort !== undefined
            ? Number(config.dashboardPort)
            : DEFAULT_DASHBOARD_PORT;
        this._config = config || {};
        this._extraArgs = extraArgs || [];
        this._process = null;
        this._proxyUrl = null;
        this._client = null;
    }

    async start() {
        if (this._process && this._process.exitCode === null) {
            return this._proxyUrl;
        }

        const binary = _findBinary();
        const args = [
            '--upstream', this._upstream,
            '--proxy-port', String(this._port),
            ..._configToArgs(this._config),
            ...this._extraArgs,
        ];

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

        // Create client connection
        const pg = await import('pg');
        const Client = pg.default?.Client ?? pg.Client;
        this._client = new Client({ connectionString: this._proxyUrl });
        await this._client.connect();

        if (this._dashboardPort) {
            console.log(`goldlapel → :${this._port} (proxy) | http://127.0.0.1:${this._dashboardPort} (dashboard)`);
        } else {
            console.log(`goldlapel → :${this._port} (proxy)`);
        }

        return this._proxyUrl;
    }

    stop() {
        if (this._client) {
            this._client.end().catch(() => {});
            this._client = null;
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

    get client() {
        if (!this._client) {
            throw new Error('Not connected. Call start() before accessing client.');
        }
        return this._client;
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

    // ─── Instance methods (delegate to utils with this.client) ─────────

    // Document store
    async docCreateCollection(...args) { return docCreateCollection(this.client, ...args); }
    async docInsert(...args) { return docInsert(this.client, ...args); }
    async docInsertMany(...args) { return docInsertMany(this.client, ...args); }
    async docFind(...args) { return docFind(this.client, ...args); }
    async *docFindCursor(...args) { yield* docFindCursor(this.client, ...args); }
    async docFindOne(...args) { return docFindOne(this.client, ...args); }
    async docUpdate(...args) { return docUpdate(this.client, ...args); }
    async docUpdateOne(...args) { return docUpdateOne(this.client, ...args); }
    async docDelete(...args) { return docDelete(this.client, ...args); }
    async docDeleteOne(...args) { return docDeleteOne(this.client, ...args); }
    async docFindOneAndUpdate(...args) { return docFindOneAndUpdate(this.client, ...args); }
    async docFindOneAndDelete(...args) { return docFindOneAndDelete(this.client, ...args); }
    async docDistinct(...args) { return docDistinct(this.client, ...args); }
    async docCount(...args) { return docCount(this.client, ...args); }
    async docCreateIndex(...args) { return docCreateIndex(this.client, ...args); }
    async docAggregate(...args) { return docAggregate(this.client, ...args); }
    async docWatch(...args) { return docWatch(this.client, ...args); }
    async docUnwatch(...args) { return docUnwatch(this.client, ...args); }
    async docCreateTtlIndex(...args) { return docCreateTtlIndex(this.client, ...args); }
    async docRemoveTtlIndex(...args) { return docRemoveTtlIndex(this.client, ...args); }
    async docCreateCapped(...args) { return docCreateCapped(this.client, ...args); }
    async docRemoveCap(...args) { return docRemoveCap(this.client, ...args); }

    // Search
    async search(...args) { return search(this.client, ...args); }
    async searchFuzzy(...args) { return searchFuzzy(this.client, ...args); }
    async searchPhonetic(...args) { return searchPhonetic(this.client, ...args); }
    async similar(...args) { return similar(this.client, ...args); }
    async suggest(...args) { return suggest(this.client, ...args); }
    async facets(...args) { return facets(this.client, ...args); }
    async aggregate(...args) { return aggregate(this.client, ...args); }
    async createSearchConfig(...args) { return createSearchConfig(this.client, ...args); }

    // Percolation
    async percolateAdd(...args) { return percolateAdd(this.client, ...args); }
    async percolate(...args) { return percolate(this.client, ...args); }
    async percolateDelete(...args) { return percolateDelete(this.client, ...args); }

    // Analysis
    async analyze(...args) { return analyze(this.client, ...args); }
    async explainScore(...args) { return explainScore(this.client, ...args); }

    // Pub/Sub & Queues
    async publish(...args) { return publish(this.client, ...args); }
    async subscribe(...args) { return subscribe(this.client, ...args); }
    async enqueue(...args) { return enqueue(this.client, ...args); }
    async dequeue(...args) { return dequeue(this.client, ...args); }

    // Counters
    async incr(...args) { return incr(this.client, ...args); }
    async getCounter(...args) { return getCounter(this.client, ...args); }

    // Hash maps
    async hset(...args) { return hset(this.client, ...args); }
    async hget(...args) { return hget(this.client, ...args); }
    async hgetall(...args) { return hgetall(this.client, ...args); }
    async hdel(...args) { return hdel(this.client, ...args); }

    // Sorted sets
    async zadd(...args) { return zadd(this.client, ...args); }
    async zincrby(...args) { return zincrby(this.client, ...args); }
    async zrange(...args) { return zrange(this.client, ...args); }
    async zrank(...args) { return zrank(this.client, ...args); }
    async zscore(...args) { return zscore(this.client, ...args); }
    async zrem(...args) { return zrem(this.client, ...args); }

    // Geo
    async geoadd(...args) { return geoadd(this.client, ...args); }
    async georadius(...args) { return georadius(this.client, ...args); }
    async geodist(...args) { return geodist(this.client, ...args); }

    // Misc
    async countDistinct(...args) { return countDistinct(this.client, ...args); }
    async script(...args) { return script(this.client, ...args); }

    // Streams
    async streamAdd(...args) { return streamAdd(this.client, ...args); }
    async streamCreateGroup(...args) { return streamCreateGroup(this.client, ...args); }
    async streamRead(...args) { return streamRead(this.client, ...args); }
    async streamAck(...args) { return streamAck(this.client, ...args); }
    async streamClaim(...args) { return streamClaim(this.client, ...args); }
}

// Module-level singleton
let _instance = null;
let _cleanupRegistered = false;

export async function start(upstream, opts) {
    if (_instance && _instance.running) {
        if (_instance._upstream !== upstream) {
            throw new Error(
                'Gold Lapel is already running for a different upstream. ' +
                'Call goldlapel.stop() before starting with a new upstream.'
            );
        }
        return _instance._wrappedClient || _instance.url;
    }
    _instance = new GoldLapel(upstream, opts);
    if (!_cleanupRegistered) {
        process.on('exit', _cleanup);
        _cleanupRegistered = true;
    }
    const url = await _instance.start();

    // Auto-detect pg and return wrapped client with L1 cache
    let pg;
    try {
        pg = await import('pg');
    } catch {
        throw new Error(
            'No supported database driver found. ' +
            'Install one (e.g. npm install pg) ' +
            'or use proxyUrl() if you only need the connection string.'
        );
    }
    const Client = pg.default?.Client ?? pg.Client;
    const { wrap } = await import('./wrap.js');
    const client = new Client({ connectionString: url });
    await client.connect();
    const invPort = opts?.config?.invalidationPort ?? (_instance._port + 2);
    const wrapped = wrap(client, invPort);
    _instance._wrappedClient = wrapped;
    return wrapped;
}

export function stop() {
    if (_instance) {
        if (_instance._wrappedClient && typeof _instance._wrappedClient.end === 'function') {
            _instance._wrappedClient.end();
        }
        _instance.stop();
        _instance = null;
    }
    NativeCache._reset();
}

export function proxyUrl() {
    return _instance ? _instance.url : null;
}

export function dashboardUrl() {
    return _instance ? _instance.dashboardUrl : null;
}

function _cleanup() {
    if (_instance) {
        if (_instance._wrappedClient && typeof _instance._wrappedClient.end === 'function') {
            _instance._wrappedClient.end();
        }
        _instance.stop();
        _instance = null;
    }
}

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

export default { GoldLapel, start, stop, proxyUrl, dashboardUrl, configKeys, _configToArgs };
