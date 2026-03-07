import { spawn, execFileSync } from 'child_process';
import { createConnection } from 'net';
import { existsSync } from 'fs';
import { join, dirname } from 'path';
import { platform, arch } from 'os';
import { fileURLToPath } from 'url';
import { createRequire } from 'module';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

const DEFAULT_PORT = 7932;
const STARTUP_TIMEOUT = 10000;
const STARTUP_POLL_INTERVAL = 50;

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
        binaryName = `goldlapel-linux-${archName}`;
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
    const npmPkgName = `@goldlapel/${npmPlatform}-${machine}`;
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

    // pg URL with explicit port: scheme://[userinfo@]host:PORT[/path][?query]
    const withPort = upstream.match(/^(postgres(?:ql)?:\/\/(?:.*@)?)([^:/?#]+):(\d+)(.*)$/);
    if (withPort) {
        return `${withPort[1]}localhost:${port}${withPort[4]}`;
    }

    // pg URL without port: scheme://[userinfo@]host[/path][?query]
    const noPort = upstream.match(/^(postgres(?:ql)?:\/\/(?:.*@)?)([^:/?#]+)(.*)$/);
    if (noPort) {
        return `${noPort[1]}localhost:${port}${noPort[3]}`;
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
    constructor(upstream, { port, extraArgs } = {}) {
        this._upstream = upstream;
        this._port = port ?? DEFAULT_PORT;
        this._extraArgs = extraArgs || [];
        this._process = null;
        this._proxyUrl = null;
    }

    async start() {
        if (this._process && this._process.exitCode === null) {
            return this._proxyUrl;
        }

        const binary = _findBinary();
        const args = [
            '--upstream', this._upstream,
            '--port', String(this._port),
            ...this._extraArgs,
        ];

        this._process = spawn(binary, args, {
            stdio: ['ignore', 'ignore', 'pipe'],
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
        return this._proxyUrl;
    }

    stop() {
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

    get url() {
        return this._proxyUrl;
    }

    get running() {
        return this._process !== null && this._process.exitCode === null;
    }
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
        return _instance.url;
    }
    _instance = new GoldLapel(upstream, opts);
    if (!_cleanupRegistered) {
        process.on('exit', _cleanup);
        _cleanupRegistered = true;
    }
    return _instance.start();
}

export function stop() {
    if (_instance) {
        _instance.stop();
        _instance = null;
    }
}

export function proxyUrl() {
    return _instance ? _instance.url : null;
}

function _cleanup() {
    if (_instance) {
        _instance.stop();
        _instance = null;
    }
}

export default { GoldLapel, start, stop, proxyUrl };
