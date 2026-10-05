// Port allocation, proxy sharing and readiness — against a fake proxy binary,
// then (with GOLDLAPEL_INTEGRATION=1) the real one. They share this file so
// they run one after the other: both take ports from 7932 up, and test files
// run in parallel processes.
//
// The fake behaves like the real proxy where it matters here: it binds its
// proxy port and its dashboard port (--dashboard-port, else proxy + 1; 0 for
// none) on all interfaces, and if either is taken it prints the proxy's
// "already in use" message and exits 1. Each run appends its PID to
// GL_FAKE_PIDLOG so tests can count spawns. GL_FAKE_MODE=exit makes it fail
// at startup instead; GL_FAKE_MODE=refuse-first makes its first run report
// its port taken.

import { describe, it, before, after, afterEach } from 'node:test';
import assert from 'node:assert/strict';
import { writeFileSync, chmodSync, readFileSync, existsSync, mkdtempSync, rmSync, unlinkSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from 'node:net';

import { start } from '../index.js';
import { integrationGate } from './_integration-gate.js';

const FAKE_BINARY_SRC = `#!/usr/bin/env node
const net = require('node:net');
const fs = require('node:fs');

const args = process.argv.slice(2);
const flag = (name) => {
    const i = args.indexOf(name);
    return i === -1 ? undefined : Number(args[i + 1]);
};
const proxyPort = flag('--proxy-port');
const dashboardPort = flag('--dashboard-port') ?? proxyPort + 1;

const firstRun = !fs.existsSync(process.env.GL_FAKE_PIDLOG);
fs.appendFileSync(process.env.GL_FAKE_PIDLOG, process.pid + '\\n');

if (process.env.GL_FAKE_MODE === 'refuse-first' && firstRun) {
    // As if another program took the port between the wrapper's probe and ours.
    console.error("I'm afraid port " + proxyPort + ", for the proxy, is already in use — perhaps another Gold Lapel.");
    process.exit(1);
}

if (process.env.GL_FAKE_MODE === 'exit') {
    console.error('fake proxy: something went badly wrong');
    process.exit(3);
}

const bind = (port, what, next) => {
    if (!port) return next();
    const srv = net.createServer((sock) => sock.destroy());
    srv.on('error', () => {
        console.error("I'm afraid port " + port + ", for the " + what + ", is already in use — perhaps another Gold Lapel.");
        process.exit(1);
    });
    srv.listen(port, '0.0.0.0', next);
};
bind(proxyPort, 'proxy', () => bind(dashboardPort, 'dashboard', () => {}));
`;

const UP1 = 'postgresql://app:s3cret@db1.example:5432/one';
const UP2 = 'postgresql://app:s3cret@db2.example:5432/two';
const OPTS = { noConnect: true, silent: true };

function portOf(url) {
    return Number(url.match(/localhost:(\d+)/)[1]);
}

function isAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (err) {
        return err.code === 'EPERM';
    }
}

async function waitForPidGone(pid, timeoutMs = 10_000) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        if (!isAlive(pid)) return true;
        await new Promise((r) => setTimeout(r, 25));
    }
    return false;
}

// Listen on `port` on all interfaces, as another program would. Resolves
// null if another program already does — the port is just as taken.
function occupy(port) {
    return new Promise((resolve, reject) => {
        const srv = createServer();
        srv.once('error', (err) => (err.code === 'EADDRINUSE' ? resolve(null) : reject(err)));
        srv.listen(port, '0.0.0.0', () => resolve(srv));
    });
}

function closeServer(srv) {
    return new Promise((resolve) => (srv ? srv.close(resolve) : resolve()));
}

describe('proxy ports and sharing', () => {
    let workdir;
    let pidlog;
    const origBinary = process.env.GOLDLAPEL_BINARY;
    const started = [];

    function pids() {
        if (!existsSync(pidlog)) return [];
        return readFileSync(pidlog, 'utf8').trim().split('\n').filter(Boolean).map(Number);
    }

    async function startTracked(upstream, opts = {}) {
        const gl = await start(upstream, { ...OPTS, ...opts });
        started.push(gl);
        return gl;
    }

    before(() => {
        workdir = mkdtempSync(join(tmpdir(), 'gl-js-ports-'));
        const binary = join(workdir, 'goldlapel-fake');
        pidlog = join(workdir, 'pids');
        writeFileSync(binary, FAKE_BINARY_SRC);
        chmodSync(binary, 0o755);
        process.env.GOLDLAPEL_BINARY = binary;
        process.env.GL_FAKE_PIDLOG = pidlog;
    });

    afterEach(async () => {
        for (const gl of started.splice(0)) await gl.stop();
        for (const pid of pids()) assert.ok(await waitForPidGone(pid), `fake proxy ${pid} left running`);
        if (existsSync(pidlog)) unlinkSync(pidlog);
        delete process.env.GL_FAKE_MODE;
    });

    after(() => {
        if (origBinary !== undefined) process.env.GOLDLAPEL_BINARY = origBinary;
        else delete process.env.GOLDLAPEL_BINARY;
        delete process.env.GL_FAKE_PIDLOG;
        rmSync(workdir, { recursive: true, force: true });
    });

    it('gives two upstreams non-overlapping proxy/dashboard pairs', async () => {
        const a = await startTracked(UP1);
        const b = await startTracked(UP2);

        assert.ok(a._proxyPort >= 7932 && b._proxyPort >= 7932);
        assert.equal(a.dashboardPort, a._proxyPort + 1);
        assert.equal(b.dashboardPort, b._proxyPort + 1);
        const aPorts = [a._proxyPort, a.dashboardPort];
        assert.ok(!aPorts.includes(b._proxyPort) && !aPorts.includes(b.dashboardPort),
            `pairs overlap: ${aPorts} vs ${[b._proxyPort, b.dashboardPort]}`);
        assert.equal(portOf(a.url), a._proxyPort);
        assert.equal(portOf(b.url), b._proxyPort);
        assert.notEqual(a._process.pid, b._process.pid);
        assert.equal(pids().length, 2);
    });

    it('a second start() for the same upstream shares the proxy until the last stop()', async () => {
        const a = await startTracked(UP1);
        const b = await startTracked(UP1);

        assert.notStrictEqual(a, b);
        assert.strictEqual(a._process, b._process);
        assert.equal(a.url, b.url);
        assert.equal(a.dashboardToken, b.dashboardToken);
        assert.equal(pids().length, 1);

        await a.stop();
        assert.equal(a.running, false);
        assert.equal(b.running, true, 'the other holder still needs the proxy');
        assert.ok(isAlive(pids()[0]));

        await b.stop();
        assert.ok(await waitForPidGone(pids()[0]), 'the last stop() ends the proxy');
    });

    it('concurrent start() calls for one upstream spawn one proxy', async () => {
        const [a, b, c] = await Promise.all([UP1, UP1, UP1].map((u) => startTracked(u)));
        assert.equal(pids().length, 1);
        assert.strictEqual(a._process, b._process);
        assert.strictEqual(b._process, c._process);
    });

    it('concurrent start() calls for different upstreams get different pairs', async () => {
        const [a, b] = await Promise.all([startTracked(UP1), startTracked(UP2)]);
        const ports = new Set([a._proxyPort, a.dashboardPort, b._proxyPort, b.dashboardPort]);
        assert.equal(ports.size, 4);
    });

    it('a shared proxy refuses a start() asking for different options', async () => {
        const a = await startTracked(UP1);
        await assert.rejects(
            () => startTracked(UP1, { mode: 'consideration' }),
            /already running for postgresql:\/\/app:\*\*\*@db1\.example:5432\/one on port \d+ with different options/,
        );
        await assert.rejects(() => startTracked(UP1, { proxyPort: a._proxyPort + 10 }), /different options/);
        assert.equal(a.running, true);
        // Same options (ports left unset) share it.
        const b = await startTracked(UP1, { silent: false });
        assert.strictEqual(b._process, a._process);
    });

    it('an explicit port another upstream holds is an error naming it, password masked', async () => {
        const a = await startTracked(UP1);

        await assert.rejects(() => startTracked(UP2, { proxyPort: a._proxyPort }), (err) => {
            assert.match(err.message, new RegExp(`cannot use port ${a._proxyPort} as the proxy port`));
            assert.match(err.message, /proxy for postgresql:\/\/app:\*\*\*@db1\.example:5432\/one already holds it as its proxy port/);
            assert.doesNotMatch(err.message, /s3cret/);
            return true;
        });
        await assert.rejects(() => startTracked(UP2, { proxyPort: a.dashboardPort }),
            /as the proxy port: .* holds it as its dashboard port/);
        // Its derived dashboard port (proxy + 1) would land on a's proxy port.
        await assert.rejects(() => startTracked(UP2, { proxyPort: a._proxyPort - 1 }),
            new RegExp(`cannot use port ${a._proxyPort} as the dashboard port`));
        await assert.rejects(() => startTracked(UP2, { dashboardPort: a._proxyPort }),
            /as the dashboard port: .* holds it as its proxy port/);
        assert.equal(pids().length, 1, 'no proxy is spawned for a refused port');
    });

    it('a stopped proxy frees its ports for an explicit start()', async () => {
        const a = await startTracked(UP1);
        const port = a._proxyPort;
        await a.stop();
        assert.ok(await waitForPidGone(pids()[0]));

        const b = await startTracked(UP2, { proxyPort: port });
        assert.equal(b._proxyPort, port);
    });

    it('skips a pair another program holds', async () => {
        const a = await startTracked(UP1);
        const port = a._proxyPort;
        await a.stop();
        assert.ok(await waitForPidGone(pids()[0]));

        for (const busy of [port, port + 1]) {
            const blocker = await occupy(busy);
            try {
                const b = await startTracked(UP2);
                assert.ok(b._proxyPort !== busy && b.dashboardPort !== busy,
                    `got ${b._proxyPort}/${b.dashboardPort} though ${busy} is taken`);
                await b.stop();
            } finally {
                await closeServer(blocker);
            }
        }
    });

    it('an explicit port another program holds fails with the proxy\'s own message', async () => {
        const a = await startTracked(UP1);
        const port = a._proxyPort;
        await a.stop();
        assert.ok(await waitForPidGone(pids()[0]));

        const blocker = await occupy(port);
        try {
            // The blocker answers TCP connects, so readiness by connect alone
            // would wrongly succeed here.
            await assert.rejects(() => startTracked(UP2, { proxyPort: port }), (err) => {
                assert.match(err.message, /exited \(code 1\) before it was ready/);
                assert.match(err.message, new RegExp(`port ${port}, for the proxy, is already in use`));
                return true;
            });
        } finally {
            await closeServer(blocker);
        }
        // The failed start released its claim.
        const b = await startTracked(UP2, { proxyPort: port });
        assert.equal(b._proxyPort, port);
    });

    it('a proxy that exits during startup fails start() with its status and stderr', async () => {
        process.env.GL_FAKE_MODE = 'exit';
        await assert.rejects(() => startTracked(UP1), (err) => {
            assert.match(err.message, /exited \(code 3\) before it was ready/);
            assert.match(err.message, /something went badly wrong/);
            return true;
        });
        delete process.env.GL_FAKE_MODE;
        // Nothing is left claimed or registered for the upstream.
        const a = await startTracked(UP1);
        assert.equal(a.running, true);
    });

    it('a chosen pair lost to another program between probe and spawn is chosen again', async () => {
        process.env.GL_FAKE_MODE = 'refuse-first';
        const a = await startTracked(UP1);
        assert.equal(pids().length, 2);
        assert.equal(a.running, true);
        assert.equal(portOf(a.url), a._proxyPort);
    });

    it('an explicit port is not retried elsewhere', async () => {
        process.env.GL_FAKE_MODE = 'refuse-first';
        await assert.rejects(() => startTracked(UP1, { proxyPort: 7990 }), /port 7990, for the proxy, is already in use/);
        assert.equal(pids().length, 1);
    });

    it('a start() rejected before spawning claims nothing', async () => {
        const a = await startTracked(UP1);
        const port = a._proxyPort;
        await a.stop();
        assert.ok(await waitForPidGone(pids()[0]));
        unlinkSync(pidlog);

        await assert.rejects(() => startTracked(UP2, { proxyPort: port, invalidationPort: 1 }), /Unknown options/);
        await assert.rejects(() => startTracked(UP2, { proxyPort: port, logLevel: 'loud' }), /logLevel/);
        await assert.rejects(() => startTracked(UP2, { proxyPort: port, config: { bogus: 1 } }), /Unknown config keys/);
        assert.deepEqual(pids(), []);

        const b = await startTracked(UP1, { proxyPort: port });
        assert.equal(b._proxyPort, port);
    });

    it('a proxy that dies frees its upstream and ports', async () => {
        const a = await startTracked(UP1);
        const pid = a._process.pid;
        process.kill(pid, 'SIGKILL');
        assert.ok(await waitForPidGone(pid));
        await new Promise((r) => setImmediate(r));
        assert.equal(a.running, false);

        const b = await startTracked(UP1);
        assert.notStrictEqual(b._process, a._process);
        assert.equal(b.running, true);
    });

    it('dashboardPort: 0 claims no dashboard port', async () => {
        const a = await startTracked(UP1, { dashboardPort: 0 });
        const b = await startTracked(UP2, { proxyPort: a._proxyPort + 1 });
        assert.equal(b._proxyPort, a._proxyPort + 1);
        assert.equal(a.dashboardUrl, null);
    });
});


// ─── Real binary ────────────────────────────────────────────────────────────

const gate = integrationGate();

if (gate.failReason) {
    describe('ports integration (misconfigured)', () => {
        it('fails when GOLDLAPEL_INTEGRATION=1 but GOLDLAPEL_TEST_UPSTREAM missing', () => {
            throw new Error(gate.failReason);
        });
    });
} else if (!gate.shouldRun) {
    describe('ports integration (skipped)', { skip: gate.skipReason }, () => {
        it('skipped', () => {});
    });
} else {

const pg = (await import('pg')).default;

// The same database under a second spelling, so the process sees two upstreams.
const UP1 = gate.upstream;
const UP2 = gate.upstream.includes('localhost')
    ? gate.upstream.replace('localhost', '127.0.0.1')
    : gate.upstream.replace('127.0.0.1', 'localhost');

async function query(url, text) {
    const client = new pg.Client({ connectionString: url });
    await client.connect();
    try {
        return (await client.query(text)).rows;
    } finally {
        await client.end();
    }
}

describe('ports integration', () => {
    const started = [];

    afterEach(async () => {
        for (const gl of started.splice(0)) await gl.stop();
    });

    async function startTracked(upstream, opts = {}) {
        const gl = await start(upstream, { silent: true, noConnect: true, ...opts });
        started.push(gl);
        return gl;
    }

    it('two upstreams without proxyPort each get a working proxy', async () => {
        assert.notEqual(UP1, UP2);
        const a = await startTracked(UP1);
        const b = await startTracked(UP2);

        assert.ok(![a._proxyPort, a.dashboardPort].includes(b._proxyPort));
        assert.ok(![a._proxyPort, a.dashboardPort].includes(b.dashboardPort));
        assert.deepEqual(await query(a.url, 'SELECT 1 AS one'), [{ one: 1 }]);
        assert.deepEqual(await query(b.url, 'SELECT 2 AS two'), [{ two: 2 }]);
    });

    it('a second start() for the same upstream shares the proxy', async () => {
        const a = await startTracked(UP1);
        const b = await startTracked(UP1);
        assert.strictEqual(a._process, b._process);
        await a.stop();
        assert.deepEqual(await query(b.url, 'SELECT 3 AS three'), [{ three: 3 }]);
    });

    it('an explicit port another program holds fails with the proxy\'s message', async () => {
        const blocker = createServer();
        await new Promise((resolve) => blocker.listen(0, '0.0.0.0', resolve));
        const port = blocker.address().port;
        try {
            await assert.rejects(
                () => startTracked(UP1, { proxyPort: port }),
                new RegExp(`exited \\(code 1\\)[\\s\\S]*port ${port}, for the proxy, is already in use`),
            );
        } finally {
            await new Promise((resolve) => blocker.close(resolve));
        }
    });

    it('the app URL leaves the upstream\'s sslmode behind', async () => {
        const sep = UP1.includes('?') ? '&' : '?';
        const a = await startTracked(`${UP1}${sep}sslmode=prefer`);
        assert.doesNotMatch(a.url, /sslmode/);
        assert.deepEqual(await query(a.url, 'SELECT 4 AS four'), [{ four: 4 }]);
    });
});

}
