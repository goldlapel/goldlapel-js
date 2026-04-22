// Regression test: goldlapel.start() must clean up its subprocess if the
// eager driver.connect() (or any step after _spawn()) raises after the
// subprocess has been spawned.
//
// Same bug class as Python and Ruby's subprocess-cleanup tests:
// (goldlapel-python/tests/test_v02_subprocess_cleanup.py,
//  goldlapel-ruby/test/test_v02_subprocess_cleanup.rb).
//
// Popen/spawn succeeded, the port got bound, but then driver.connect() failed
// (bad creds, fake non-Postgres listener, etc.) and the subprocess kept
// running indefinitely — orphaned, holding its port. The wrapper's start()
// must guarantee subprocess teardown in the failure path.
//
// Strategy: point GOLDLAPEL_BINARY at a fake binary that binds the proxy
// port (so _waitForPort resolves true and _spawn succeeds) but immediately
// closes any connection it accepts. The fake writes its own PID to a
// pidfile during startup. pg.Client.connect() then fails the Postgres
// startup handshake, triggering start()'s cleanup path. The test reads the
// pidfile AFTER start() rejects and uses process.kill(pid, 0) — the
// canonical "is this PID alive" check in Node — to verify the orphan was
// reaped.

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import { writeFileSync, unlinkSync, chmodSync, readFileSync, existsSync, mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { createServer } from 'node:net';

import { start, GoldLapel } from '../index.js';

// Fake binary source. Node script (executed via shebang) that:
//   1. Parses --proxy-port N from argv.
//   2. Writes its PID to the pidfile passed via GL_FAKE_PIDFILE.
//   3. Binds TCP on 127.0.0.1:N and destroys any incoming socket
//      immediately (so pg's startup handshake fails predictably).
//   4. Holds open until killed.
const FAKE_BINARY_SRC = `#!/usr/bin/env node
const net = require('node:net');
const fs = require('node:fs');

const args = process.argv.slice(2);
let port = 7932;
for (let i = 0; i < args.length; i++) {
    if (args[i] === '--proxy-port' && i + 1 < args.length) {
        port = Number(args[i + 1]);
        break;
    }
}

const pidfile = process.env.GL_FAKE_PIDFILE;
if (!pidfile) {
    console.error('fake binary: GL_FAKE_PIDFILE not set');
    process.exit(2);
}

const server = net.createServer((sock) => {
    // Accept then slam shut — pg's startup handshake sees a dropped
    // connection and throws, which is the failure we want to inject.
    sock.destroy();
});

server.on('error', (err) => {
    console.error('fake binary bind error:', err.message);
    process.exit(1);
});

server.listen(port, '127.0.0.1', () => {
    // Write the PID only AFTER the port is bound — this mirrors the Rust
    // binary's "ready" signal (port bound) and ensures the test never
    // reads a stale/missing pidfile.
    fs.writeFileSync(pidfile, String(process.pid));
});

// Hold forever until SIGTERM/SIGKILL. Default signal handlers are what we
// want — the wrapper's kill() should terminate us, and the test asserts
// that it did.
`;

// Grabs a free TCP port by opening a listener on port 0, reading the OS-
// assigned port, and closing. A tiny TOCTOU race exists (another process
// could grab it between close and our fake binding it), but in practice
// it's reliable for test runs.
function pickFreePort() {
    return new Promise((resolve, reject) => {
        const srv = createServer();
        srv.unref();
        srv.on('error', reject);
        srv.listen(0, '127.0.0.1', () => {
            const { port } = srv.address();
            srv.close(() => resolve(port));
        });
    });
}

// Polls process.kill(pid, 0) until the PID is gone (ESRCH) or the deadline
// passes. Resolves true if the PID is confirmed dead, false on timeout.
// Deterministic: no arbitrary setTimeout waits, just a bounded polling
// loop with a small interval. 10s is generous — kill('SIGTERM') followed
// by the JS wrapper's 5s SIGKILL escalation gives us plenty of headroom.
async function waitForPidGone(pid, timeoutMs = 10_000, intervalMs = 25) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
        try {
            process.kill(pid, 0);
            // Still alive — wait and retry.
        } catch (err) {
            if (err.code === 'ESRCH') return true;
            // EPERM means the PID exists but we don't own it — shouldn't
            // happen here (we spawned it), but treat it as "still alive".
            if (err.code !== 'EPERM') throw err;
        }
        await new Promise((r) => setTimeout(r, intervalMs));
    }
    return false;
}

// Convenience: is the PID alive right now?
function isAlive(pid) {
    try {
        process.kill(pid, 0);
        return true;
    } catch (err) {
        if (err.code === 'ESRCH') return false;
        if (err.code === 'EPERM') return true; // exists, not ours
        throw err;
    }
}


describe('subprocess-cleanup-on-connect-failure', () => {
    let workdir;
    let fakeBinary;
    let pidfile;
    // Track every PID the fake binary ever wrote so after() can reap
    // stranded processes even if the pidfile has been overwritten by a
    // later test. Without this, a broken wrapper that leaked from test 1
    // would go unreaped because test 2's pidfile overwrites test 1's.
    const observedPids = new Set();
    const origBinary = process.env.GOLDLAPEL_BINARY;
    const origPidfile = process.env.GL_FAKE_PIDFILE;

    before(() => {
        workdir = mkdtempSync(join(tmpdir(), 'gl-js-cleanup-'));
        fakeBinary = join(workdir, 'goldlapel-fake');
        pidfile = join(workdir, 'fake.pid');
        writeFileSync(fakeBinary, FAKE_BINARY_SRC);
        chmodSync(fakeBinary, 0o755);
    });

    after(() => {
        // Restore env vars
        if (origBinary !== undefined) process.env.GOLDLAPEL_BINARY = origBinary;
        else delete process.env.GOLDLAPEL_BINARY;
        if (origPidfile !== undefined) process.env.GL_FAKE_PIDFILE = origPidfile;
        else delete process.env.GL_FAKE_PIDFILE;

        // Belt-and-suspenders: reap any stranded fake binaries so CI stays
        // clean. A leaked PID here means the wrapper's cleanup failed —
        // the test should have flagged it, but don't compound the problem
        // by leaving orphans behind.
        for (const pid of observedPids) {
            if (pid > 0 && isAlive(pid)) {
                try { process.kill(pid, 'SIGKILL'); } catch {}
            }
        }
        try { rmSync(workdir, { recursive: true, force: true }); } catch {}
    });

    // Record every PID the pidfile has held so after() can reap stranded
    // processes across test boundaries.
    function recordPidIfPresent() {
        if (!existsSync(pidfile)) return null;
        try {
            const pid = Number(readFileSync(pidfile, 'utf8').trim());
            if (Number.isInteger(pid) && pid > 0) {
                observedPids.add(pid);
                return pid;
            }
        } catch {}
        return null;
    }

    it('kills the spawned binary when driver.connect() fails after spawn', async () => {
        // Fresh pidfile per test run.
        if (existsSync(pidfile)) unlinkSync(pidfile);

        const port = await pickFreePort();
        process.env.GOLDLAPEL_BINARY = fakeBinary;
        process.env.GL_FAKE_PIDFILE = pidfile;

        // Real start(): the fake binary will bind the port (so _spawn
        // succeeds), then pg.Client.connect() will attempt to speak
        // Postgres to it and fail (the fake slams the socket shut on
        // every accept). This is the exact failure path the cleanup
        // code must handle.
        await assert.rejects(
            () => start('postgresql://u:p@localhost:1/nope', {
                proxyPort: port,
                silent: true,
            }),
            (err) => {
                // pg surfaces the failed handshake as some Error — don't
                // overfit on the exact message, just confirm something
                // threw so we know we're exercising the cleanup path.
                assert.ok(err instanceof Error, `expected Error, got ${err}`);
                return true;
            },
        );

        // Now verify the subprocess was cleaned up.
        const pid = recordPidIfPresent();
        assert.ok(pid !== null,
            'fake binary pidfile should exist with a valid PID — without it we cannot verify cleanup');

        // The subprocess MUST be gone. Wait (bounded) for the SIGTERM to
        // propagate — the wrapper calls proc.kill('SIGTERM') in stop(),
        // which is async delivery.
        const gone = await waitForPidGone(pid);
        assert.ok(
            gone,
            `subprocess PID ${pid} is still alive after start() failed — ` +
            `wrapper did not clean up its spawned binary (orphaned proxy). ` +
            `This is the Tests-Q6 regression.`,
        );
    });

    it('clears instance state after failed start()', async () => {
        // Parity with the Python test's `gl._process is None and gl._proxy_url
        // is None` assertion. Because start() constructs the GoldLapel
        // internally, we exercise the same cleanup path via the instance-
        // level API: spawn manually, force a failure, and verify stop()
        // leaves no dangling references.
        if (existsSync(pidfile)) unlinkSync(pidfile);

        const port = await pickFreePort();
        process.env.GOLDLAPEL_BINARY = fakeBinary;
        process.env.GL_FAKE_PIDFILE = pidfile;

        const gl = new GoldLapel('postgresql://u:p@localhost:1/nope', {
            proxyPort: port,
            silent: true,
        });

        // Simulate the exact start() flow but observe the instance directly.
        let threw = null;
        try {
            await gl._spawn();
            // Force _openDefaultConn to throw by pointing at a driver that
            // will fail the handshake (fake binary slams the socket).
            await gl._openDefaultConn();
        } catch (err) {
            threw = err;
            try { await gl.stop(); } catch {}
        }

        assert.ok(threw, 'expected _openDefaultConn to throw against the fake binary');
        assert.strictEqual(gl._process, null, 'instance must drop _process reference');
        assert.strictEqual(gl._proxyUrl, null, 'instance must drop _proxyUrl reference');
        assert.strictEqual(gl._defaultConn, null, 'instance must drop _defaultConn reference');
        assert.strictEqual(gl.running, false, 'running must be false after cleanup');

        // And the subprocess itself must be gone.
        const pid = recordPidIfPresent();
        if (pid !== null) {
            const gone = await waitForPidGone(pid);
            assert.ok(gone, `subprocess PID ${pid} still alive after stop()`);
        }
    });
});
