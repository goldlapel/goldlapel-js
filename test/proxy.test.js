import { describe, it } from 'node:test';
import assert from 'node:assert';
import net from 'net';
import path from 'path';
import fs from 'fs';
import os from 'os';

import {
    GoldLapel,
    configKeys,
    _applicationNameMarker,
    _findBinary,
    _makeProxyUrl,
    _waitForPort,
    _configToArgs,
} from '../index.js';

// The proxy URL gets `application_name=goldlapel:js:<version>` appended so
// the proxy can classify wrapper-vs-raw traffic and skip L2 cache for
// wrappers (they have their own L1). The suffix is computed from the
// installed package.json — local dev installs see "0.0.0".
const _APP_NAME_SUFFIX = `application_name=${_applicationNameMarker()}`;

// Every test in this file runs without PGAPPNAME so the marker is applied
// deterministically. (A developer with PGAPPNAME set in their shell would
// otherwise see different URLs.)
const _origPgappname = process.env.PGAPPNAME;
delete process.env.PGAPPNAME;
process.on('exit', () => {
    if (_origPgappname !== undefined) process.env.PGAPPNAME = _origPgappname;
});


describe('findBinary', () => {
    it('finds binary via env var', () => {
        const tmp = path.join(os.tmpdir(), 'goldlapel-test-binary');
        fs.writeFileSync(tmp, '');

        const orig = process.env.GOLDLAPEL_BINARY;
        process.env.GOLDLAPEL_BINARY = tmp;
        try {
            assert.strictEqual(_findBinary(), tmp);
        } finally {
            if (orig !== undefined) {
                process.env.GOLDLAPEL_BINARY = orig;
            } else {
                delete process.env.GOLDLAPEL_BINARY;
            }
            fs.unlinkSync(tmp);
        }
    });

    it('throws when env var points to missing file', () => {
        const orig = process.env.GOLDLAPEL_BINARY;
        process.env.GOLDLAPEL_BINARY = '/nonexistent/goldlapel';
        try {
            assert.throws(() => _findBinary(), /GOLDLAPEL_BINARY/);
        } finally {
            if (orig !== undefined) {
                process.env.GOLDLAPEL_BINARY = orig;
            } else {
                delete process.env.GOLDLAPEL_BINARY;
            }
        }
    });
});


describe('makeProxyUrl', () => {
    it('replaces host and port in postgresql URL', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@dbhost:5432/mydb', 7932),
            `postgresql://user:pass@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('replaces host and port in postgres URL', () => {
        assert.strictEqual(
            _makeProxyUrl('postgres://user:pass@remote.aws.com:5432/mydb', 7932),
            `postgres://user:pass@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles pg URL without explicit port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@host.aws.com/mydb', 7932),
            `postgresql://user:pass@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles pg URL without port or path', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@host.aws.com', 7932),
            `postgresql://user:pass@localhost:7932?${_APP_NAME_SUFFIX}`
        );
    });

    it('replaces port in bare host:port', () => {
        // Bare-host form skips the marker — atypical caller path.
        assert.strictEqual(_makeProxyUrl('dbhost:5432', 7932), 'localhost:7932');
    });

    it('replaces bare host', () => {
        assert.strictEqual(_makeProxyUrl('dbhost', 7932), 'localhost:7932');
    });

    it('preserves query params', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@remote:5432/mydb?sslmode=require', 7932),
            `postgresql://user:pass@localhost:7932/mydb?sslmode=require&${_APP_NAME_SUFFIX}`
        );
    });

    it('preserves percent-encoded characters in password', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p%40ss@remote:5432/mydb', 7932),
            `postgresql://user:p%40ss@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles URL without userinfo', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://dbhost:5432/mydb', 7932),
            `postgresql://localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles URL without userinfo and without port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://dbhost/mydb', 7932),
            `postgresql://localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('keeps localhost when upstream is already localhost', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@localhost:5432/mydb', 7932),
            `postgresql://user:pass@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles @ in password with port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host:5432/mydb', 7932),
            `postgresql://user:p@ss@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles @ in password without port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host/mydb', 7932),
            `postgresql://user:p@ss@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles @ in password with query params', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue', 7932),
            `postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue&${_APP_NAME_SUFFIX}`
        );
    });

    it('handles password starting with digits without explicit port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:1pass@host/mydb', 7932),
            `postgresql://user:1pass@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles all-digit password without explicit port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:5432@host/mydb', 7932),
            `postgresql://user:5432@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles password starting with digits with explicit port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:9999@host:5432/mydb', 7932),
            `postgresql://user:9999@localhost:7932/mydb?${_APP_NAME_SUFFIX}`
        );
    });

    it('handles all-digit password without port or path', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:12345@host', 7932),
            `postgresql://user:12345@localhost:7932?${_APP_NAME_SUFFIX}`
        );
    });
});


describe('applicationNameMarker', () => {
    // L2-router architecture: the wrapper tags PG connections with
    // `application_name=goldlapel:js:<version>` so the proxy can classify
    // wrapper-vs-raw traffic and gate L2 result cache (wrapper has its
    // own L1; raw clients don't).

    it('marker has goldlapel:js:<version> shape', () => {
        const m = _applicationNameMarker();
        assert.match(m, /^goldlapel:js:.+$/);
    });

    it('appends marker when no existing query', () => {
        const out = _makeProxyUrl('postgresql://localhost:5432/mydb', 7932);
        assert.ok(out.includes(`?${_APP_NAME_SUFFIX}`), `expected suffix in ${out}`);
    });

    it('appends marker after existing query params', () => {
        const out = _makeProxyUrl('postgresql://localhost:5432/mydb?sslmode=require', 7932);
        assert.ok(out.includes('sslmode=require'));
        assert.ok(out.includes(`&${_APP_NAME_SUFFIX}`));
    });

    it('respects user-set application_name in URL (does not override)', () => {
        const out = _makeProxyUrl('postgresql://localhost:5432/mydb?application_name=my-app', 7932);
        assert.ok(out.includes('application_name=my-app'));
        assert.ok(!out.includes('goldlapel:js'));
    });

    it('respects PGAPPNAME env var (does not override)', () => {
        process.env.PGAPPNAME = 'my-app';
        try {
            const out = _makeProxyUrl('postgresql://localhost:5432/mydb', 7932);
            assert.ok(!out.includes('application_name='));
            assert.ok(!out.includes('goldlapel:js'));
        } finally {
            delete process.env.PGAPPNAME;
        }
    });
});


describe('waitForPort', () => {
    it('returns true for open port', async () => {
        const server = net.createServer();
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const port = server.address().port;

        try {
            const result = await _waitForPort('127.0.0.1', port, 1000);
            assert.strictEqual(result, true);
        } finally {
            server.close();
        }
    });

    it('returns false for closed port', async () => {
        const result = await _waitForPort('127.0.0.1', 19999, 200);
        assert.strictEqual(result, false);
    });
});


describe('GoldLapel class', () => {
    it('uses default proxy port', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._proxyPort, 7932);
    });

    it('accepts custom proxy port', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { proxyPort: 9000 });
        assert.strictEqual(gl._proxyPort, 9000);
    });

    it('not running initially', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });

    it('stop() is no-op when never started', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await gl.stop();
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });

    it('stop() is idempotent', async () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        await gl.stop();
        await gl.stop();
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });
});


describe('dashboardUrl', () => {
    it('default dashboard port is 7933', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._dashboardPort, 7933);
    });

    it('dashboard port inside config map is rejected', () => {
        // Regression guard: dashboardPort was promoted out of the config
        // map to a top-level option. Passing it via `config` must raise.
        assert.throws(
            () => new GoldLapel('postgresql://localhost:5432/mydb', {
                config: { dashboardPort: 8080 },
            }),
            { message: /Unknown config keys: dashboardPort/ }
        );
    });

    it('derives from custom proxy port when dashboardPort not supplied', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            proxyPort: 17932,
        });
        assert.strictEqual(gl._dashboardPort, 17933);
    });

    it('explicit dashboardPort wins over proxy-port derivation', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            proxyPort: 17932,
            dashboardPort: 9999,
        });
        assert.strictEqual(gl._dashboardPort, 9999);
    });

    it('custom port from top-level opt', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            dashboardPort: 8080,
        });
        assert.strictEqual(gl._dashboardPort, 8080);
    });

    it('top-level dashboardPort=0 disables (dashboardUrl returns null)', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            dashboardPort: 0,
        });
        assert.strictEqual(gl._dashboardPort, 0);
        assert.strictEqual(gl.dashboardUrl, null);
    });

    it('not running returns null', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl.dashboardUrl, null);
    });

    it('returns null when process exited on its own', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        // Simulate a process that exited naturally (not via .kill())
        gl._process = { exitCode: 1, killed: false, kill() {} };
        assert.strictEqual(gl.dashboardUrl, null);
    });

    it('returns URL when process is still running', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        // Simulate a running process
        gl._process = { exitCode: null, killed: false, kill() {} };
        assert.strictEqual(gl.dashboardUrl, 'http://127.0.0.1:7933');
    });
});


describe('configToArgs', () => {
    it('converts string value to correct flags', () => {
        const args = _configToArgs({ poolMode: 'transaction' });
        assert.deepStrictEqual(args, ['--pool-mode', 'transaction']);
    });

    it('converts numeric value to stringified flag', () => {
        const args = _configToArgs({ poolSize: 10 });
        assert.deepStrictEqual(args, ['--pool-size', '10']);
    });

    it('includes flag for boolean true', () => {
        const args = _configToArgs({ disableMatviews: true });
        assert.deepStrictEqual(args, ['--disable-matviews']);
    });

    it('omits flag for boolean false', () => {
        const args = _configToArgs({ disableMatviews: false });
        assert.deepStrictEqual(args, []);
    });

    it('repeats flags for array values', () => {
        const args = _configToArgs({ replica: ['host1:5432', 'host2:5432'] });
        assert.deepStrictEqual(args, [
            '--replica', 'host1:5432',
            '--replica', 'host2:5432',
        ]);
    });

    it('repeats --exclude-tables for array', () => {
        const args = _configToArgs({ excludeTables: ['logs', 'sessions'] });
        assert.deepStrictEqual(args, [
            '--exclude-tables', 'logs',
            '--exclude-tables', 'sessions',
        ]);
    });

    it('throws Error for unknown key', () => {
        assert.throws(
            () => _configToArgs({ bogusKey: 'value' }),
            { name: 'Error', message: /Unknown config keys: bogusKey/ }
        );
    });

    it('converts multiple keys to all flags', () => {
        const args = _configToArgs({ poolMode: 'transaction', poolSize: 5, disablePool: true });
        assert.ok(args.includes('--pool-mode'));
        assert.ok(args.includes('transaction'));
        assert.ok(args.includes('--pool-size'));
        assert.ok(args.includes('5'));
        assert.ok(args.includes('--disable-pool'));
    });

    it('returns empty array for empty config', () => {
        assert.deepStrictEqual(_configToArgs({}), []);
    });

    it('returns empty array for undefined config', () => {
        assert.deepStrictEqual(_configToArgs(undefined), []);
    });

    it('throws TypeError for boolean key with non-boolean value', () => {
        assert.throws(
            () => _configToArgs({ disableRewrite: 'yes' }),
            { name: 'TypeError', message: /expects a boolean, got string/ }
        );
    });

    it('wraps string value in array for list keys (replica)', () => {
        const args = _configToArgs({ replica: 'host1:5432' });
        assert.deepStrictEqual(args, ['--replica', 'host1:5432']);
    });

    it('wraps string value in array for list keys (excludeTables)', () => {
        const args = _configToArgs({ excludeTables: 'logs' });
        assert.deepStrictEqual(args, ['--exclude-tables', 'logs']);
    });

    it('throws TypeError for non-array non-string list key value', () => {
        assert.throws(
            () => _configToArgs({ replica: 42 }),
            { name: 'TypeError', message: /expects an array or string, got number/ }
        );
    });

    it('config passed through constructor is stored', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            config: { poolMode: 'transaction', disablePool: true },
        });
        assert.deepStrictEqual(gl._config, { poolMode: 'transaction', disablePool: true });
    });

    it('rejects log_level / mode / dashboardPort inside config map', () => {
        // Regression guard: these are top-level options on the canonical
        // surface. Passing them through `config` must raise.
        for (const bad of ['logLevel', 'mode', 'dashboardPort', 'invalidationPort', 'license', 'client', 'config']) {
            assert.throws(
                () => new GoldLapel('postgresql://localhost:5432/mydb', {
                    config: { [bad]: 'x' },
                }),
                { message: new RegExp(`Unknown config keys: ${bad}`) },
                `expected rejection of promoted top-level key '${bad}' in config map`
            );
        }
    });
});


describe('mesh startup options', () => {
    // Top-level canonical-surface options: mesh (bool) + meshTag (string).
    // Translate to --mesh / --mesh-tag CLI flags; never valid inside `config`.

    it('defaults to disabled', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._mesh, false);
        assert.strictEqual(gl._meshTag, null);
    });

    it('stores mesh=true', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { mesh: true });
        assert.strictEqual(gl._mesh, true);
    });

    it('stores meshTag', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            mesh: true, meshTag: 'prod-east',
        });
        assert.strictEqual(gl._meshTag, 'prod-east');
    });

    it('emits --mesh and --mesh-tag flags when set', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            mesh: true, meshTag: 'prod-east',
        });
        const args = gl._buildSpawnArgs();
        assert.ok(args.includes('--mesh'));
        const idx = args.indexOf('--mesh-tag');
        assert.ok(idx >= 0);
        assert.strictEqual(args[idx + 1], 'prod-east');
    });

    it('omits --mesh flags when unset', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        const args = gl._buildSpawnArgs();
        assert.ok(!args.includes('--mesh'));
        assert.ok(!args.includes('--mesh-tag'));
    });

    it('emits --mesh alone when meshTag omitted', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { mesh: true });
        const args = gl._buildSpawnArgs();
        assert.ok(args.includes('--mesh'));
        assert.ok(!args.includes('--mesh-tag'));
    });

    it('rejects mesh / meshTag inside config map', () => {
        for (const bad of ['mesh', 'meshTag']) {
            assert.throws(
                () => new GoldLapel('postgresql://localhost:5432/mydb', {
                    config: { [bad]: true },
                }),
                { message: new RegExp(`Unknown config keys: ${bad}`) },
            );
        }
    });

    it('mesh / meshTag are not valid config keys', () => {
        const keys = configKeys();
        assert.ok(!keys.has('mesh'));
        assert.ok(!keys.has('meshTag'));
    });
});


describe('configKeys', () => {
    it('returns a Set of valid config keys', () => {
        const keys = configKeys();
        assert.ok(keys instanceof Set);
        // Tuning knobs still live in the structured config map.
        assert.ok(keys.has('poolSize'));
        assert.ok(keys.has('disableMatviews'));
        // Top-level concepts must NOT appear — passing them via config is a
        // user error.
        assert.ok(!keys.has('mode'));
        assert.ok(!keys.has('logLevel'));
        assert.ok(!keys.has('dashboardPort'));
    });

    it('returns a new Set each call (not the internal reference)', () => {
        const a = configKeys();
        const b = configKeys();
        assert.notStrictEqual(a, b);
    });
});
