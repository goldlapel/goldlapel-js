import { describe, it } from 'node:test';
import assert from 'node:assert';
import net from 'net';
import path from 'path';
import fs from 'fs';
import os from 'os';

import {
    GoldLapel,
    stop,
    proxyUrl,
    dashboardUrl,
    configKeys,
    _findBinary,
    _makeProxyUrl,
    _waitForPort,
    _configToArgs,
} from '../index.js';


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
            'postgresql://user:pass@localhost:7932/mydb'
        );
    });

    it('replaces host and port in postgres URL', () => {
        assert.strictEqual(
            _makeProxyUrl('postgres://user:pass@remote.aws.com:5432/mydb', 7932),
            'postgres://user:pass@localhost:7932/mydb'
        );
    });

    it('handles pg URL without explicit port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@host.aws.com/mydb', 7932),
            'postgresql://user:pass@localhost:7932/mydb'
        );
    });

    it('handles pg URL without port or path', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@host.aws.com', 7932),
            'postgresql://user:pass@localhost:7932'
        );
    });

    it('replaces port in bare host:port', () => {
        assert.strictEqual(_makeProxyUrl('dbhost:5432', 7932), 'localhost:7932');
    });

    it('replaces bare host', () => {
        assert.strictEqual(_makeProxyUrl('dbhost', 7932), 'localhost:7932');
    });

    it('preserves query params', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@remote:5432/mydb?sslmode=require', 7932),
            'postgresql://user:pass@localhost:7932/mydb?sslmode=require'
        );
    });

    it('preserves percent-encoded characters in password', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p%40ss@remote:5432/mydb', 7932),
            'postgresql://user:p%40ss@localhost:7932/mydb'
        );
    });

    it('handles URL without userinfo', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://dbhost:5432/mydb', 7932),
            'postgresql://localhost:7932/mydb'
        );
    });

    it('handles URL without userinfo and without port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://dbhost/mydb', 7932),
            'postgresql://localhost:7932/mydb'
        );
    });

    it('keeps localhost when upstream is already localhost', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:pass@localhost:5432/mydb', 7932),
            'postgresql://user:pass@localhost:7932/mydb'
        );
    });

    it('handles @ in password with port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host:5432/mydb', 7932),
            'postgresql://user:p@ss@localhost:7932/mydb'
        );
    });

    it('handles @ in password without port', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host/mydb', 7932),
            'postgresql://user:p@ss@localhost:7932/mydb'
        );
    });

    it('handles @ in password with query params', () => {
        assert.strictEqual(
            _makeProxyUrl('postgresql://user:p@ss@host:5432/mydb?sslmode=require&param=val@ue', 7932),
            'postgresql://user:p@ss@localhost:7932/mydb?sslmode=require&param=val@ue'
        );
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
    it('uses default port', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._port, 7932);
    });

    it('accepts custom port', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', { port: 9000 });
        assert.strictEqual(gl._port, 9000);
    });

    it('not running initially', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });

    it('stop() is no-op when never started', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl.stop();
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });

    it('stop() is idempotent', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        gl.stop();
        gl.stop();
        assert.strictEqual(gl.running, false);
        assert.strictEqual(gl.url, null);
    });
});


describe('dashboardUrl', () => {
    it('default dashboard port is 7933', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl._dashboardPort, 7933);
    });

    it('custom port from config', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            config: { dashboardPort: 8080 },
        });
        assert.strictEqual(gl._dashboardPort, 8080);
    });

    it('port 0 means disabled (dashboardUrl returns null)', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            config: { dashboardPort: 0 },
        });
        assert.strictEqual(gl._dashboardPort, 0);
        assert.strictEqual(gl.dashboardUrl, null);
    });

    it('not running returns null', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb');
        assert.strictEqual(gl.dashboardUrl, null);
    });

    it('module-level dashboardUrl returns null when not started', () => {
        stop();
        assert.strictEqual(dashboardUrl(), null);
    });
});


describe('configToArgs', () => {
    it('converts string value to correct flags', () => {
        const args = _configToArgs({ mode: 'butler' });
        assert.deepStrictEqual(args, ['--mode', 'butler']);
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
        const args = _configToArgs({ mode: 'butler', poolSize: 5, disablePool: true });
        assert.ok(args.includes('--mode'));
        assert.ok(args.includes('butler'));
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

    it('config passed through constructor is stored', () => {
        const gl = new GoldLapel('postgresql://localhost:5432/mydb', {
            config: { mode: 'butler', disablePool: true },
        });
        assert.deepStrictEqual(gl._config, { mode: 'butler', disablePool: true });
    });
});


describe('configKeys', () => {
    it('returns a Set of valid config keys', () => {
        const keys = configKeys();
        assert.ok(keys instanceof Set);
        assert.ok(keys.has('mode'));
        assert.ok(keys.has('poolSize'));
        assert.strictEqual(keys.size, 43);
    });

    it('returns a new Set each call (not the internal reference)', () => {
        const a = configKeys();
        const b = configKeys();
        assert.notStrictEqual(a, b);
    });
});


describe('module functions', () => {
    it('proxyUrl returns null when not started', () => {
        stop();
        assert.strictEqual(proxyUrl(), null);
    });
});
