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
    _findBinary,
    _makeProxyUrl,
    _waitForPort,
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


describe('module functions', () => {
    it('proxyUrl returns null when not started', () => {
        stop();
        assert.strictEqual(proxyUrl(), null);
    });
});
