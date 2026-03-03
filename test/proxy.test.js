const { describe, it, before, after } = require('node:test');
const assert = require('node:assert');
const net = require('net');
const path = require('path');
const fs = require('fs');
const os = require('os');

const {
    GoldLapel,
    stop,
    proxyUrl,
    _findBinary,
    _replacePort,
    _waitForPort,
} = require('../index');


describe('findBinary', () => {
    it('finds binary via env var', (t) => {
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


describe('replacePort', () => {
    it('replaces port in postgresql URL', () => {
        const result = _replacePort('postgresql://user:pass@localhost:5432/mydb', 7932);
        assert.match(result, /7932/);
        assert.match(result, /postgresql/);
        assert.match(result, /user:pass/);
    });

    it('replaces port in postgres URL', () => {
        const result = _replacePort('postgres://user:pass@dbhost:5432/mydb', 7932);
        assert.match(result, /7932/);
        assert.match(result, /user:pass/);
    });

    it('replaces port in bare host:port', () => {
        assert.strictEqual(_replacePort('localhost:5432', 7932), 'localhost:7932');
    });

    it('appends port to bare host', () => {
        assert.strictEqual(_replacePort('localhost', 7932), 'localhost:7932');
    });

    it('preserves query params', () => {
        const result = _replacePort('postgresql://user:pass@localhost:5432/mydb?sslmode=require', 7932);
        assert.match(result, /7932/);
        assert.match(result, /sslmode=require/);
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
});


describe('module functions', () => {
    it('proxyUrl returns null when not started', () => {
        stop();
        assert.strictEqual(proxyUrl(), null);
    });
});
