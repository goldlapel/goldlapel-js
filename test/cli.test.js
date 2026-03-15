import { describe, it } from 'node:test';
import assert from 'node:assert';
import { execFile } from 'node:child_process';
import { writeFileSync, unlinkSync, chmodSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { tmpdir } from 'node:os';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const CLI_PATH = join(__dirname, '..', 'cli.js');

function run(args, env) {
    return new Promise((resolve) => {
        execFile('node', [CLI_PATH, ...args], { env: { ...process.env, ...env } }, (err, stdout, stderr) => {
            resolve({
                code: err ? err.code : 0,
                stdout,
                stderr,
            });
        });
    });
}


describe('cli', () => {
    it('forwards args to the binary', async () => {
        const script = join(tmpdir(), `gl-test-echo-${process.pid}.sh`);
        writeFileSync(script, '#!/bin/sh\necho "ARGS:$*"\n');
        chmodSync(script, 0o755);

        try {
            const result = await run(['activate', 'tok_abc123'], { GOLDLAPEL_BINARY: script });
            assert.strictEqual(result.code, 0);
            assert.match(result.stdout, /ARGS:activate tok_abc123/);
        } finally {
            unlinkSync(script);
        }
    });

    it('propagates exit code from binary', async () => {
        const script = join(tmpdir(), `gl-test-exit-${process.pid}.sh`);
        writeFileSync(script, '#!/bin/sh\nexit 42\n');
        chmodSync(script, 0o755);

        try {
            const result = await run([], { GOLDLAPEL_BINARY: script });
            assert.strictEqual(result.code, 42);
        } finally {
            unlinkSync(script);
        }
    });

    it('exits with code 1 when binary not found', async () => {
        const result = await run([], { GOLDLAPEL_BINARY: '/nonexistent/goldlapel-fake' });
        assert.strictEqual(result.code, 1);
        assert.match(result.stderr, /GOLDLAPEL_BINARY/);
    });

    it('forwards stdout from binary', async () => {
        const script = join(tmpdir(), `gl-test-stdout-${process.pid}.sh`);
        writeFileSync(script, '#!/bin/sh\necho "hello from goldlapel"\n');
        chmodSync(script, 0o755);

        try {
            const result = await run([], { GOLDLAPEL_BINARY: script });
            assert.strictEqual(result.code, 0);
            assert.match(result.stdout, /hello from goldlapel/);
        } finally {
            unlinkSync(script);
        }
    });

    it('forwards stderr from binary', async () => {
        const script = join(tmpdir(), `gl-test-stderr-${process.pid}.sh`);
        writeFileSync(script, '#!/bin/sh\necho "warn: something" >&2\n');
        chmodSync(script, 0o755);

        try {
            const result = await run([], { GOLDLAPEL_BINARY: script });
            assert.strictEqual(result.code, 0);
            assert.match(result.stderr, /warn: something/);
        } finally {
            unlinkSync(script);
        }
    });
});
