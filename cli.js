#!/usr/bin/env node
import { _findBinary } from './index.js';
import { spawn } from 'node:child_process';

let binary;
try {
    binary = _findBinary();
} catch (err) {
    console.error(`Error: ${err.message}`);
    process.exit(1);
}
const child = spawn(binary, process.argv.slice(2), { stdio: 'inherit' });

child.on('error', (err) => {
    console.error(`Error: ${err.message}`);
    process.exit(1);
});

child.on('exit', (code, signal) => {
    if (signal) {
        process.kill(process.pid, signal);
    } else {
        process.exit(code ?? 1);
    }
});
