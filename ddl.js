// DDL API client — fetches canonical helper-table DDL + query patterns from
// the Rust proxy's dashboard port so the wrapper never hand-writes CREATE TABLE
// for helper families (streams, docs, counters, ...).
//
// Architecture: see docs/wrapper-v0.2/SCHEMA-TO-CORE-PLAN.md in the goldlapel repo.
//
// - One HTTP call per (family, name) per session (cached).
// - Cache key: (family, name). Value: { tables, query_patterns }.
// - Cache lives on the wrapper instance via WeakMap.
// - Errors: HTTP failures throw with actionable messages.
//
// Token + port resolution:
// - `GoldLapel` passes dashboardPort + dashboardToken explicitly when it
//   spawned the proxy subprocess (the happy path).
// - For externally-launched proxies, wrapper reads
//   GOLDLAPEL_DASHBOARD_TOKEN env or ~/.goldlapel/dashboard-token file.

import { readFileSync, existsSync } from 'fs';
import { homedir } from 'os';
import { join } from 'path';

const SUPPORTED_VERSIONS = {
    stream: 'v1',
    doc_store: 'v1',
    counter: 'v1',
    zset: 'v1',
    hash: 'v1',
    queue: 'v1',
    geo: 'v1',
};

// Per-instance cache. WeakMap so entries are GC'd with the owner.
const _CACHE = new WeakMap();

export function supportedVersion(family) {
    return SUPPORTED_VERSIONS[family];
}

export function tokenFromEnvOrFile() {
    const env = process.env.GOLDLAPEL_DASHBOARD_TOKEN;
    if (env && env.trim()) return env.trim();
    const path = join(homedir(), '.goldlapel', 'dashboard-token');
    if (existsSync(path)) {
        try {
            const text = readFileSync(path, 'utf8').trim();
            return text || null;
        } catch {
            return null;
        }
    }
    return null;
}

function _cacheFor(owner) {
    let bucket = _CACHE.get(owner);
    if (!bucket) {
        bucket = new Map();
        _CACHE.set(owner, bucket);
    }
    return bucket;
}

async function _postDefault(url, token, body) {
    // Node 18+ has global fetch. We don't pin a lower bound here because the
    // rest of the wrapper already assumes Node 18+.
    let resp;
    try {
        resp = await fetch(url, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-GL-Dashboard': token,
            },
            body: JSON.stringify(body),
        });
    } catch (e) {
        throw new Error(
            `Gold Lapel dashboard not reachable at ${url}: ${e.message}. ` +
            `Is \`goldlapel\` running? The dashboard port must be open for ` +
            `helper families (streams, docs, ...) to work.`
        );
    }
    const text = await resp.text();
    let parsed;
    try {
        parsed = text ? JSON.parse(text) : {};
    } catch {
        parsed = { _raw: text };
    }
    return { status: resp.status, body: parsed };
}

// Fetch (and cache) the canonical { tables, query_patterns } for a helper.
//
// Per-session cache: one HTTP call on the first call for a given
// (family, name); cached result for every subsequent call in the same
// session.
//
// `options` is a per-family creation options bag (e.g. doc_store accepts
// `{ unlogged: true }`). Only used on the create call — once the table
// exists, its shape is fixed and subsequent options are silently ignored on
// the proxy side (idempotent CREATE TABLE IF NOT EXISTS).
export async function fetchPatterns(owner, family, name, dashboardPort, dashboardToken, { options = null } = {}) {
    const cache = _cacheFor(owner);
    const key = `${family}:${name}`;
    if (cache.has(key)) return cache.get(key);

    if (!dashboardToken) {
        throw new Error(
            'No dashboard token available. Set GOLDLAPEL_DASHBOARD_TOKEN or let ' +
            'GoldLapel spawn the proxy (which provisions a token automatically).'
        );
    }
    if (!dashboardPort) {
        throw new Error(
            `No dashboard port available. Gold Lapel's helper families (${family}, ...) ` +
            `require the proxy's dashboard to be reachable.`
        );
    }

    const url = `http://127.0.0.1:${dashboardPort}/api/ddl/${family}/create`;
    const reqBody = {
        name,
        schema_version: supportedVersion(family),
    };
    if (options) reqBody.options = options;
    // Indirect through `_internals.post` so tests can swap the HTTP layer
    // for a counting spy — see test/streams-integration.test.js + test/ddl.test.js.
    const { status, body } = await _internals.post(url, dashboardToken, reqBody);

    if (status !== 200) {
        const error = (body && body.error) || 'unknown';
        const detail = (body && body.detail) || String(body);
        if (status === 409 && error === 'version_mismatch') {
            throw new Error(
                `Gold Lapel schema version mismatch for ${family} '${name}': ${detail}. ` +
                `Upgrade the proxy or the wrapper so versions agree.`
            );
        }
        if (status === 403) {
            throw new Error(
                'Gold Lapel dashboard rejected the DDL request (403). ' +
                'The dashboard token is missing or incorrect — check ' +
                'GOLDLAPEL_DASHBOARD_TOKEN or ~/.goldlapel/dashboard-token.'
            );
        }
        throw new Error(
            `Gold Lapel DDL API ${family}/${name} failed with ${status} ${error}: ${detail}`
        );
    }

    const entry = {
        tables: body.tables,
        query_patterns: body.query_patterns,
    };
    cache.set(key, entry);
    return entry;
}

export function invalidate(owner) {
    _CACHE.delete(owner);
}

// Internal test hooks (not part of the public API).
// Tests can swap `_internals.post` to instrument or fake the HTTP layer.
// Mutable object — do NOT freeze.
export const _internals = {
    post: _postDefault,
    _cacheFor,
    _CACHE,
};
