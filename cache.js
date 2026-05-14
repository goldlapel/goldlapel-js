import { createConnection } from 'net';
import { existsSync, readFileSync } from 'fs';
import { platform } from 'os';
import { randomUUID } from 'crypto';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const DDL_SENTINEL = '__ddl__';

// --- Native cache telemetry tuning ---
//
// Demand-driven model (2026-05-03 — port of the Python pattern, see
// goldlapel-python/docs/wrapper-telemetry-pattern.md): the wrapper has
// NO background timer. Cache counters increment on cache ops (free);
// state-change events are emitted synchronously when a relevant counter
// crosses a threshold; snapshot replies are sent only when the proxy
// asks via `?:<request>` over the existing invalidation socket.
//
// Eviction-rate sliding window. cache_full fires when ≥ EVICT_RATE_HIGH
// of the last EVICT_RATE_WINDOW cache writes (puts) caused an eviction;
// cache_recovered fires when the rate falls back below EVICT_RATE_LOW.
// Hysteresis (50% / 10%) avoids flapping at the boundary.
const EVICT_RATE_WINDOW = 200;
const EVICT_RATE_HIGH = 0.5; // 50% of recent puts evicted → cache_full
const EVICT_RATE_LOW = 0.1;  // ≤ 10% → cache_recovered

const __dirname = dirname(fileURLToPath(import.meta.url));

// Wrapper version, read once at module init from package.json. Mirrors
// the `application_name` marker logic in index.js but is duplicated
// here so the cache module stays standalone (no circular import).
function _readWrapperVersion() {
    try {
        const raw = readFileSync(join(__dirname, 'package.json'), 'utf8');
        const pkg = JSON.parse(raw);
        return pkg.version || 'unknown';
    } catch {
        return 'unknown';
    }
}

const TX_START = /^\s*(BEGIN|START\s+TRANSACTION)\b/i;
const TX_END = /^\s*(COMMIT|ROLLBACK|END)\b/i;

const TABLE_PATTERN = /\b(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:(\w+)\.)?(\w+)/gi;

// ─── Unsafe-GUC state hash (Option Y, mirrored from proxy guc_state.rs) ────
//
// Custom-GUC-driven RLS — `SET app.user_id = '42'; SELECT * FROM accounts`
// where the policy reads `current_setting('app.user_id')` — is a real cache
// leak when keying purely by SQL+params: user A's cached rows get served to
// user B. The proxy fixed this in `src/guc_state.rs` (commit 3e02359) by
// folding a per-connection unsafe-GUC state hash into every cache /
// coalescing key. The wrapper's native cache has the same vulnerability,
// so we mirror the same fix here on a per-CachedClient basis.
//
// `SET LOCAL` is intentionally ignored: the wrapper's read path bypasses
// the cache entirely while `_inTransaction` is true, so transaction-scoped
// settings can never leak into a cacheable response.

const UNSAFE_GUC_SHORT_LIST = new Set([
    'search_path',
    'role',
    'session_authorization',
    'default_transaction_isolation',
    'default_transaction_read_only',
    'transaction_isolation',
    'row_security',
]);

// GUC names that are well-known safe — formatting / locale knobs that
// affect text rendering of values without changing which rows the user
// can see. Folded into the classifier as an explicit allowlist so the
// classification is documented (rather than relying on "happens not to
// be in the unsafe list and contains no `.`"). Comparison is
// case-insensitive — PG itself preserves the conventional casing
// (`DateStyle`, `IntervalStyle`, `TimeZone`) but accepts any case.
//
// These never enter `_values`, never participate in the state hash, and
// therefore can be set freely without fragmenting the cache.
const KNOWN_SAFE_GUCS = new Set([
    'datestyle',
    'intervalstyle',
    'timezone',
    'bytea_output',
    'lc_messages',
    'lc_monetary',
    'lc_numeric',
    'lc_time',
]);

// FNV-1a 32-bit. Cheap, stable, no deps. We don't need cryptographic
// strength — the hash is folded into a cache key alongside the SQL text,
// not used as a secret. 32 bits is plenty of headroom for the small
// per-connection key space (rarely more than a handful of unsafe GUCs).
function _fnv1a32(str) {
    let h = 0x811c9dc5;
    for (let i = 0; i < str.length; i++) {
        h ^= str.charCodeAt(i);
        // 32-bit FNV prime multiply, kept in 32-bit unsigned via `Math.imul`
        // and the `>>> 0` mask at the end.
        h = Math.imul(h, 0x01000193);
    }
    return h >>> 0;
}

// Classify a GUC name as state-affecting (true) or harmless (false).
//
// Order of checks (each is case-insensitive):
//   1. Known-safe explicit list — formatting / locale knobs that affect
//      text rendering only, never row visibility.
//   2. Unsafe shortlist — security-affecting session state (search_path,
//      role, session_authorization, transaction-isolation knobs,
//      row_security).
//   3. Namespaced GUCs (anything containing `.` — `app.*`, `myapp.*`,
//      `rls.*`) — the canonical custom-RLS pattern.
//   4. Default: safe (unrecognised GUC, treated as harmless — formatting
//      / planner cost knobs, statement_timeout, application_name, etc.).
//
// The known-safe list runs first so a future shortlist edit that
// accidentally adds e.g. `timezone` doesn't silently start fragmenting
// the cache on every `SET TIME ZONE` call.
function isUnsafeGuc(name) {
    if (typeof name !== 'string' || name.length === 0) return false;
    const lower = name.toLowerCase();
    if (KNOWN_SAFE_GUCS.has(lower)) return false;
    if (UNSAFE_GUC_SHORT_LIST.has(lower)) return true;
    if (lower.includes('.')) return true;
    return false;
}

// Strip a single layer of matching surrounding `'...'` or `"..."` quotes.
function _stripValueQuotes(value) {
    const v = value.trim();
    if (v.length >= 2) {
        const first = v[0];
        const last = v[v.length - 1];
        if ((first === "'" && last === "'") || (first === '"' && last === '"')) {
            return v.slice(1, -1);
        }
    }
    return v;
}

// Lowercase the GUC name and strip surrounding double quotes (PG treats
// `"app.user_id"` and `app.user_id` as the same configuration parameter).
function _normalizeGucName(token) {
    const trimmed = token.replace(/^"+|"+$/g, '');
    if (trimmed.length === 0) return null;
    return trimmed.toLowerCase();
}

// Recognise `SELECT [pg_catalog.]set_config(setting_name, new_value,
// is_local)` and return a SetCommand-shaped object so the state machine
// can apply it the same way as a regular `SET ...`. Returns `null` for
// anything else (including `set_config(...)` calls embedded in larger
// SELECTs — only top-level invocations are tracked, mirroring the
// proxy-side conservatism).
//
// Argument splitter walks the inside of the outermost `(...)`, respects
// `'...'` / `"..."` literals (including PG's doubled-quote `''` / `""`
// escape), and tolerates whitespace / case variations:
//
//   SELECT set_config('app.user_id', '42', false)
//   SELECT pg_catalog.set_config('app.user_id', '42', FALSE)
//   SELECT SET_CONFIG ('app.user_id', '42', f)
//
// `is_local` accepts the standard PG boolean spellings: `true/false`,
// `t/f`, `yes/no`, `on/off`, `1/0` (case-insensitive). Any other value
// → null (we'd rather miss a tracking opportunity than misclassify).
function _parseSetConfigCall(sql) {
    if (typeof sql !== 'string') return null;
    // Strip leading SELECT, optional `pg_catalog.` qualifier, and
    // whitespace before the function name. Matched lazily so we can
    // anchor against the literal `set_config(` opener.
    const re = /^\s*SELECT\s+(?:pg_catalog\s*\.\s*)?set_config\s*\(/i;
    const m = sql.match(re);
    if (!m) return null;
    const argStart = m[0].length;

    // Walk forward to the matching `)`, respecting quoted literals and
    // nested `(...)` groups (set_config takes scalars but a paranoid
    // parser is cheap insurance against future syntax extensions).
    let depth = 1;
    let quote = null;
    let i = argStart;
    while (i < sql.length) {
        const c = sql[i];
        if (quote !== null) {
            if (c === quote) {
                if (i + 1 < sql.length && sql[i + 1] === quote) {
                    i += 2;
                    continue;
                }
                quote = null;
            }
        } else {
            if (c === "'" || c === '"') {
                quote = c;
            } else if (c === '(') {
                depth++;
            } else if (c === ')') {
                depth--;
                if (depth === 0) break;
            }
        }
        i++;
    }
    if (depth !== 0) return null;
    const inner = sql.slice(argStart, i);

    // After the closing `)`, only whitespace and an optional trailing `;`
    // are allowed. Anything else (e.g. `set_config(...) FROM ...`,
    // `set_config(...) || other`) is not a top-level invocation we should
    // track.
    const tail = sql.slice(i + 1).trim();
    if (tail.length > 0 && tail !== ';') return null;

    // Comma-split inner, respecting quoted literals. set_config is
    // strictly 3-arg.
    const args = _splitTopLevelCommas(inner);
    if (args.length !== 3) return null;

    const nameRaw = args[0].trim();
    const valueRaw = args[1].trim();
    const isLocalRaw = args[2].trim();

    // The first arg MUST be a string literal containing the GUC name —
    // PG also accepts an expression there, but evaluating expressions
    // accurately is out of scope (Option Y v1 limitation). Detect the
    // literal case by checking that quote-stripping actually peeled
    // something; bare-identifier args are rejected.
    const nameStripped = _stripValueQuotes(nameRaw);
    if (nameStripped === nameRaw || nameStripped.length === 0) return null;
    const name = _normalizeGucName(nameStripped);
    if (!name) return null;

    // Same logic for the value — accept either a string literal or a
    // bare unquoted token (PG coerces non-text args to text via the
    // function signature `set_config(text, text, bool)`, but most callers
    // pass quoted strings). Don't allow expressions.
    const value = _stripValueQuotes(valueRaw);

    // is_local: PG boolean. Reject anything ambiguous — better to skip
    // tracking than to misclassify and let a session-scoped change
    // mascarade as transaction-local (or vice versa).
    const isLocal = _parsePgBool(isLocalRaw);
    if (isLocal === null) return null;

    return isLocal
        ? { kind: 'set_local', name, value }
        : { kind: 'set', name, value };
}

// Split `inner` (the contents between an outer `(...)`) on top-level
// commas, respecting `'...'` / `"..."` literals (including PG's
// doubled-quote escape) and nested `(...)` groups.
function _splitTopLevelCommas(inner) {
    const out = [];
    let start = 0;
    let depth = 0;
    let quote = null;
    let i = 0;
    while (i < inner.length) {
        const c = inner[i];
        if (quote !== null) {
            if (c === quote) {
                if (i + 1 < inner.length && inner[i + 1] === quote) {
                    i += 2;
                    continue;
                }
                quote = null;
            }
        } else {
            if (c === "'" || c === '"') {
                quote = c;
            } else if (c === '(') {
                depth++;
            } else if (c === ')') {
                if (depth > 0) depth--;
            } else if (c === ',' && depth === 0) {
                out.push(inner.slice(start, i));
                start = i + 1;
            }
        }
        i++;
    }
    out.push(inner.slice(start));
    return out;
}

// Best-effort PG boolean parser. Returns `true`, `false`, or `null` for
// anything ambiguous. Accepts the standard PG spellings (case-insensitive,
// surrounding quotes tolerated): `true/false`, `t/f`, `yes/no`, `on/off`,
// `1/0`.
function _parsePgBool(raw) {
    const v = _stripValueQuotes(raw.trim()).toLowerCase();
    if (v === 'true' || v === 't' || v === 'yes' || v === 'on' || v === '1') return true;
    if (v === 'false' || v === 'f' || v === 'no' || v === 'off' || v === '0') return false;
    return null;
}

// Replace the contents of `'...'` and `"..."` string literals with spaces,
// preserving overall length so positions line up with the original. PG's
// doubled-quote `''` / `""` escapes are handled the same way as in
// splitStatements. Used by detectWrite's SELECT branch so that bare words
// like `INTO` inside a literal (e.g. `SELECT 'INSERT INTO orders' FROM
// audit_log`) don't trip the SELECT-INTO DDL classifier.
function stripStringLiterals(sql) {
    if (typeof sql !== 'string' || sql.length === 0) return sql;
    const out = sql.split('');
    let quote = null;
    let i = 0;
    while (i < sql.length) {
        const c = sql[i];
        if (quote !== null) {
            if (c === quote) {
                if (i + 1 < sql.length && sql[i + 1] === quote) {
                    // Doubled-quote escape: blank both, stay inside literal.
                    out[i] = ' ';
                    out[i + 1] = ' ';
                    i += 2;
                    continue;
                }
                // Closing quote: leave the delimiter, drop the literal body.
                quote = null;
            } else {
                out[i] = ' ';
            }
        } else {
            if (c === "'" || c === '"') {
                quote = c;
            }
        }
        i++;
    }
    return out.join('');
}

// Split a SQL string on top-level `;` characters, respecting `'...'` and
// `"..."` string literals (including PG's doubled-quote `''` / `""`
// escape). No comment / dollar-quote support — this is the lightest
// possible splitter, just enough to handle multi-statement bodies like
// `SET app.user_id = '42'; SELECT 1`.
function splitStatements(sql) {
    if (typeof sql !== 'string' || sql.length === 0) return [];
    const out = [];
    let start = 0;
    let quote = null;
    let i = 0;
    while (i < sql.length) {
        const c = sql[i];
        if (quote !== null) {
            if (c === quote) {
                if (i + 1 < sql.length && sql[i + 1] === quote) {
                    i += 2;
                    continue;
                }
                quote = null;
            }
        } else {
            if (c === "'" || c === '"') {
                quote = c;
            } else if (c === ';') {
                const segment = sql.slice(start, i).trim();
                if (segment.length > 0) out.push(segment);
                start = i + 1;
            }
        }
        i++;
    }
    const tail = sql.slice(start).trim();
    if (tail.length > 0) out.push(tail);
    return out;
}

// Parse a single `SET ...` / `RESET ...` / `DISCARD ...` statement, OR a
// `SELECT [pg_catalog.]set_config(name, value, is_local)` function-form
// invocation. Returns one of:
//   { kind: 'set',         name, value }
//   { kind: 'set_local',   name, value }
//   { kind: 'reset',       name }
//   { kind: 'reset_all' }
//   { kind: 'discard_all' }
//   { kind: 'discard_plans' }
//   { kind: 'discard_other' }            // SEQUENCES / TEMP / TEMPORARY — no-op
// or null for anything else. Callers use the kind tag to drive
// `ConnectionGucState.apply()`.
//
// Recognised shapes (case-insensitive, trailing `;` tolerated):
//   SET name = value         SET name TO value
//   SET SESSION name = ...   SET SESSION name TO ...
//   SET LOCAL name = ...     SET LOCAL name TO ...
//   RESET name               RESET ALL
//   DISCARD ALL              DISCARD PLANS
//   DISCARD SEQUENCES        DISCARD TEMP / TEMPORARY
//   SELECT set_config('name', 'value', is_local)
//   SELECT pg_catalog.set_config('name', 'value', is_local)
// Everything else (e.g. `SET TIME ZONE ...`, plain queries) returns null.
function parseSetCommand(sql) {
    if (typeof sql !== 'string') return null;
    let s = sql.trim();
    if (s.endsWith(';')) s = s.slice(0, -1).trimEnd();
    if (s.length === 0) return null;

    const tokens = s.split(/\s+/);
    if (tokens.length === 0) return null;
    const head = tokens[0];
    const headUpper = head.toUpperCase();

    // RESET name  /  RESET ALL
    if (headUpper === 'RESET') {
        if (tokens.length < 2) return null;
        const target = tokens[1];
        // RESET takes exactly one arg.
        if (tokens.length > 2) return null;
        if (target.toUpperCase() === 'ALL') {
            return { kind: 'reset_all' };
        }
        const name = _normalizeGucName(target);
        if (!name) return null;
        return { kind: 'reset', name };
    }

    // DISCARD ALL / PLANS / SEQUENCES / TEMP / TEMPORARY.
    //
    // PG `DISCARD ALL` resets ALL session state — including custom GUCs like
    // `app.user_id`. Equivalent to `RESET ALL` for our purposes (we only
    // track unsafe-GUC values), so we map it to clearing the state map.
    // `DISCARD PLANS` clears the prepared-statement plan cache; we don't
    // maintain one, so it's a no-op for state but we tag it so callers can
    // wire prepared-statement caches in the future without touching this
    // parser. `DISCARD SEQUENCES / TEMP / TEMPORARY` are storage-side and
    // don't affect GUC state — emit a `discard_other` sentinel that
    // ConnectionGucState.apply() ignores.
    if (headUpper === 'DISCARD') {
        if (tokens.length < 2 || tokens.length > 2) return null;
        const targetUpper = tokens[1].toUpperCase();
        if (targetUpper === 'ALL') return { kind: 'discard_all' };
        if (targetUpper === 'PLANS') return { kind: 'discard_plans' };
        if (
            targetUpper === 'SEQUENCES'
            || targetUpper === 'TEMP'
            || targetUpper === 'TEMPORARY'
        ) {
            return { kind: 'discard_other' };
        }
        return null;
    }

    // SELECT [pg_catalog.]set_config(name, value, is_local)
    //
    // Supabase's canonical RLS pattern. `set_config()` is the function-form
    // equivalent of `SET name = value`; the third arg is `is_local` — when
    // `true`, the change reverts at end-of-tx (mirrors `SET LOCAL`); when
    // `false`, it's session-scoped (mirrors `SET`). Detected at the
    // statement level so a single Q-message body like
    // `SELECT set_config('app.user_id', '42', false)` updates the
    // per-connection state hash before the wrapper looks up the cache key.
    if (headUpper === 'SELECT') {
        const cfg = _parseSetConfigCall(s);
        if (cfg) return cfg;
        return null;
    }

    if (headUpper !== 'SET') return null;

    // Optional LOCAL / SESSION modifier.
    let idx = 1;
    if (idx >= tokens.length) return null;
    let isLocal = false;
    const modifier = tokens[idx].toUpperCase();
    if (modifier === 'LOCAL') {
        isLocal = true;
        idx++;
    } else if (modifier === 'SESSION') {
        // Default behavior — same as bare SET.
        idx++;
    }

    if (idx >= tokens.length) return null;
    let nameToken = tokens[idx];
    idx++;

    // The name token may have an `=` glued onto it, e.g. `SET app.user='42'`.
    let gluedValue = null;
    const eqIdx = nameToken.indexOf('=');
    if (eqIdx !== -1) {
        gluedValue = nameToken.slice(eqIdx + 1);
        nameToken = nameToken.slice(0, eqIdx);
        if (gluedValue.length === 0) gluedValue = null;
    }

    const name = _normalizeGucName(nameToken);
    if (!name) return null;

    let valueStr;
    if (gluedValue !== null) {
        // SET name=value rest...
        const rest = tokens.slice(idx).join(' ');
        valueStr = rest.length > 0 ? `${gluedValue} ${rest}` : gluedValue;
    } else {
        if (idx >= tokens.length) return null;
        const sep = tokens[idx];
        idx++;
        if (!(sep === '=' || sep.toUpperCase() === 'TO')) return null;
        valueStr = tokens.slice(idx).join(' ');
    }

    const value = _stripValueQuotes(valueStr.trim());
    if (value.length === 0 && valueStr.trim().length === 0) return null;

    return { kind: isLocal ? 'set_local' : 'set', name, value };
}

// Per-connection unsafe-GUC state. Backed by a Map keyed by lowercased GUC
// name (insertion order is normalised at hash time, so two states with the
// same {name → value} bindings hash the same regardless of SET order).
//
// `stateHash` returns 0 for empty/baseline state — a fresh connection's
// hash matches "no GUCs set" cache slots, which is exactly the correct
// behavior (cache hits across connections that never SET any unsafe GUC).
//
// Wave 2 (2026-05-05) — SET-actually-applied: state mutation is split into
// two phases. `parseSql()` returns parsed commands without applying them
// (so the wrapper can run the query first); `applyParsed()` commits them
// after the server confirms the SET took effect. Failed SETs (server
// rejects with ErrorResponse) discard the pending commands without ever
// touching `_values` / `_hash`, so wrapper state can never diverge from
// server-side state on a failed mutation.
//
// Transaction snapshots (`beginTx` / `commitTx` / `rollbackTx`): bare
// `SET` inside a transaction reverts on ROLLBACK server-side (only `SET
// LOCAL` is special-cased — that's already a wrapper no-op). Snapshot
// stack saves the pre-tx values + hash; ROLLBACK restores the snapshot,
// COMMIT discards it.
class ConnectionGucState {
    constructor() {
        this._values = new Map();
        this._hash = 0;
        // Monotonic counter bumped by `_bumpDmlSeq()` after every observed
        // DML when aggressive-verify is active. Folded into the state
        // hash so each post-DML read on this connection produces a fresh
        // cache slot — closes the trigger-internal-SET correctness gap:
        // a server-side trigger that did `SET app.user_id = ...` would
        // otherwise be invisible to the wire-side state observer, and a
        // cached pre-DML response could be served under stale state.
        //
        // Reset to 0 whenever the rest of the state is wiped (RESET ALL /
        // DISCARD ALL) so a recycled connection re-converges to a
        // peer-shareable baseline. Matches the proxy's `dml_seq` field
        // (see `src/guc_state.rs` ConnectionGucState::dml_seq).
        this._dmlSeq = 0;
        // Transaction snapshot stack — supports nested savepoint-like
        // usage in the future, but for now BEGIN/COMMIT/ROLLBACK push +
        // pop a single frame at a time. Each frame is `{values, hash,
        // dmlSeq}` captured at BEGIN — the `dmlSeq` save mirrors the
        // values save so a ROLLBACK undoing tx-internal SETs also undoes
        // tx-internal post-DML bumps (a server-side ROLLBACK reverts
        // both).
        this._txStack = [];
    }

    stateHash() {
        return this._hash;
    }

    // Bump the post-DML sequence counter so the next cache-key
    // computation on this connection produces a fresh slot. Called from
    // `CachedClient.query()` after every confirmed
    // INSERT/UPDATE/DELETE/MERGE/TRUNCATE/DDL when aggressive-verify is
    // active. The bump means any subsequent cacheable read on this
    // connection cannot share a slot with a pre-DML read from this same
    // connection — closing the trigger-internal-SET correctness gap.
    //
    // This is the v1 mitigation: cache-key isolation, not observation of
    // the trigger-applied GUC values. The server always knows its own
    // session state and returns correct rows from `_real.query`; the
    // wrapper just guarantees the cache can't hand back a stale response
    // keyed on the pre-trigger state.
    _bumpDmlSeq() {
        // JS numbers are double-precision floats; integer-safe up to
        // 2^53. Wrap at 2^32 to stay in 32-bit unsigned territory (same
        // domain as `_fnv1a32`'s output) — a connection issuing 4B DMLs
        // would wrap, and post-wrap collisions with the same `_values`
        // are statistically irrelevant.
        this._dmlSeq = (this._dmlSeq + 1) >>> 0;
        this._recomputeHash();
    }

    // Apply a parsed SET / RESET / DISCARD command. No-op for SetLocal
    // (transient — wrapper bypasses cache while `_inTransaction` anyway),
    // for safe GUC names, and for DISCARD subcommands that don't affect
    // GUC state (PLANS / SEQUENCES / TEMP / TEMPORARY). Returns true iff
    // the hash changed.
    apply(cmd) {
        if (!cmd) return false;
        const before = this._hash;
        switch (cmd.kind) {
            case 'set':
                if (isUnsafeGuc(cmd.name)) {
                    this._values.set(cmd.name, cmd.value);
                    this._recomputeHash();
                }
                break;
            case 'set_local':
                // Intentionally ignored. SET LOCAL only applies inside a
                // transaction and the wrapper's cache is bypassed in that
                // window, so it never influences a cacheable response.
                break;
            case 'reset':
                if (isUnsafeGuc(cmd.name) && this._values.delete(cmd.name)) {
                    this._recomputeHash();
                }
                break;
            case 'reset_all':
            case 'discard_all':
                // DISCARD ALL clears every session-scoped setting,
                // including custom unsafe GUCs. Treat as RESET ALL for
                // hash purposes. (DISCARD ALL also drops temp tables,
                // prepared plans, and listen channels — all out of scope
                // for the GUC state hash.) Also resets `_dmlSeq` to 0
                // so a recycled connection rejoins the peer-shareable
                // baseline (otherwise a returned-to-pool conn with a
                // non-zero seq would never collide with a fresh peer's
                // cache slots).
                if (this._values.size > 0 || this._dmlSeq !== 0) {
                    this._values.clear();
                    this._dmlSeq = 0;
                    this._recomputeHash();
                }
                break;
            case 'discard_plans':
            case 'discard_other':
                // PLANS clears the prepared-statement plan cache; we
                // don't maintain one. SEQUENCES / TEMP / TEMPORARY are
                // storage-side. None of these touch GUC state.
                break;
            default:
                break;
        }
        return this._hash !== before;
    }

    // Convenience: parse a SQL string and apply every recognised SET /
    // RESET it contains. Multi-statement bodies (e.g. a single Q wire
    // message containing `SET app.user_id = '42'; SELECT 1`) are split on
    // top-level `;` so callers don't have to. Returns true iff the hash
    // changed.
    //
    // Note (Wave 2): callers that need to defer mutation until after the
    // server confirms the SET should use `parseSql()` + `applyParsed()`
    // instead. `observeSql` is the eager-apply path and remains the right
    // API for direct unit tests of state semantics, but it is no longer
    // called from `wrap.js`'s `CachedClient.query()` hot path.
    observeSql(sql) {
        if (typeof sql !== 'string' || sql.length === 0) return false;
        const before = this._hash;
        for (const cmd of this.parseSql(sql)) {
            this.apply(cmd);
        }
        return this._hash !== before;
    }

    // Parse a SQL string into an array of recognised SET / RESET / DISCARD
    // commands (and `SELECT set_config(...)` calls), WITHOUT applying any
    // of them to `_values` / `_hash`. Multi-statement bodies are split on
    // top-level `;`. Statements that don't match a SET-shape return
    // nothing for that segment (so the array length reflects only
    // recognised mutations — never a 1:1 mapping to the input statements).
    //
    // Used by the wrapper's deferred-mutation flow: parse before the real
    // query runs, then `applyParsed()` only after the server confirms.
    parseSql(sql) {
        if (typeof sql !== 'string' || sql.length === 0) return [];
        // Fast path — most queries are single-statement bodies. Avoid the
        // splitter's allocation when there's no inner `;`.
        const trimmed = sql.trimEnd().replace(/;+$/, '');
        if (!trimmed.includes(';')) {
            const cmd = parseSetCommand(sql);
            return cmd ? [cmd] : [];
        }
        const out = [];
        for (const stmt of splitStatements(sql)) {
            const cmd = parseSetCommand(stmt);
            if (cmd) out.push(cmd);
        }
        return out;
    }

    // Apply an array of pre-parsed commands (from `parseSql`) in order.
    // Each command goes through the same `apply()` switch as the eager
    // path, so safe-GUC filtering, SET LOCAL no-ops, RESET ALL clears,
    // and DISCARD ALL handling are all consistent. Returns true iff the
    // hash changed.
    applyParsed(cmds) {
        if (!Array.isArray(cmds) || cmds.length === 0) return false;
        const before = this._hash;
        for (const cmd of cmds) {
            this.apply(cmd);
        }
        return this._hash !== before;
    }

    // ─── Transaction snapshots ────────────────────────────────────────────
    //
    // Server-side, `SET app.user_id = '42'` issued inside a transaction
    // reverts on ROLLBACK and persists on COMMIT. The wrapper mirrors that
    // by snapshotting state at BEGIN and restoring (or discarding) the
    // snapshot at ROLLBACK / COMMIT. `SET LOCAL` is already a wrapper
    // no-op so it's not in scope here — only bare `SET` mutations made
    // during the open tx need to be reverted.
    //
    // The stack is per-instance; nested BEGINs (which Postgres treats as
    // savepoints in some clients) push additional frames. ROLLBACK pops
    // and restores; COMMIT pops and discards. Empty-stack ROLLBACK is a
    // no-op (there was no tx to revert).

    beginTx() {
        this._txStack.push({
            values: new Map(this._values),
            hash: this._hash,
            dmlSeq: this._dmlSeq,
        });
    }

    commitTx() {
        // Discard the snapshot — current state IS the post-COMMIT state.
        if (this._txStack.length > 0) this._txStack.pop();
    }

    rollbackTx() {
        // Restore from snapshot — undoes any SETs AND any post-DML
        // sequence bumps that ran inside the tx (server-side ROLLBACK
        // reverts both).
        if (this._txStack.length === 0) return;
        const snap = this._txStack.pop();
        this._values = snap.values;
        this._hash = snap.hash;
        this._dmlSeq = snap.dmlSeq;
    }

    _recomputeHash() {
        // Empty values + zero dml_seq is the canonical "fresh
        // connection" state — keep the hash exactly 0 so cache-slot-
        // sharing across "no SETs, no DMLs yet" peers stays intact.
        if (this._values.size === 0 && this._dmlSeq === 0) {
            this._hash = 0;
            return;
        }
        // Sort by name so insertion order doesn't affect the hash. Mirrors
        // the proxy's `BTreeMap` ordering — two states with identical
        // bindings hash identically regardless of SET order on the wire.
        const keys = Array.from(this._values.keys()).sort();
        // Build a single canonical buffer string. \x1f (ASCII unit
        // separator) and \x1e (record separator) keep value boundaries
        // unambiguous so `name=foo, value=bar:` and `name=foo:bar, value=`
        // can never collide. `_dmlSeq` is appended as a record so two
        // states with identical SETs but different seqs hash to
        // different slots.
        let buf = '';
        for (const k of keys) {
            buf += k;
            buf += '\x1f';
            buf += this._values.get(k);
            buf += '\x1e';
        }
        if (this._dmlSeq !== 0) {
            buf += '\x1f';
            buf += String(this._dmlSeq);
            buf += '\x1e';
        }
        this._hash = _fnv1a32(buf);
    }
}

const SQL_KEYWORDS = new Set([
    'select', 'from', 'where', 'and', 'or', 'not', 'in', 'exists',
    'between', 'like', 'is', 'null', 'true', 'false', 'as', 'on',
    'left', 'right', 'inner', 'outer', 'cross', 'full', 'natural',
    'group', 'order', 'having', 'limit', 'offset', 'union', 'intersect',
    'except', 'all', 'distinct', 'lateral', 'values',
]);

function makeKey(sql, values, stateHash = 0) {
    try {
        // State hash is folded in as a hex prefix so two connections with
        // different unsafe-GUC state never collide on the same cache slot.
        // `0` (the empty-state hash) renders as `"0"` and is the default
        // for callers that don't care about GUC tracking — preserves the
        // pre-state-hash key shape's distinguishability across SQL+values
        // and keeps backwards compatibility with existing callers.
        const hashHex = (stateHash >>> 0).toString(16);
        return hashHex + '\0' + sql + '\0' + JSON.stringify(values ?? null);
    } catch {
        return null;
    }
}

function detectWrite(sql) {
    const trimmed = sql.trim();
    const tokens = trimmed.split(/\s+/);
    if (!tokens.length) return null;
    const first = tokens[0].toUpperCase();

    if (first === 'INSERT') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'INTO') return null;
        return bareTable(tokens[2]);
    } else if (first === 'UPDATE') {
        if (tokens.length < 2) return null;
        return bareTable(tokens[1]);
    } else if (first === 'DELETE') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'FROM') return null;
        return bareTable(tokens[2]);
    } else if (first === 'TRUNCATE') {
        if (tokens.length < 2) return null;
        if (tokens[1].toUpperCase() === 'TABLE') {
            if (tokens.length < 3) return null;
            return bareTable(tokens[2]);
        }
        return bareTable(tokens[1]);
    } else if (first === 'CREATE' || first === 'ALTER' || first === 'DROP' || first === 'REFRESH' || first === 'DO' || first === 'CALL') {
        return DDL_SENTINEL;
    } else if (first === 'MERGE') {
        if (tokens.length < 3 || tokens[1].toUpperCase() !== 'INTO') return null;
        return bareTable(tokens[2]);
    } else if (first === 'SELECT') {
        // Re-tokenize from a literal-stripped form so that bare words like
        // `INTO` or `FROM` inside `'...'` / `"..."` don't trigger the
        // SELECT-INTO DDL classifier (e.g. `SELECT 'INSERT INTO orders'
        // FROM audit_log`, `SELECT * FROM "into_table"`).
        const scanTokens = stripStringLiterals(trimmed).split(/\s+/);
        let sawInto = false;
        let intoTarget = null;
        for (let i = 1; i < scanTokens.length; i++) {
            const upper = scanTokens[i].toUpperCase();
            if (upper === 'INTO' && !sawInto) {
                sawInto = true;
                continue;
            }
            if (sawInto && intoTarget === null) {
                if (upper === 'TEMPORARY' || upper === 'TEMP' || upper === 'UNLOGGED') {
                    continue;
                }
                intoTarget = scanTokens[i];
                continue;
            }
            if (sawInto && intoTarget !== null && upper === 'FROM') {
                return DDL_SENTINEL;
            }
            if (upper === 'FROM') {
                return null;
            }
        }
        return null;
    } else if (first === 'COPY') {
        if (tokens.length < 2) return null;
        const raw = tokens[1];
        if (raw.startsWith('(')) return null;
        const tablePart = raw.split('(')[0];
        for (let i = 2; i < tokens.length; i++) {
            const upper = tokens[i].toUpperCase();
            if (upper === 'FROM') return bareTable(tablePart);
            if (upper === 'TO') return null;
        }
        return null;
    } else if (first === 'WITH') {
        const restUpper = trimmed.slice(tokens[0].length).toUpperCase();
        for (const token of restUpper.split(/\s+/)) {
            const word = token.replace(/^\(+/, '');
            if (word === 'INSERT' || word === 'UPDATE' || word === 'DELETE') {
                return DDL_SENTINEL;
            }
        }
        return null;
    }

    return null;
}

function bareTable(raw) {
    let table = raw.split('(')[0];
    const parts = table.split('.');
    table = parts[parts.length - 1];
    return table.toLowerCase();
}

function extractTables(sql) {
    const tables = new Set();
    TABLE_PATTERN.lastIndex = 0;
    let match;
    while ((match = TABLE_PATTERN.exec(sql)) !== null) {
        const table = match[2].toLowerCase();
        if (!SQL_KEYWORDS.has(table)) {
            tables.add(table);
        }
    }
    return tables;
}

let _instance = null;
// Process-level guard so test-driven `_reset()` cycles don't accumulate
// dozens of `exit`/`SIGINT`/`SIGTERM` listeners and trip Node's default
// MaxListeners warning. We install the hooks exactly once per process.
let _exitHooksInstalledGlobal = false;

class NativeCache {
    constructor(opts = {}) {
        if (_instance) {
            // Re-entry against the existing singleton. Only `disabled` is
            // applicable here — other state (capacity, env-driven enable
            // flag, sockets) is fixed at first construction. This lets
            // `GoldLapel({ disableNativeCache: true })` flip the bit on a
            // cache that a `wrap()` call already lazily created.
            if (opts && Object.prototype.hasOwnProperty.call(opts, 'disabled')) {
                _instance._disabled = !!opts.disabled;
            }
            return _instance;
        }
        this._cache = new Map();
        this._tableIndex = new Map();
        this._maxEntries = parseInt(process.env.GOLDLAPEL_NATIVE_CACHE_SIZE || '32768', 10);
        this._enabled = (process.env.GOLDLAPEL_NATIVE_CACHE || 'true').toLowerCase() !== 'false';
        // Explicit native-cache toggle — distinct from `_enabled` (which
        // gates the whole module via env var). When `_disabled` is true
        // the cache acts as a no-op pass-through: get() always misses
        // (and bumps the misses counter so dashboards still see traffic),
        // put() is a no-op, no eviction occurs. Invalidation socket still
        // connects so telemetry continues to flow. Default false; flipped
        // via `goldlapel.start(url, { disableNativeCache: true })`.
        this._disabled = !!opts.disabled;
        this._invalidationConnected = false;
        this._socket = null;
        this._reconnectTimer = null;
        this._reconnectAttempt = 0;
        this._invalidationPort = 0;
        this._buf = '';
        this.statsHits = 0;
        this.statsMisses = 0;
        this.statsInvalidations = 0;
        // Native cache telemetry (2026-05-03). Eviction counter — was missing
        // before; bumped in `_evictOne`. Configurable opt-out: set
        // GOLDLAPEL_REPORT_STATS=false to disable all snapshot replies
        // and state-change emissions (cache continues to function;
        // invalidation listener still runs, only telemetry output is
        // suppressed).
        this.statsEvictions = 0;
        this._reportStats = (
            (process.env.GOLDLAPEL_REPORT_STATS || 'true').toLowerCase() !== 'false'
        );
        // Stable wrapper identity for the lifetime of the process. Lets
        // the proxy aggregate per wrapper across reconnects.
        this._wrapperId = randomUUID();
        this._wrapperLang = 'javascript';
        this._wrapperVersion = _readWrapperVersion();
        // Sliding window for eviction-rate state-change detection. A
        // bounded ring buffer; updates are O(1) amortised. We keep a
        // running sum (`_recentEvictionsSum`) updated on every record
        // so the threshold check is O(1) — the hot path is each put.
        // 1 = evicted on this put, 0 = inserted without eviction.
        this._recentEvictions = [];
        this._recentEvictionsIdx = 0;
        this._recentEvictionsSum = 0;
        // Latched state — only emit a state-change event when the state
        // transitions. Without latching the wrapper would re-emit every
        // put once the rate crossed the threshold.
        this._stateCacheFull = false;
        _instance = this;
    }

    get connected() { return this._invalidationConnected; }
    get enabled() { return this._enabled; }
    get size() { return this._cache.size; }

    get(sql, values, stateHash = 0) {
        if (!this._enabled || !this._invalidationConnected) return null;
        // Native cache disabled: every get is a miss. We still bump the
        // miss counter so the dashboard sees real read traffic flowing
        // through the wrapper; hits stay 0 (no entries are ever
        // stored), evictions stay 0.
        if (this._disabled) {
            this.statsMisses++;
            return null;
        }
        const key = makeKey(sql, values, stateHash);
        if (key === null) return null;
        const entry = this._cache.get(key);
        if (entry !== undefined) {
            // LRU: delete and re-insert to move to end
            this._cache.delete(key);
            this._cache.set(key, entry);
            this.statsHits++;
            return entry;
        }
        this.statsMisses++;
        return null;
    }

    put(sql, values, rows, fields, stateHash = 0) {
        if (!this._enabled || !this._invalidationConnected) return;
        // Native cache disabled: drop the write silently. No eviction,
        // no table index update — the cache stays empty for the
        // lifetime of the process.
        if (this._disabled) return;
        const key = makeKey(sql, values, stateHash);
        if (key === null) return;
        const tables = extractTables(sql);
        let evicted = 0;
        if (this._cache.has(key)) {
            this._cache.delete(key);
        } else if (this._cache.size >= this._maxEntries) {
            this._evictOne();
            evicted = 1;
        }
        this._cache.set(key, { rows, fields, tables });
        for (const table of tables) {
            let keys = this._tableIndex.get(table);
            if (!keys) {
                keys = new Set();
                this._tableIndex.set(table, keys);
            }
            keys.add(key);
        }
        this._recordEviction(evicted);
        // Eviction-rate threshold check. Emits `cache_full` /
        // `cache_recovered` on transition; no-op otherwise. Cheap
        // (constant-time sum over a bounded ring) so it's safe on the
        // hot path.
        this._maybeEmitEvictionRateStateChange();
    }

    invalidateTable(table) {
        table = table.toLowerCase();
        const keys = this._tableIndex.get(table);
        if (!keys) return;
        this._tableIndex.delete(table);
        for (const key of keys) {
            const entry = this._cache.get(key);
            this._cache.delete(key);
            if (entry) {
                for (const otherTable of entry.tables) {
                    if (otherTable !== table) {
                        const otherKeys = this._tableIndex.get(otherTable);
                        if (otherKeys) {
                            otherKeys.delete(key);
                            if (otherKeys.size === 0) this._tableIndex.delete(otherTable);
                        }
                    }
                }
            }
        }
        this.statsInvalidations += keys.size;
    }

    invalidateAll() {
        const count = this._cache.size;
        this._cache.clear();
        this._tableIndex.clear();
        this.statsInvalidations += count;
    }

    connectInvalidation(port) {
        if (this._socket) return;
        this._invalidationPort = port;
        this._reconnectAttempt = 0;
        this._installExitHooks();
        this._tryConnect();
    }

    stopInvalidation() {
        if (this._reconnectTimer) {
            clearTimeout(this._reconnectTimer);
            this._reconnectTimer = null;
        }
        if (this._socket) {
            // Emit `wrapper_disconnected` BEFORE tearing down the socket
            // so the proxy gets a clean farewell line. Best-effort: if
            // the underlying FD is already torn down on the kernel side
            // the write is a no-op (we swallow EPIPE in `_sendLine`).
            // The at-exit hook is the same emit, but if the user calls
            // `gl.stop()` explicitly we want the goodbye to happen now,
            // not at process exit (the socket will already be gone).
            try { this.emitWrapperDisconnected(); } catch {}
            this._socket.destroy();
            this._socket = null;
        }
        this._invalidationConnected = false;
    }

    _tryConnect() {
        const port = this._invalidationPort;
        const sockPath = `/tmp/goldlapel-${port}.sock`;

        let socket;
        if (platform() !== 'win32' && existsSync(sockPath)) {
            socket = createConnection({ path: sockPath });
        } else {
            socket = createConnection({ host: '127.0.0.1', port });
        }

        socket.setEncoding('utf8');

        socket.on('connect', () => {
            this._socket = socket;
            this._invalidationConnected = true;
            this._reconnectAttempt = 0;
            this._buf = '';
            // Announce ourselves so the proxy can register the wrapper
            // identity and its language/version. Stash the socket FIRST
            // (above) so `_emitStateChange` finds it.
            this._emitStateChange('wrapper_connected');
        });

        socket.on('data', (data) => {
            this._buf += data;
            let idx;
            while ((idx = this._buf.indexOf('\n')) !== -1) {
                const line = this._buf.slice(0, idx);
                this._buf = this._buf.slice(idx + 1);
                this._processSignal(line);
            }
        });

        socket.on('close', () => {
            if (this._invalidationConnected) {
                this._invalidationConnected = false;
                this.invalidateAll();
            }
            this._socket = null;
            this._scheduleReconnect();
        });

        socket.on('error', () => {
            // error fires before close — just let close handle cleanup
        });
    }

    _processSignal(line) {
        // Backwards-compat: unknown prefixes are silently ignored. Older
        // proxies sent only `I:` and `P:` (keepalive); newer proxies
        // route additional request types via `?:`. Forward-compat: this
        // dispatcher accepts any well-formed prefix and routes by type.
        if (line.startsWith('I:')) {
            const table = line.slice(2).trim();
            if (table === '*') {
                this.invalidateAll();
            } else {
                this.invalidateTable(table);
            }
        } else if (line.startsWith('?:')) {
            // Snapshot request from the proxy. Reply with R:<json>.
            this._processRequest(line.slice(2));
        }
        // P: keepalive — ignored. Anything else also ignored.
    }

    _evictOne() {
        const oldest = this._cache.keys().next().value;
        if (oldest === undefined) return;
        const entry = this._cache.get(oldest);
        this._cache.delete(oldest);
        if (entry) {
            for (const table of entry.tables) {
                const keys = this._tableIndex.get(table);
                if (keys) {
                    keys.delete(oldest);
                    if (keys.size === 0) this._tableIndex.delete(table);
                }
            }
        }
        this.statsEvictions++;
    }

    // ─── Native cache telemetry: sliding window ───────────────────────────

    _recordEviction(evicted) {
        // Bounded ring — once at capacity, overwrites oldest in O(1).
        // Records the put() outcome (1 evicted, 0 inserted) and keeps
        // the running window sum in sync so the threshold check stays
        // O(1) on the hot path.
        if (this._recentEvictions.length < EVICT_RATE_WINDOW) {
            this._recentEvictions.push(evicted);
            this._recentEvictionsSum += evicted;
        } else {
            const old = this._recentEvictions[this._recentEvictionsIdx];
            this._recentEvictions[this._recentEvictionsIdx] = evicted;
            this._recentEvictionsSum += evicted - old;
            this._recentEvictionsIdx = (this._recentEvictionsIdx + 1) % EVICT_RATE_WINDOW;
        }
    }

    // ─── Native cache telemetry: snapshot + emit ──────────────────────────

    _buildSnapshot() {
        // The proxy computes deltas across snapshots; we just expose the
        // raw counters. Counter reads in JS are atomic (single-threaded),
        // so no lock is needed to keep the snapshot internally
        // consistent (no `await` between reads either).
        const snap = {
            wrapper_id: this._wrapperId,
            lang: this._wrapperLang,
            version: this._wrapperVersion,
            hits: this.statsHits,
            misses: this.statsMisses,
            evictions: this.statsEvictions,
            invalidations: this.statsInvalidations,
            current_size_entries: this._cache.size,
            capacity_entries: this._maxEntries,
        };
        // Surface the explicit-disable bit so the dashboard can render
        // "native cache off" rather than misreading hits=0 as a cold
        // cache. Only emit the field when truthy to keep the
        // on-the-wire snapshot shape identical for the common (enabled)
        // case.
        if (this._disabled) snap.disabled = true;
        return snap;
    }

    _sendLine(line) {
        // Best-effort line write. Socket errors are swallowed: the
        // 'close' / 'error' handlers will rebuild the connection. We
        // never throw from the emit path because emissions happen on
        // the cache hot path (put → cache_full) and synchronously from
        // process exit hooks (wrapper_disconnected).
        //
        // Concurrency: Node is single-threaded for JS execution, so two
        // emits cannot happen "at the same time" — each `socket.write()`
        // call enqueues its bytes atomically into the socket's outgoing
        // stream. No send-lock is needed (unlike the Python wrapper,
        // where the recv thread and the calling thread can race).
        if (!this._reportStats) return;
        const sock = this._socket;
        if (!sock || sock.destroyed) return;
        const data = line.endsWith('\n') ? line : line + '\n';
        try {
            sock.write(data);
        } catch {
            // Connection dead — close handler will rebuild on next
            // event-loop tick. Don't try to repair from here.
        }
    }

    _emitStateChange(state) {
        if (!this._reportStats) return;
        const payload = this._buildSnapshot();
        payload.state = state;
        payload.ts_ms = Date.now();
        let line;
        try {
            line = 'S:' + JSON.stringify(payload);
        } catch {
            return;
        }
        this._sendLine(line);
    }

    _emitResponse(snapshot) {
        if (!this._reportStats) return;
        const payload = snapshot ?? this._buildSnapshot();
        if (payload.ts_ms === undefined) payload.ts_ms = Date.now();
        let line;
        try {
            line = 'R:' + JSON.stringify(payload);
        } catch {
            return;
        }
        this._sendLine(line);
    }

    _maybeEmitEvictionRateStateChange() {
        // Hysteresis-guarded: crossing HIGH emits cache_full, falling
        // back below LOW emits cache_recovered, rates between the two
        // leave the latched state unchanged (no flapping). Need a full
        // window before reporting — a single eviction in 3 puts is
        // noise, not signal.
        const n = this._recentEvictions.length;
        if (n < EVICT_RATE_WINDOW) return;
        const rate = this._recentEvictionsSum / n;
        let emit = null;
        if (!this._stateCacheFull && rate >= EVICT_RATE_HIGH) {
            this._stateCacheFull = true;
            emit = 'cache_full';
        } else if (this._stateCacheFull && rate <= EVICT_RATE_LOW) {
            this._stateCacheFull = false;
            emit = 'cache_recovered';
        }
        if (emit !== null) {
            this._emitStateChange(emit);
        }
    }

    _processRequest(raw) {
        // Today the only request is `snapshot`. Future request types
        // can extend this without breaking older proxies — they'd just
        // ignore unknown R: replies, but only the proxy that sent ?:<x>
        // expects a reply, so the contract is local to the request.
        const body = raw ? raw.trim() : '';
        if (body === '' || body === 'snapshot') {
            this._emitResponse();
        }
    }

    emitWrapperDisconnected() {
        // Called from at-exit hooks before the process tears down.
        // Best-effort; the socket may already be torn down (the proxy
        // will time us out anyway in that case).
        this._emitStateChange('wrapper_disconnected');
    }

    _installExitHooks() {
        // Process-level guard — we only ever install one set of
        // listeners per process. Test cycles that reset the singleton
        // would otherwise accumulate one set per `new NativeCache()`
        // and trip Node's MaxListeners warning around the 10th cycle.
        if (_exitHooksInstalledGlobal) return;
        _exitHooksInstalledGlobal = true;

        // The hooks always re-resolve `_instance` so a reset between
        // install and exit fires the *current* singleton's emit (or
        // a no-op if nothing is wired right now).
        const onExit = () => {
            try { _instance && _instance.emitWrapperDisconnected(); } catch {}
        };

        // SIGINT / SIGTERM: run our emit, then let Node's default
        // disposition exit the process. We only force-exit if no
        // other handler is registered — otherwise we'd hijack the
        // user's intentional signal handling.
        const onSignal = (sig, code) => {
            try { _instance && _instance.emitWrapperDisconnected(); } catch {}
            if (process.listenerCount(sig) <= 1) {
                process.exit(code);
            }
        };

        process.on('exit', onExit);
        process.on('SIGINT', () => onSignal('SIGINT', 130));
        process.on('SIGTERM', () => onSignal('SIGTERM', 143));
    }

    // ─── Reconnect ─────────────────────────────────────────────────────────

    _scheduleReconnect() {
        if (this._reconnectTimer) return;
        const delay = Math.min(2 ** this._reconnectAttempt, 15) * 1000;
        this._reconnectAttempt++;
        this._reconnectTimer = setTimeout(() => {
            this._reconnectTimer = null;
            this._tryConnect();
        }, delay);
        // Don't keep process alive just for reconnection
        if (this._reconnectTimer.unref) this._reconnectTimer.unref();
    }

    static _reset() {
        if (_instance) {
            _instance.stopInvalidation();
            _instance = null;
        }
    }
}

export {
    NativeCache, makeKey, detectWrite, extractTables,
    DDL_SENTINEL, TX_START, TX_END,
    EVICT_RATE_WINDOW, EVICT_RATE_HIGH, EVICT_RATE_LOW,
    // Per-connection unsafe-GUC state-hash machinery (Option Y mirror of
    // proxy's src/guc_state.rs). Used by wrap.js's CachedClient to fold
    // a state hash into the cache key on every query.
    isUnsafeGuc, parseSetCommand, splitStatements, ConnectionGucState,
    KNOWN_SAFE_GUCS, UNSAFE_GUC_SHORT_LIST,
};
