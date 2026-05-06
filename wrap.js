import {
    NativeCache, detectWrite, DDL_SENTINEL, TX_START, TX_END,
    ConnectionGucState, splitStatements, parseSetCommand,
    isUnsafeGuc,
} from './cache.js';

// Distinguish ROLLBACK (revert) from COMMIT/END (persist) — both match
// TX_END but only ROLLBACK rolls back any SETs that ran inside the tx.
const TX_ROLLBACK = /^\s*ROLLBACK\b/i;

// Postgres response `command` strings that signal a session-state /
// control-flow command rather than a cacheable read. Caching their
// `{rows: [], fields: []}` reply bloats the cache with no-row entries
// that never serve real data and triggers needless eviction pressure.
// Truthy-array check (`result.rows && result.fields`) misses this in JS
// because `[] && []` is truthy.
const NON_CACHEABLE_COMMANDS = new Set([
    'SET', 'RESET', 'DISCARD', 'LISTEN', 'UNLISTEN', 'NOTIFY',
    'BEGIN', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
]);

// Test whether `sql` contains a top-level `SELECT [pg_catalog.]<ident>(...)`
// — i.e. an opaque function call whose body could SET unsafe GUCs without
// the wire layer seeing it. Used to decide whether to schedule an async
// post-call state verify.
//
// Multi-statement bodies are walked: any segment whose top-level shape
// matches triggers verify. `SELECT set_config(...)` is excluded because
// `parseSetCommand` already applied it inline; running a follow-up verify
// would be redundant.
//
// Conservative: false positives (a SELECT-of-function-call that doesn't
// actually mutate state) just trigger one extra cheap pg_settings query,
// which is harmless. False negatives (missing a real SET) are the bug we
// can't tolerate, so the matcher errs toward firing.
function _containsOpaqueFunctionCall(sql) {
    if (typeof sql !== 'string' || sql.length === 0) return false;
    const trimmed = sql.trimEnd().replace(/;+$/, '');
    if (!trimmed.includes(';')) {
        return _segmentIsOpaqueFunctionCall(sql);
    }
    for (const seg of splitStatements(sql)) {
        if (_segmentIsOpaqueFunctionCall(seg)) return true;
    }
    return false;
}

// Per-segment top-level-function-call detector. Returns true for shapes
// like `SELECT foo()`, `SELECT pg_catalog.foo(...)`, `SELECT FOO(args)`,
// excluding `SELECT set_config(...)` which is already handled inline by
// `parseSetCommand`. The matcher checks the leading shape only — anything
// after the function call (e.g. `... FROM tbl`, `... || other`) does NOT
// disqualify it (a function called inside a larger SELECT can still be
// stateful — `SELECT my_setter(), * FROM users` is a common pattern).
const _OPAQUE_FN_RE = /^\s*SELECT\s+(?:pg_catalog\s*\.\s*)?([A-Za-z_][\w$]*)\s*\(/i;
function _segmentIsOpaqueFunctionCall(seg) {
    if (typeof seg !== 'string') return false;
    const m = seg.match(_OPAQUE_FN_RE);
    if (!m) return false;
    const fn = m[1].toLowerCase();
    // set_config() is the function-form SET — already applied inline.
    if (fn === 'set_config') return false;
    return true;
}

// Multi-statement Q-message write detection. Reuses splitStatements so
// `SET app.user_id = '42'; INSERT INTO orders VALUES (1)` (a single Q
// message) doesn't slip past detectWrite's first-token check. Returns:
//   - DDL_SENTINEL  if any segment is DDL (caller invalidateAll's)
//   - Set<string>   of table names if any segments are table-scoped writes
//   - null          if no segment is a write
// Single-statement bodies skip the splitter entirely (hot path).
function detectWritesMulti(sql) {
    if (typeof sql !== 'string' || sql.length === 0) return null;
    // Fast path: no inner `;` → single-statement, original detectWrite.
    const trimmed = sql.trimEnd().replace(/;+$/, '');
    if (!trimmed.includes(';')) {
        const t = detectWrite(sql);
        if (t === null) return null;
        if (t === DDL_SENTINEL) return DDL_SENTINEL;
        return new Set([t]);
    }
    const tables = new Set();
    for (const seg of splitStatements(sql)) {
        const t = detectWrite(seg);
        if (t === DDL_SENTINEL) return DDL_SENTINEL;
        if (t !== null) tables.add(t);
    }
    return tables.size > 0 ? tables : null;
}

// Walk a SQL body and build a list of state-mutation operations to apply
// AFTER the server confirms execution. Each op is one of:
//   { kind: 'begin' }                — push tx snapshot, _inTransaction=true
//   { kind: 'commit' }               — discard snapshot, _inTransaction=false
//   { kind: 'rollback' }             — restore snapshot, _inTransaction=false
//   { kind: 'set',  cmd: parsedSet } — apply parsed SET / RESET / DISCARD
//
// Order matches segment order in the SQL — replayed in order on success
// so a body like `BEGIN; SET app.user_id='42'; COMMIT` snapshots THEN
// applies the SET THEN discards the snapshot, matching the server's
// post-COMMIT state.
//
// `hasTxBoundary` is true iff any segment is BEGIN/COMMIT/ROLLBACK/END —
// the caller uses this to decide whether to bypass the cache read path
// for THIS query (tx-boundary statements are never cacheable). It does
// NOT mutate `_inTransaction` or `_gucState`; the deferred-apply call
// site does that on response confirmation.
//
// Wave 2 (2026-05-05): replaces the eager-mutate `applyTxBoundaries` and
// the eager `observeSql` call. Pre-fix, both ran BEFORE `_real.query`,
// which meant a failed SET (server rejects the statement) left the
// wrapper's hash diverged from server-side state. Now nothing mutates
// until `_real.query` resolves successfully.
function _buildPendingOps(sql) {
    if (typeof sql !== 'string' || sql.length === 0) {
        return { ops: [], hasTxBoundary: false };
    }
    const ops = [];
    let hasTxBoundary = false;
    // Fast path: no inner `;` → single-statement body. Skip the splitter.
    const trimmed = sql.trimEnd().replace(/;+$/, '');
    const segments = trimmed.includes(';') ? splitStatements(sql) : [sql];
    for (const seg of segments) {
        if (TX_START.test(seg)) {
            ops.push({ kind: 'begin' });
            hasTxBoundary = true;
            continue;
        }
        if (TX_END.test(seg)) {
            ops.push({ kind: TX_ROLLBACK.test(seg) ? 'rollback' : 'commit' });
            hasTxBoundary = true;
            continue;
        }
        // Not a tx-boundary segment — try parsing it as a SET / RESET /
        // DISCARD / set_config(). Non-matching segments contribute no op.
        const cmd = parseSetCommand(seg);
        if (cmd) ops.push({ kind: 'set', cmd });
    }
    return { ops, hasTxBoundary };
}

// Replay a pending-ops list on the given CachedClient, mutating
// `_inTransaction` and `_gucState` to reflect the server's confirmed
// state. Called from the success branch of a `_real.query` await.
function _commitPendingOps(client, ops) {
    if (!Array.isArray(ops) || ops.length === 0) return;
    for (const op of ops) {
        switch (op.kind) {
            case 'begin':
                client._gucState.beginTx();
                client._inTransaction = true;
                break;
            case 'commit':
                client._gucState.commitTx();
                client._inTransaction = false;
                break;
            case 'rollback':
                client._gucState.rollbackTx();
                client._inTransaction = false;
                break;
            case 'set':
                client._gucState.apply(op.cmd);
                break;
            default:
                break;
        }
    }
}

let _cache = null;

function _detectInvalidationPort() {
    // Default: proxy port + 2
    return 7934;
}

// Heuristic: is `obj` a pg-pool `Pool` instance? pg-pool's Pool has a
// `connect()` method that returns a Promise<PoolClient>, plus a few
// lifecycle methods (`end`, `query`). We sniff on the constructor name
// (`Pool`) AND duck-type the surface — duplication keeps us safe against
// minified bundles where `constructor.name` is mangled.
//
// Returns false for plain `Client` instances (they have a `connect()` that
// also returns a Promise but no `totalCount` / `idleCount` accessors,
// distinguishing them from pools).
export function _isPgPool(obj) {
    if (!obj || typeof obj !== 'object') return false;
    const ctor = obj.constructor && obj.constructor.name;
    if (ctor === 'Pool' || ctor === 'BoundPool') return true;
    // Duck-type: pg-pool exposes connect(), end(), query() AND has the
    // pool-specific telemetry properties. A plain Client doesn't expose
    // `totalCount` or `idleCount` so this filter excludes it.
    if (
        typeof obj.connect === 'function'
        && typeof obj.end === 'function'
        && typeof obj.query === 'function'
        && (
            typeof obj.totalCount === 'number'
            || typeof obj.idleCount === 'number'
            || typeof obj.waitingCount === 'number'
        )
    ) {
        return true;
    }
    return false;
}

// Wrap a pg-pool `Pool` so every client checked out via `pool.connect()`
// has its `release()` augmented to issue `DISCARD ALL` before returning to
// the pool. Without this, pg-pool reuses physical connections with their
// previous session's GUC state intact — a different user picking up the
// same client would inherit a stale `app.user_id` and the proxy/native
// cache state-hash trick can't help (server-side state diverged from
// what the wrapper observed on the wire).
//
// Return mode is "wrap, don't mutate": we return a Proxy over the pool
// that intercepts `connect()`. The user's pool object is untouched (so
// other code paths that hold a reference to the original Pool don't get
// surprised by mutated methods). The Proxy passes through everything
// else — `pool.query()`, `pool.end()`, telemetry properties, etc.
//
// Note: pg-pool's own `pool.query()` flow internally calls
// `this.connect()`, picks up our hooked client, runs the query, and
// then calls `client.release()` — which now issues DISCARD ALL. So the
// fix covers BOTH user-driven `pool.connect()` and the auto-managed
// `pool.query()` flow.
function _wrapPoolForDiscard(pool) {
    // Pre-bind a hooked connect that always returns DISCARD-on-release
    // clients. Used both by the Proxy's `connect` get-trap AND by the
    // `query` re-implementation below (so pool.query auto-flows go
    // through the same hook).
    const hookedConnect = async (...args) => {
        const client = await pool.connect(...args);
        _hookReleaseToDiscard(client);
        return client;
    };

    return new Proxy(pool, {
        get(target, prop, receiver) {
            const val = Reflect.get(target, prop, receiver);
            if (prop === 'connect' && typeof val === 'function') {
                return hookedConnect;
            }
            if (prop === 'query' && typeof val === 'function') {
                // pg-pool's pool.query internally calls `this.connect()`,
                // but `this` there is the underlying Pool — NOT our
                // Proxy — so the original (unhooked) connect runs. We
                // re-implement query at the Proxy level so the auto-
                // managed flow (connect → query → release) goes through
                // our hooked connect, which guarantees DISCARD on
                // release. Mirrors pg-pool's own query semantics:
                //   - String overload: query(text, values?, callback?)
                //   - Config overload: query(queryConfig, callback?)
                //   - Returns a Promise; if the last arg is a callback,
                //     calls it with (err, result) instead.
                return async function _hookedPoolQuery(...args) {
                    // Detect a trailing callback (pg-pool's query
                    // signature). Pull it out so we can adapt the
                    // Promise to the callback contract.
                    let cb = null;
                    if (args.length > 0 && typeof args[args.length - 1] === 'function') {
                        cb = args.pop();
                    }
                    let client;
                    try {
                        client = await hookedConnect();
                    } catch (err) {
                        if (cb) { cb(err); return; }
                        throw err;
                    }
                    let result;
                    try {
                        result = await client.query(...args);
                    } catch (err) {
                        // Release with err so pool destroys the client
                        // (don't issue DISCARD on a broken client). The
                        // hooked release respects truthy first arg.
                        try { client.release(err); } catch {}
                        if (cb) { cb(err); return; }
                        throw err;
                    }
                    // Successful path: release without err triggers our
                    // DISCARD ALL hook, then returns the client to the
                    // pool. Don't await release errors — they don't
                    // affect the user's result.
                    try { client.release(); } catch {}
                    if (cb) { cb(null, result); return; }
                    return result;
                };
            }
            // Most properties pass through. Methods are bound to the
            // ORIGINAL pool so internal state (the queue of pending
            // connects, the idle-client list, etc.) stays consistent.
            return typeof val === 'function' ? val.bind(target) : val;
        },
    });
}

// In-place hook on a pg-pool `PoolClient`'s `release` method so the
// release issues `DISCARD ALL` first. Mutates the per-checkout client
// object (which is owned by the user for the duration of the checkout
// — we're allowed to add behavior to it).
//
// The hook is idempotent: if the same client object is hooked twice
// (e.g. wrap() called multiple times on the same Pool), the second
// pass detects the marker and skips. This avoids re-wrapping the
// already-wrapped release, which would issue DISCARD twice per
// release.
function _hookReleaseToDiscard(client) {
    if (!client || typeof client.release !== 'function') return;
    if (client.__goldlapelDiscardHooked) return;
    client.__goldlapelDiscardHooked = true;
    const origRelease = client.release.bind(client);
    client.release = function _hookedRelease(...releaseArgs) {
        // If the user passed `release(err)` or `release(true)` we honor
        // it: the client is being destroyed anyway, no DISCARD needed
        // (and it would error on a dying connection). pg-pool's
        // contract: any truthy first arg triggers destroy.
        if (releaseArgs.length > 0 && releaseArgs[0]) {
            return origRelease(...releaseArgs);
        }
        // Best-effort DISCARD ALL. If the underlying connection is
        // already broken (network error, server-initiated close), the
        // query throws — destroy the client instead of returning it to
        // the pool with potentially-stale GUC state. Errors are NOT
        // re-thrown to the user (release shouldn't fail user code).
        let p;
        try {
            p = client.query('DISCARD ALL');
        } catch (err) {
            // Synchronous throw (rare — pg's query is async, but a
            // disposed client may throw on entry). Destroy and bail.
            return origRelease(err);
        }
        if (p && typeof p.then === 'function') {
            return p.then(
                () => origRelease(...releaseArgs),
                (err) => origRelease(err),
            );
        }
        // Defensive: query returned a non-Promise (shouldn't happen with
        // pg, but custom client implementations might). Just release.
        return origRelease(...releaseArgs);
    };
}

export function wrap(client, invalidationPort) {
    if (!_cache) {
        _cache = new NativeCache();
    }
    if (invalidationPort == null) {
        invalidationPort = _detectInvalidationPort();
    }
    if (!_cache._socket) {
        _cache.connectInvalidation(invalidationPort);
    }
    // pg-pool branch: return a Proxy that wraps `pool.connect()` so the
    // returned PoolClients DISCARD on release. The wrapped pool's own
    // surface (query, end, totalCount, ...) flows through untouched.
    // Connection-state safety is handled at release-time, not in any
    // CachedClient — the per-Pool object doesn't have a meaningful
    // single GUC state.
    if (_isPgPool(client)) {
        return _wrapPoolForDiscard(client);
    }
    const cached = new CachedClient(client, _cache);
    return new Proxy(cached, {
        get(target, prop) {
            if (prop in target) {
                const val = target[prop];
                return typeof val === 'function' ? val.bind(target) : val;
            }
            const val = client[prop];
            return typeof val === 'function' ? val.bind(client) : val;
        }
    });
}

class CachedClient {
    constructor(realClient, cache) {
        this._real = realClient;
        this._cache = cache;
        this._inTransaction = false;
        // Per-connection unsafe-GUC state hash. Mirrors the proxy's
        // per-connection `ConnectionGucState` (commit 3e02359). Folded
        // into the native cache key on every read/write so two
        // CachedClients with different unsafe-GUC state never collide on
        // a shared singleton cache (the wrapper has one cache singleton
        // per process; CachedClient is per-conn).
        this._gucState = new ConnectionGucState();
        // Dirty bit — set when we have reason to believe `_gucState`
        // diverges from the actual server-side session state. The next
        // user-visible query will reconcile by querying `pg_settings`
        // and rebuilding `_gucState` from the response. Causes:
        //   1. An async post-call verify (after a `SELECT <fn>(...)`)
        //      failed — connection blip, server in error state, etc.
        //      We can no longer trust the hash.
        //   2. The user manually flipped it (e.g. via `gl.using()`
        //      semantics that hand the conn to another consumer).
        // Cleared by `_runVerify()` on success.
        this._dirty = false;
        // Promise tracking the in-flight async post-call verify. Single-
        // flight: never two verifies queued at once on the same conn.
        // Resolves to `undefined` on success, never rejects (we swallow
        // failures into `_dirty`).
        this._pendingVerify = null;
        // Set to true when the underlying connection is closed via
        // `client.end()`. Suppresses any pending or future verify so
        // we don't try to query a dead connection.
        this._closed = false;
    }

    async query(textOrConfig, values) {
        let sql, params;
        if (typeof textOrConfig === 'object' && textOrConfig !== null) {
            sql = textOrConfig.text;
            params = textOrConfig.values;
        } else {
            sql = textOrConfig;
            params = values;
        }

        // Verify-on-checkout: if a previous async verify failed (or
        // the client was handed off and re-acquired) we can't trust
        // `_gucState`. Reconcile synchronously by querying pg_settings
        // before continuing — this guarantees the cache key derived
        // below reflects real server-side state. Skipped while in tx
        // (cache is bypassed anyway, and verify-during-tx would see
        // uncommitted state).
        if (this._dirty && !this._inTransaction) {
            await this._runVerify();
        }

        // Build the pending state-mutation op list (SET / RESET / DISCARD
        // / set_config + tx snapshots for BEGIN/COMMIT/ROLLBACK). NOTHING
        // is applied yet — we wait for `_real.query` to confirm the
        // server actually executed the body. Pre-Wave-2, this was an
        // eager `observeSql` + `applyTxBoundaries` pair; a failed SET
        // would leave wrapper state diverged from server state. The
        // deferred-apply contract: success → `_commitPendingOps`, error →
        // discard the op list and mark `_dirty=true` so the next query
        // reconciles from `pg_settings`.
        const { ops: pendingOps, hasTxBoundary } = _buildPendingOps(sql);

        // Detect top-level `SELECT <ident>(...)` shape in the original
        // SQL — opaque function calls may SET unsafe GUCs internally
        // (the wire layer can't see the SET). Schedule an async verify
        // AFTER the user's query completes; outcome updates the state
        // map without ever blocking the user's hot path.
        // Single-statement set_config(...) calls were already captured
        // as pendingOps and will commit on success — no follow-up
        // verify needed for those.
        const needsVerify = _containsOpaqueFunctionCall(sql);

        // Multi-statement-aware write detection. Runs BEFORE transaction
        // tracking so a single Q message like `BEGIN; INSERT INTO orders
        // VALUES (1); COMMIT` still surfaces the INSERT for invalidation
        // — TX_START's first-token check would otherwise swallow the
        // whole body as a transaction-boundary marker and the INSERT
        // would never invalidate the `orders` cache slot. Same gap fixed
        // for `SET app.tenant = 'x'; INSERT INTO t ...` (SET hides the
        // INSERT from detectWrite's single-token shape).
        //
        // Invalidation runs eagerly even though the SQL hasn't run yet:
        // a failed write doesn't leave the cache stale (it just makes
        // the next read miss → re-fetch), so eager invalidation is safe.
        const writeTables = detectWritesMulti(sql);
        if (writeTables !== null) {
            if (writeTables === DDL_SENTINEL) {
                this._cache.invalidateAll();
            } else {
                for (const t of writeTables) this._cache.invalidateTable(t);
            }
        }

        // Tx-boundary, write, or already-inside-tx → bypass the cache
        // read path and dispatch straight to the real client. Each path
        // uses `_runRealQuery` which awaits `_real.query` and then either
        // commits the pending ops (success) or discards + marks dirty
        // (error). Tx-boundary bodies (BEGIN/COMMIT/ROLLBACK) are never
        // cacheable; multi-statement bodies that contain writes are also
        // uncacheable (extractTables on the joined SQL would index the
        // wrong slot).
        if (hasTxBoundary || writeTables !== null || this._inTransaction) {
            const r = await this._runRealQuery(textOrConfig, values, pendingOps);
            if (needsVerify) this._scheduleVerify();
            return r;
        }

        // Read path: check native cache, gated on the per-connection
        // unsafe-GUC state hash so user A's RLS-scoped rows can never
        // be served to user B from a shared cache slot.
        const stateHash = this._gucState.stateHash();
        const entry = this._cache.get(sql, params, stateHash);
        if (entry !== null) {
            // Cache hit — no _real.query ran, so no opaque function got
            // a chance to mutate state, and no SET could have been on
            // the wire (cache hits are SELECTs, never SET commands —
            // pendingOps is empty for a cacheable SELECT). No verify or
            // op-commit needed.
            return {
                rows: entry.rows,
                fields: entry.fields,
                rowCount: entry.rows.length,
                command: 'SELECT',
            };
        }

        // Cache miss: execute for real (and commit pending ops on
        // success — though for a pure SELECT pendingOps is empty).
        const result = await this._runRealQuery(textOrConfig, values, pendingOps);

        // Cache the result only if it's a real read response. Same state
        // hash gating as the get() above — the entry is keyed to the
        // connection state at the time of the response.
        //
        // Skip session-state command replies. `SET foo = 'bar'` returns
        // `{rows: [], fields: [], command: 'SET'}` — `[] && []` is truthy
        // in JS so the old check let these through, bloating the cache
        // with no-row entries that never serve real data and triggering
        // needless eviction pressure on session-heavy workloads.
        // Empty-result SELECTs (zero rows from `WHERE id = -1`) are
        // intentionally still cached — the proxy does the same.
        if (
            result.rows && result.fields
            && !NON_CACHEABLE_COMMANDS.has(result.command)
        ) {
            this._cache.put(sql, params, result.rows, result.fields, stateHash);
        }

        if (needsVerify) this._scheduleVerify();
        return result;
    }

    // Dispatch the user's query to the real client and, on success,
    // commit any pending state-mutation ops (SET / RESET / DISCARD / tx
    // boundary). On error: discard the ops AND mark `_dirty=true` so
    // the next user query reconciles `_gucState` from `pg_settings`.
    //
    // The "discard + dirty" combo is conservative-correct: in a
    // multi-statement body like `SET a; SET b; <other>`, when the server
    // hits an error mid-batch, statements 1..N-1 already applied
    // server-side but we can't tell from pg's single rejection which
    // succeeded. Discarding all pendingOps + reconciling on the next
    // query reads the canonical post-batch state out of `pg_settings`
    // and rebuilds `_values` / `_hash` to match.
    //
    // This method ALWAYS re-throws the underlying error so the user's
    // promise rejects exactly as pg would have rejected — wrapper-side
    // bookkeeping is invisible to the caller.
    async _runRealQuery(textOrConfig, values, pendingOps) {
        let result;
        try {
            result = await this._real.query(textOrConfig, values);
        } catch (err) {
            // Server rejected the batch — never trust optimistic state.
            // Mark dirty so the next query reconciles from pg_settings.
            // Skip the dirty mark if the connection is closed (no
            // recovery is possible; the next query won't run anyway).
            if (!this._closed && pendingOps && pendingOps.length > 0) {
                this._dirty = true;
            }
            throw err;
        }
        // Server confirmed — commit the pending ops in order.
        _commitPendingOps(this, pendingOps);
        return result;
    }

    // Schedule an async post-call state verify. Single-flight: if a
    // verify is already in flight, don't queue another — the in-flight
    // one will see whatever state the server is in by the time it
    // runs. Never blocks the caller; never throws (failures land in
    // `_dirty` for the next checkout-time reconcile).
    //
    // We deliberately use `setImmediate` (or a microtask via Promise
    // resolution if setImmediate is unavailable) so the verify queues
    // AFTER the current call's result has been returned to the user.
    // This keeps the user's hot path strictly one network round-trip,
    // even on connections that observe a function call.
    _scheduleVerify() {
        if (this._closed) return;
        if (this._inTransaction) return; // verify-in-tx sees uncommitted state
        if (this._pendingVerify) return; // single-flight
        // Wrap in a Promise that will never reject — failures go into
        // `_dirty`. Use `queueMicrotask` for predictable test ordering;
        // it runs strictly after the current call's resolution and
        // before the next event-loop tick.
        const verifyPromise = new Promise((resolve) => {
            queueMicrotask(() => {
                this._runVerify().finally(resolve);
            });
        });
        this._pendingVerify = verifyPromise;
        verifyPromise.finally(() => {
            // Clear the single-flight token once the verify resolves
            // (success or fail). A subsequent function call will then
            // schedule a fresh verify.
            if (this._pendingVerify === verifyPromise) {
                this._pendingVerify = null;
            }
        });
    }

    // Synchronously reconcile `_gucState` with `pg_settings`. Runs
    // either:
    //   - As the body of a scheduled async verify (after a top-level
    //     SELECT <fn>(...) so we may have missed an internal SET).
    //   - On the user's next query if `_dirty` is set (verify-on-
    //     checkout fallback for the case where an async verify failed
    //     or we couldn't schedule one).
    //
    // Failures (connection broken, server in error state) flip
    // `_dirty=true` so the next user query reconciles again. Never
    // re-throws — the user's flow must not be derailed by our
    // bookkeeping. On success clears `_dirty` and replaces
    // `_gucState`'s contents with the live server-side unsafe-GUC set.
    async _runVerify() {
        if (this._closed) return;
        try {
            const result = await this._real.query(
                "SELECT name, setting FROM pg_settings WHERE source = 'session'"
            );
            const newValues = new Map();
            for (const row of (result && result.rows) || []) {
                // pg returns column values as strings here; defensive
                // string coercion lets us handle adapters that return
                // {name: Buffer} or similar quirks.
                const name = String(row.name || '').toLowerCase();
                if (!name) continue;
                if (isUnsafeGuc(name)) {
                    newValues.set(name, String(row.setting ?? ''));
                }
            }
            // Mutate the existing ConnectionGucState in place so any
            // outside reference (e.g. test introspection on
            // `cached._gucState`) stays current.
            this._gucState._values = newValues;
            this._gucState._recomputeHash();
            this._dirty = false;
        } catch {
            // Anything went wrong — connection blip, server error
            // state, mid-tx unexpected verify. Mark dirty so the next
            // user query retries the reconcile. Never re-throw.
            this._dirty = true;
        }
    }

    async connect() {
        return this._real.connect();
    }

    async end() {
        // Mark closed BEFORE forwarding so any in-flight `_pendingVerify`
        // that hasn't yet fired its query becomes a no-op. Verifies
        // already in flight will throw on the closing connection and
        // fall into the dirty path, which is harmless after end().
        this._closed = true;
        return this._real.end();
    }

    on(event, handler) {
        return this._real.on(event, handler);
    }

    off(event, handler) {
        return this._real.off(event, handler);
    }

    once(event, handler) {
        return this._real.once(event, handler);
    }
}

export { CachedClient, _containsOpaqueFunctionCall };
