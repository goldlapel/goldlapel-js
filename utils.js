export function validateIdentifier(name) {
    if (typeof name !== 'string' || name.length === 0) {
        throw new Error('Identifier must be a non-empty string');
    }
    // Bound to 63 chars (Postgres NAMEDATALEN-1) so identifiers match the
    // proxy's server-side regex exactly: `^[A-Za-z_][A-Za-z0-9_]{0,62}$`.
    if (!/^[a-zA-Z_][a-zA-Z0-9_]{0,62}$/.test(name)) {
        throw new Error(`Invalid identifier: ${name}`);
    }
    return name;
}

export async function search(client, table, column, query, { limit = 50, lang = 'english', highlight = false } = {}) {
    validateIdentifier(table);
    const columns = Array.isArray(column) ? column : [column];
    columns.forEach(validateIdentifier);
    const tsvector = columns.map(c => `coalesce(${c}, '')`).join(" || ' ' || ");
    const tsv = `to_tsvector($1, ${tsvector})`;
    const tsq = `plainto_tsquery($2, $3)`;
    const fields = highlight
        ? `*, ts_rank(${tsv}, ${tsq}) AS _score, ts_headline($4, ${tsvector}, ${tsq}, 'StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15') AS _highlight`
        : `*, ts_rank(${tsv}, ${tsq}) AS _score`;
    const params = highlight
        ? [lang, lang, query, lang, limit]
        : [lang, lang, query, limit];
    const limitIdx = highlight ? '$5' : '$4';
    const result = await client.query(
        `SELECT ${fields} FROM ${table} WHERE ${tsv} @@ ${tsq} ORDER BY _score DESC LIMIT ${limitIdx}`,
        params
    );
    return result.rows;
}

export async function searchFuzzy(client, table, column, query, { limit = 50, threshold = 0.3 } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    const result = await client.query(
        `SELECT *, similarity(${column}, $1) AS _score FROM ${table} WHERE similarity(${column}, $1) > $2 ORDER BY _score DESC LIMIT $3`,
        [query, threshold, limit]
    );
    return result.rows;
}

export async function searchPhonetic(client, table, column, query, { limit = 50 } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    const result = await client.query(
        `SELECT *, similarity(${column}, $1) AS _score FROM ${table} WHERE soundex(${column}) = soundex($1) ORDER BY _score DESC, ${column} LIMIT $2`,
        [query, limit]
    );
    return result.rows;
}

export async function similar(client, table, column, vector, { limit = 10 } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    const vectorLiteral = '[' + vector.join(',') + ']';
    const result = await client.query(
        `SELECT *, (${column} <=> $1::vector) AS _score FROM ${table} ORDER BY _score LIMIT $2`,
        [vectorLiteral, limit]
    );
    return result.rows;
}

export async function suggest(client, table, column, prefix, { limit = 10 } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    const result = await client.query(
        `SELECT *, similarity(${column}, $1) AS _score FROM ${table} WHERE ${column} ILIKE $2 ORDER BY _score DESC, ${column} LIMIT $3`,
        [prefix, prefix + '%', limit]
    );
    return result.rows;
}

export async function publish(client, channel, message) {
    validateIdentifier(channel);
    await client.query('SELECT pg_notify($1, $2)', [channel, String(message)]);
}

export async function subscribe(client, channel, callback) {
    validateIdentifier(channel);
    await client.query(`LISTEN ${channel}`);
    client.on('notification', (msg) => {
        if (msg.channel === channel) {
            callback(msg.channel, msg.payload);
        }
    });
}

// ─── Phase 5 Redis-compat families ─────────────────────────────────────────
//
// Counter / zset / hash / queue / geo. The proxy owns DDL (one canonical
// table per (family, name)) and emits query patterns parameterized with
// `$N`. `pg` binds `$N` natively, so the wrapper runs each pattern verbatim
// with no placeholder translation — same SQL across all wrappers.
//
// Flat helper functions accept `{ patterns }` from the namespace classes
// (`gl.counters` / `gl.zsets` / …) and never reach for the dashboard
// directly. Calling these functions without `patterns` is an error — the
// wrapper API never builds DDL itself anymore.

// _familyPattern: pull a query pattern from the proxy's response. Family is
// only used to make the error message helpful when callers forget to pass
// `patterns=` (i.e. they invoked the util directly rather than via the
// namespaced API).
function _familyPattern(patterns, key, family) {
    if (!patterns || !patterns.query_patterns) {
        throw new Error(
            `${family} utils require DDL patterns from the proxy — call via ` +
            `\`gl.${family}s.<verb>(...)\` rather than the utils function directly.`
        );
    }
    const sql = patterns.query_patterns[key];
    if (sql === undefined) {
        throw new Error(
            `${family} pattern '${key}' missing from proxy response — ` +
            `wrapper/proxy version mismatch?`
        );
    }
    return sql;
}

// JSONB column values: psycopg-style drivers may hand back the row value as
// either a decoded object/array/scalar or as the raw JSON text. Normalize
// either shape to the user's actual JS value.
function _decodeJsonb(value) {
    if (value === null || value === undefined) return value;
    if (typeof value !== 'string') return value;
    try {
        return JSON.parse(value);
    } catch {
        return value;
    }
}

// ─── Counter family ────────────────────────────────────────────────────────

export async function counterIncr(client, name, key, amount = 1, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'incr', 'counter');
    const result = await client.query(sql, [String(key), Number(amount)]);
    return Number(result.rows[0].value);
}

// Decrement is `incr` with a negative amount. Provided as a separate method
// so callers don't need to remember the sign convention.
export async function counterDecr(client, name, key, amount = 1, { patterns } = {}) {
    return counterIncr(client, name, key, -Number(amount), { patterns });
}

export async function counterSet(client, name, key, value, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'set', 'counter');
    const result = await client.query(sql, [String(key), Number(value)]);
    return Number(result.rows[0].value);
}

// Returns 0 for unknown keys (matches the Redis convention — no NULL
// surprise on cold cache).
export async function counterGet(client, name, key, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'get', 'counter');
    const result = await client.query(sql, [String(key)]);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].value);
}

// Returns true if a row was deleted, false if the key was already absent.
export async function counterDelete(client, name, key, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'delete', 'counter');
    const result = await client.query(sql, [String(key)]);
    return result.rowCount > 0;
}

export async function counterCountKeys(client, name, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'count_keys', 'counter');
    const result = await client.query(sql);
    if (result.rows.length === 0) return 0;
    // Postgres COUNT(*) returns BIGINT → Node `pg` decodes as a string;
    // cast through Number so callers get a number.
    return Number(result.rows[0].count);
}

// ─── Sorted-set (zset) family ──────────────────────────────────────────────

export async function zsetAdd(client, name, zsetKey, member, score, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zadd', 'zset');
    const result = await client.query(sql, [String(zsetKey), String(member), Number(score)]);
    return Number(result.rows[0].score);
}

export async function zsetIncrBy(client, name, zsetKey, member, delta = 1, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zincrby', 'zset');
    const result = await client.query(sql, [String(zsetKey), String(member), Number(delta)]);
    return Number(result.rows[0].score);
}

// Returns the member's score as a number, or null if absent.
export async function zsetScore(client, name, zsetKey, member, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zscore', 'zset');
    const result = await client.query(sql, [String(zsetKey), String(member)]);
    if (result.rows.length === 0) return null;
    return Number(result.rows[0].score);
}

export async function zsetRemove(client, name, zsetKey, member, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zrem', 'zset');
    const result = await client.query(sql, [String(zsetKey), String(member)]);
    return result.rowCount > 0;
}

// Members by rank within `zsetKey`. Returns an array of `[member, score]`
// tuples. `desc=true` orders highest score first (leaderboard order).
// `start`/`stop` are 0-based inclusive bounds Redis-style; the SQL converts
// to LIMIT/OFFSET.
export async function zsetRange(client, name, zsetKey, start = 0, stop = 10, desc = true, { patterns } = {}) {
    validateIdentifier(name);
    const key = desc ? 'zrange_desc' : 'zrange_asc';
    const sql = _familyPattern(patterns, key, 'zset');
    const limit = Math.max(0, Number(stop) - Number(start) + 1);
    const result = await client.query(sql, [String(zsetKey), limit, Number(start)]);
    return result.rows.map(row => [row.member, Number(row.score)]);
}

// Get members whose score is between min and max (inclusive).
export async function zsetRangeByScore(client, name, zsetKey, minScore, maxScore, limit = 100, offset = 0, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zrangebyscore', 'zset');
    const result = await client.query(sql, [
        String(zsetKey), Number(minScore), Number(maxScore),
        Number(limit), Number(offset),
    ]);
    return result.rows.map(row => [row.member, Number(row.score)]);
}

// 0-based rank within `zsetKey`, or null if member is absent.
export async function zsetRank(client, name, zsetKey, member, { desc = true, patterns } = {}) {
    validateIdentifier(name);
    const key = desc ? 'zrank_desc' : 'zrank_asc';
    const sql = _familyPattern(patterns, key, 'zset');
    const result = await client.query(sql, [String(zsetKey), String(member)]);
    if (result.rows.length === 0) return null;
    return Number(result.rows[0].rank);
}

// Cardinality of one zsetKey namespace.
export async function zsetCard(client, name, zsetKey, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'zcard', 'zset');
    const result = await client.query(sql, [String(zsetKey)]);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].count);
}

// ─── Hash family ───────────────────────────────────────────────────────────

// Set a field's value (single-row UPSERT). Value is JSON-encoded so callers
// can store arbitrary structured payloads.
export async function hashSet(client, name, hashKey, field, value, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hset', 'hash');
    const result = await client.query(sql, [
        String(hashKey), String(field), JSON.stringify(value),
    ]);
    if (result.rows.length === 0) return null;
    return _decodeJsonb(result.rows[0].value);
}

export async function hashGet(client, name, hashKey, field, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hget', 'hash');
    const result = await client.query(sql, [String(hashKey), String(field)]);
    if (result.rows.length === 0) return null;
    return _decodeJsonb(result.rows[0].value);
}

// Reassemble every (field, value) under `hashKey` into a JS object. Empty
// object if the key has no fields.
export async function hashGetAll(client, name, hashKey, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hgetall', 'hash');
    const result = await client.query(sql, [String(hashKey)]);
    const out = {};
    for (const row of result.rows) {
        out[row.field] = _decodeJsonb(row.value);
    }
    return out;
}

export async function hashKeys(client, name, hashKey, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hkeys', 'hash');
    const result = await client.query(sql, [String(hashKey)]);
    return result.rows.map(row => row.field);
}

export async function hashValues(client, name, hashKey, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hvals', 'hash');
    const result = await client.query(sql, [String(hashKey)]);
    return result.rows.map(row => _decodeJsonb(row.value));
}

export async function hashExists(client, name, hashKey, field, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hexists', 'hash');
    const result = await client.query(sql, [String(hashKey), String(field)]);
    if (result.rows.length === 0) return false;
    return Boolean(result.rows[0].exists);
}

export async function hashDelete(client, name, hashKey, field, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hdel', 'hash');
    const result = await client.query(sql, [String(hashKey), String(field)]);
    return result.rowCount > 0;
}

export async function hashLen(client, name, hashKey, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'hlen', 'hash');
    const result = await client.query(sql, [String(hashKey)]);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].count);
}

// ─── Queue family (at-least-once with visibility timeout) ──────────────────

// Add a message; returns its assigned `id`.
export async function queueEnqueue(client, name, payload, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'enqueue', 'queue');
    const result = await client.query(sql, [JSON.stringify(payload)]);
    if (result.rows.length === 0) return null;
    return Number(result.rows[0].id);
}

// Lease the next ready message. Returns `{ id, payload }` or null if the
// queue is empty. Caller MUST `ack(id)` to commit, or `abandon(id)` to
// release the lease immediately. A consumer that crashes leaves the lease
// standing; the message becomes ready again after `visibilityTimeoutMs` and
// is redelivered to the next claim.
export async function queueClaim(client, name, visibilityTimeoutMs = 30000, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'claim', 'queue');
    const result = await client.query(sql, [Number(visibilityTimeoutMs)]);
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
        id: Number(row.id),
        payload: _decodeJsonb(row.payload),
    };
}

// Mark a claimed message done (DELETEs the row). Returns true if the
// message existed and was removed.
export async function queueAck(client, name, messageId, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'ack', 'queue');
    const result = await client.query(sql, [Number(messageId)]);
    return result.rowCount > 0;
}

// Release a claimed message back to ready immediately. Returns true if the
// message existed and was a claim. Equivalent to a NACK in queue parlance —
// the message stays in the queue and is redelivered to the next claim.
export async function queueAbandon(client, name, messageId, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'nack', 'queue');
    const result = await client.query(sql, [Number(messageId)]);
    return result.rows.length > 0;
}

// Extend a claimed message's visibility deadline by `additionalMs`
// milliseconds. Returns the new `visible_at`, or null if the id wasn't a
// claimed message.
//
// Proxy contract: `$1 = id`, `$2 = additional_ms`. Match that order.
export async function queueExtend(client, name, messageId, additionalMs, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'extend', 'queue');
    const result = await client.query(sql, [Number(messageId), Number(additionalMs)]);
    if (result.rows.length === 0) return null;
    return result.rows[0].visible_at;
}

// Look at the next-visible message without claiming it. Returns an object
// with `id`, `payload`, `visible_at`, `status`, `created_at`, or null when
// nothing is ready.
export async function queuePeek(client, name, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'peek', 'queue');
    const result = await client.query(sql);
    if (result.rows.length === 0) return null;
    const row = result.rows[0];
    return {
        id: Number(row.id),
        payload: _decodeJsonb(row.payload),
        visible_at: row.visible_at,
        status: row.status,
        created_at: row.created_at,
    };
}

export async function queueCountReady(client, name, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'count_ready', 'queue');
    const result = await client.query(sql);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].count);
}

export async function queueCountClaimed(client, name, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'count_claimed', 'queue');
    const result = await client.query(sql);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].count);
}

// ─── Geo family (PostGIS GEOGRAPHY-native) ─────────────────────────────────

// Distance unit conversion: proxy returns meters always (GEOGRAPHY default);
// wrappers translate at the edge so callers can ask in km/mi/ft.
const _GEO_UNITS = { m: 1.0, km: 1000.0, mi: 1609.344, ft: 0.3048 };

function _toMeters(value, unit) {
    const factor = _GEO_UNITS[unit];
    if (factor === undefined) {
        throw new Error(`Unknown distance unit: '${unit}' (choose m/km/mi/ft)`);
    }
    return Number(value) * factor;
}

function _convertDistanceMeters(meters, unit) {
    const factor = _GEO_UNITS[unit];
    if (factor === undefined) {
        throw new Error(`Unknown distance unit: '${unit}' (choose m/km/mi/ft)`);
    }
    return Number(meters) / factor;
}

// Set-or-update a member's lon/lat. Idempotent on the member name (PK).
// Returns the just-stored `[lon, lat]` tuple.
export async function geoAdd(client, name, member, lon, lat, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geoadd', 'geo');
    const result = await client.query(sql, [String(member), Number(lon), Number(lat)]);
    if (result.rows.length === 0) return null;
    return [Number(result.rows[0].lon), Number(result.rows[0].lat)];
}

// Fetch a member's `[lon, lat]` tuple, or null if absent.
export async function geoPos(client, name, member, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geopos', 'geo');
    const result = await client.query(sql, [String(member)]);
    if (result.rows.length === 0) return null;
    return [Number(result.rows[0].lon), Number(result.rows[0].lat)];
}

// Distance between two members. `unit` accepts m / km / mi / ft. Returns a
// number in the requested unit, or null if either member is absent.
export async function geoDist(client, name, memberA, memberB, { unit = 'm', patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geodist', 'geo');
    const result = await client.query(sql, [String(memberA), String(memberB)]);
    if (result.rows.length === 0) return null;
    const meters = result.rows[0].distance_m;
    if (meters === null || meters === undefined) return null;
    return _convertDistanceMeters(meters, unit);
}

// Members within `radius` of (lon, lat). Returns an array of objects with
// `member`, `lon`, `lat`, `distance_m`.
//
// Proxy contract (CTE-anchor): `$1=lon`, `$2=lat`, `$3=radius_m`,
// `$4=limit`. Each `$N` appears exactly once in the rendered SQL. `pg`
// binds natively, so we pass a 4-tuple `[lon, lat, radius_m, limit]` — no
// duplicate-expansion needed.
export async function geoRadius(client, name, lon, lat, radius, { unit = 'm', limit = 50, patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'georadius_with_dist', 'geo');
    const radiusM = _toMeters(radius, unit);
    const result = await client.query(sql, [
        Number(lon), Number(lat), Number(radiusM), Number(limit),
    ]);
    return result.rows;
}

// Members within `radius` of `member`'s location.
//
// Proxy contract (NOT a CTE): `$1` and `$2` are both the anchor member
// name; `$3=radius_m`; `$4=limit`. `pg` binds `$N` natively, so the
// `$1`/`$2` pair both reads its value from the array slot for `$N`. We pass
// `[member, member, radius_m, limit]` — the proxy SQL `WHERE a.member=$1 …
// b.member<>$2` resolves both halves without expansion.
export async function geoRadiusByMember(client, name, member, radius, { unit = 'm', limit = 50, patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geosearch_member', 'geo');
    const radiusM = _toMeters(radius, unit);
    const result = await client.query(sql, [
        String(member), String(member), Number(radiusM), Number(limit),
    ]);
    return result.rows;
}

export async function geoRemove(client, name, member, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geo_remove', 'geo');
    const result = await client.query(sql, [String(member)]);
    return result.rowCount > 0;
}

export async function geoCount(client, name, { patterns } = {}) {
    validateIdentifier(name);
    const sql = _familyPattern(patterns, 'geo_count', 'geo');
    const result = await client.query(sql);
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].count);
}

// ─── Misc ──────────────────────────────────────────────────────────────────

export async function countDistinct(client, table, column) {
    validateIdentifier(table);
    validateIdentifier(column);
    const result = await client.query(
        `SELECT COUNT(DISTINCT ${column}) AS cnt FROM ${table}`
    );
    return Number(result.rows[0].cnt);
}

function _requirePatterns(patterns, fn) {
    if (!patterns || !patterns.query_patterns) {
        throw new Error(
            `${fn} requires DDL patterns from the proxy — call via ` +
            `\`gl.${fn}(...)\` rather than the utils function directly.`
        );
    }
    return patterns.query_patterns;
}

export async function streamAdd(client, stream, payload, { patterns } = {}) {
    validateIdentifier(stream);
    const qp = _requirePatterns(patterns, 'streamAdd');
    const result = await client.query(qp.insert, [JSON.stringify(payload)]);
    return Number(result.rows[0].id);
}

export async function streamCreateGroup(client, stream, group, { patterns } = {}) {
    validateIdentifier(stream);
    const qp = _requirePatterns(patterns, 'streamCreateGroup');
    await client.query(qp.create_group, [group]);
}

export async function streamRead(client, stream, group, consumer, count = 1, { patterns } = {}) {
    validateIdentifier(stream);
    const qp = _requirePatterns(patterns, 'streamRead');
    // Wrap in a transaction so the `FOR UPDATE` lock from `group_get_cursor`
    // is held until we've inserted pending rows and advanced the cursor.
    // Under autocommit, the lock is released as soon as the SELECT returns,
    // letting concurrent consumers claim the same messages.
    await client.query('BEGIN');
    try {
        const cursorResult = await client.query(qp.group_get_cursor, [group]);
        if (cursorResult.rows.length === 0) {
            await client.query('COMMIT');
            return [];
        }
        const lastId = Number(cursorResult.rows[0].last_delivered_id);
        const msgResult = await client.query(qp.read_since, [lastId, count]);
        const messages = msgResult.rows.map(row => ({
            id: Number(row.id),
            payload: typeof row.payload === 'object' ? row.payload : JSON.parse(row.payload),
            created_at: String(row.created_at),
        }));
        if (messages.length > 0) {
            const newLast = messages[messages.length - 1].id;
            await client.query(qp.group_advance_cursor, [newLast, group]);
            for (const msg of messages) {
                await client.query(qp.pending_insert, [msg.id, group, consumer]);
            }
        }
        await client.query('COMMIT');
        return messages;
    } catch (err) {
        try { await client.query('ROLLBACK'); } catch { /* swallow rollback errors; surface original */ }
        throw err;
    }
}

export async function streamAck(client, stream, group, messageId, { patterns } = {}) {
    validateIdentifier(stream);
    const qp = _requirePatterns(patterns, 'streamAck');
    const result = await client.query(qp.ack, [group, messageId]);
    return result.rowCount > 0;
}

export async function streamClaim(client, stream, group, consumer, minIdleMs = 60000, { patterns } = {}) {
    validateIdentifier(stream);
    const qp = _requirePatterns(patterns, 'streamClaim');
    const claimResult = await client.query(qp.claim, [consumer, group, minIdleMs]);
    const claimedIds = claimResult.rows.map(r => Number(r.message_id));
    const messages = [];
    for (const msgId of claimedIds) {
        const result = await client.query(qp.read_by_id, [msgId]);
        if (result.rows.length > 0) {
            const row = result.rows[0];
            messages.push({
                id: Number(row.id),
                payload: typeof row.payload === 'object' ? row.payload : JSON.parse(row.payload),
                created_at: String(row.created_at),
            });
        }
    }
    return messages;
}

export async function facets(client, table, column, { limit = 50, query = null, queryColumn = null, lang = 'english' } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    if (query && queryColumn) {
        const columns = Array.isArray(queryColumn) ? queryColumn : [queryColumn];
        columns.forEach(validateIdentifier);
        const tsvector = columns.map(c => `coalesce(${c}, '')`).join(" || ' ' || ");
        const result = await client.query(
            `SELECT ${column} AS value, COUNT(*) AS count FROM ${table} WHERE to_tsvector($1, ${tsvector}) @@ plainto_tsquery($2, $3) GROUP BY ${column} ORDER BY count DESC, ${column} LIMIT $4`,
            [lang, lang, query, limit]
        );
        return result.rows;
    }
    const result = await client.query(
        `SELECT ${column} AS value, COUNT(*) AS count FROM ${table} GROUP BY ${column} ORDER BY count DESC, ${column} LIMIT $1`,
        [limit]
    );
    return result.rows;
}

export async function aggregate(client, table, column, func, { groupBy = null, limit = 50 } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    const allowed = new Set(['count', 'sum', 'avg', 'min', 'max']);
    if (!allowed.has(func)) {
        throw new Error(`func must be one of ${[...allowed].join(', ')}`);
    }
    const aggExpr = func === 'count' ? 'COUNT(*)' : `${func.toUpperCase()}(${column})`;
    if (groupBy) {
        validateIdentifier(groupBy);
        const result = await client.query(
            `SELECT ${groupBy}, ${aggExpr} AS value FROM ${table} GROUP BY ${groupBy} ORDER BY value DESC LIMIT $1`,
            [limit]
        );
        return result.rows;
    }
    const result = await client.query(
        `SELECT ${aggExpr} AS value FROM ${table}`
    );
    return result.rows;
}

export async function createSearchConfig(client, name, { copyFrom = 'english' } = {}) {
    validateIdentifier(name);
    validateIdentifier(copyFrom);
    const check = await client.query(
        'SELECT 1 FROM pg_ts_config WHERE cfgname = $1',
        [name]
    );
    if (check.rows.length === 0) {
        await client.query(`CREATE TEXT SEARCH CONFIGURATION ${name} (COPY = ${copyFrom})`);
    }
}

export async function percolateAdd(client, name, queryId, query, { lang = 'english', metadata = null } = {}) {
    validateIdentifier(name);
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${name} (
            query_id TEXT PRIMARY KEY,
            query_text TEXT NOT NULL,
            tsquery TSQUERY NOT NULL,
            lang TEXT NOT NULL DEFAULT 'english',
            metadata JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await client.query(
        `CREATE INDEX IF NOT EXISTS ${name}_tsq_idx ON ${name} USING GIST (tsquery)`
    );
    await client.query(
        `INSERT INTO ${name} (query_id, query_text, tsquery, lang, metadata)
         VALUES ($1, $2, plainto_tsquery($3, $2), $3, $4)
         ON CONFLICT (query_id) DO UPDATE SET
             query_text = EXCLUDED.query_text,
             tsquery = EXCLUDED.tsquery,
             lang = EXCLUDED.lang,
             metadata = EXCLUDED.metadata`,
        [queryId, query, lang, metadata ? JSON.stringify(metadata) : null]
    );
}

export async function percolate(client, name, text, { lang = 'english', limit = 50 } = {}) {
    validateIdentifier(name);
    const result = await client.query(
        `SELECT query_id, query_text, metadata, ts_rank(to_tsvector($1, $2), tsquery) AS _score
         FROM ${name}
         WHERE to_tsvector($1, $2) @@ tsquery
         ORDER BY _score DESC
         LIMIT $3`,
        [lang, text, limit]
    );
    return result.rows;
}

export async function percolateDelete(client, name, queryId) {
    validateIdentifier(name);
    const result = await client.query(
        `DELETE FROM ${name} WHERE query_id = $1 RETURNING query_id`,
        [queryId]
    );
    return result.rowCount > 0;
}

export async function analyze(client, text, { lang = 'english' } = {}) {
    const result = await client.query(
        'SELECT alias, description, token, dictionaries, dictionary, lexemes FROM ts_debug($1, $2)',
        [lang, text]
    );
    return result.rows;
}

export async function explainScore(client, table, column, query, idColumn, idValue, { lang = 'english' } = {}) {
    validateIdentifier(table);
    validateIdentifier(column);
    validateIdentifier(idColumn);
    const result = await client.query(
        `SELECT
            ${column} AS document_text,
            to_tsvector($1, ${column})::text AS document_tokens,
            plainto_tsquery($1, $2)::text AS query_tokens,
            to_tsvector($1, ${column}) @@ plainto_tsquery($1, $2) AS matches,
            ts_rank(to_tsvector($1, ${column}), plainto_tsquery($1, $2)) AS score,
            ts_headline($1, ${column}, plainto_tsquery($1, $2),
                'StartSel=**, StopSel=**, MaxWords=50, MinWords=20') AS headline
        FROM ${table}
        WHERE ${idColumn} = $3`,
        [lang, query, idValue]
    );
    return result.rows[0] || null;
}

export async function script(client, luaCode, ...args) {
    await client.query('CREATE EXTENSION IF NOT EXISTS pllua');
    const funcName = '_gl_lua_' + Math.random().toString(36).slice(2, 10);
    const n = args.length;
    const params = Array.from({length: n}, (_, i) => `p${i + 1} text`).join(', ');
    await client.query(`
        CREATE OR REPLACE FUNCTION pg_temp.${funcName}(${params})
        RETURNS text LANGUAGE pllua AS $pllua$
        ${luaCode}
        $pllua$
    `);
    const placeholders = Array.from({length: n}, (_, i) => `$${i + 1}`).join(', ');
    const result = await client.query(
        `SELECT pg_temp.${funcName}(${placeholders})`,
        args.map(String)
    );
    return result.rows[0] ? result.rows[0][funcName] : null;
}

// ─── Document Store ────────────────────────────────────────────────────────

const fieldKeyPattern = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;

function expandDotKeys(obj) {
    const result = {};
    for (const [key, value] of Object.entries(obj)) {
        const parts = key.split('.');
        let current = result;
        for (const part of parts.slice(0, -1)) {
            if (!(part in current)) {
                current[part] = {};
            }
            current = current[part];
        }
        current[parts[parts.length - 1]] = value;
    }
    return result;
}

const COMPARISON_OPS = {
    $gt:  '>',
    $gte: '>=',
    $lt:  '<',
    $lte: '<=',
    $eq:  '=',
    $ne:  '!=',
};

function fieldPath(key) {
    const parts = key.split('.');
    if (parts.length === 1) {
        return `data->>'${parts[0]}'`;
    }
    const arrows = parts.slice(0, -1).map(p => `->'${p}'`).join('');
    return `data${arrows}->>'${parts[parts.length - 1]}'`;
}

function fieldPathJson(key) {
    const parts = key.split('.');
    for (const part of parts) {
        if (!fieldKeyPattern.test(part)) {
            throw new Error(`Invalid field key: ${key}`);
        }
    }
    return `data${parts.map(p => `->'${p}'`).join('')}`;
}

function jsonbPath(key) {
    const parts = key.split('.');
    for (const part of parts) {
        if (!fieldKeyPattern.test(part)) {
            throw new Error(`Invalid field key: ${key}`);
        }
    }
    return `{${parts.join(',')}}`;
}

function toJsonbExpr(value, paramIdx) {
    if (typeof value === 'boolean') {
        return { expr: `to_jsonb($${paramIdx}::boolean)`, param: value, nextParam: paramIdx + 1 };
    } else if (typeof value === 'number') {
        return { expr: `to_jsonb($${paramIdx}::numeric)`, param: value, nextParam: paramIdx + 1 };
    } else if (typeof value === 'string') {
        return { expr: `to_jsonb($${paramIdx}::text)`, param: value, nextParam: paramIdx + 1 };
    } else {
        return { expr: `$${paramIdx}::jsonb`, param: JSON.stringify(value), nextParam: paramIdx + 1 };
    }
}

function buildUpdate(update, startParam = 1) {
    if (!update || Object.keys(update).length === 0) {
        return { expr: `data || $${startParam}::jsonb`, params: [JSON.stringify(update || {})], nextParam: startParam + 1 };
    }

    const hasOps = Object.keys(update).some(k => k.startsWith('$'));
    if (!hasOps) {
        return { expr: `data || $${startParam}::jsonb`, params: [JSON.stringify(update)], nextParam: startParam + 1 };
    }

    let expr = 'data';
    const params = [];
    let paramIdx = startParam;

    // $set
    if (update.$set) {
        expr = `(${expr} || $${paramIdx}::jsonb)`;
        params.push(JSON.stringify(update.$set));
        paramIdx++;
    }

    // $unset
    if (update.$unset) {
        for (const field of update.$unset) {
            const parts = field.split('.');
            for (const part of parts) {
                if (!fieldKeyPattern.test(part)) {
                    throw new Error(`Invalid field key: ${field}`);
                }
            }
            if (parts.length === 1) {
                expr = `(${expr} - $${paramIdx})`;
                params.push(field);
                paramIdx++;
            } else {
                const path = `{${parts.join(',')}}`;
                expr = `(${expr} #- $${paramIdx}::text[])`;
                params.push(path);
                paramIdx++;
            }
        }
    }

    // $inc
    if (update.$inc) {
        for (const [field, amount] of Object.entries(update.$inc)) {
            const jp = jsonbPath(field);
            const fp = fieldPath(field);
            expr = `jsonb_set(${expr}, $${paramIdx}::text[], to_jsonb(COALESCE((${fp})::numeric, 0) + $${paramIdx + 1}))`;
            params.push(jp, amount);
            paramIdx += 2;
        }
    }

    // $mul
    if (update.$mul) {
        for (const [field, factor] of Object.entries(update.$mul)) {
            const jp = jsonbPath(field);
            const fp = fieldPath(field);
            expr = `jsonb_set(${expr}, $${paramIdx}::text[], to_jsonb(COALESCE((${fp})::numeric, 0) * $${paramIdx + 1}))`;
            params.push(jp, factor);
            paramIdx += 2;
        }
    }

    // $rename
    if (update.$rename) {
        for (const [oldName, newName] of Object.entries(update.$rename)) {
            for (const part of oldName.split('.')) {
                if (!fieldKeyPattern.test(part)) {
                    throw new Error(`Invalid field key: ${oldName}`);
                }
            }
            for (const part of newName.split('.')) {
                if (!fieldKeyPattern.test(part)) {
                    throw new Error(`Invalid field key: ${newName}`);
                }
            }
            const oldJson = fieldPathJson(oldName);
            const newJp = jsonbPath(newName);
            if (oldName.includes('.')) {
                const oldPath = `{${oldName.split('.').join(',')}}`;
                expr = `jsonb_set((${expr} #- $${paramIdx}::text[]), $${paramIdx + 1}::text[], ${oldJson})`;
                params.push(oldPath, newJp);
                paramIdx += 2;
            } else {
                expr = `jsonb_set((${expr} - $${paramIdx}), $${paramIdx + 1}::text[], ${oldJson})`;
                params.push(oldName, newJp);
                paramIdx += 2;
            }
        }
    }

    // $push
    if (update.$push) {
        for (const [field, value] of Object.entries(update.$push)) {
            const jp = jsonbPath(field);
            const fj = fieldPathJson(field);
            const val = toJsonbExpr(value, paramIdx + 1);
            expr = `jsonb_set(${expr}, $${paramIdx}::text[], COALESCE(${fj}, '[]'::jsonb) || ${val.expr})`;
            params.push(jp, val.param);
            paramIdx = val.nextParam;
        }
    }

    // $pull
    if (update.$pull) {
        for (const [field, value] of Object.entries(update.$pull)) {
            const jp = jsonbPath(field);
            const fj = fieldPathJson(field);
            const val = toJsonbExpr(value, paramIdx + 1);
            expr = `jsonb_set(${expr}, $${paramIdx}::text[], COALESCE((SELECT jsonb_agg(elem) FROM jsonb_array_elements(${fj}) AS elem WHERE elem != ${val.expr}), '[]'::jsonb))`;
            params.push(jp, val.param);
            paramIdx = val.nextParam;
        }
    }

    // $addToSet
    if (update.$addToSet) {
        for (const [field, value] of Object.entries(update.$addToSet)) {
            const jp = jsonbPath(field);
            const fj = fieldPathJson(field);
            const val = toJsonbExpr(value, paramIdx + 1);
            expr = `jsonb_set(${expr}, $${paramIdx}::text[], CASE WHEN COALESCE(${fj}, '[]'::jsonb) @> ${val.expr} THEN ${fj} ELSE COALESCE(${fj}, '[]'::jsonb) || ${val.expr} END)`;
            params.push(jp, val.param);
            // $addToSet uses the value param TWICE (for @> check and for append)
            // but it's the same placeholder in SQL, so we only push it once
            paramIdx = val.nextParam;
        }
    }

    return { expr, params, nextParam: paramIdx };
}

function hasOperators(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value)
        && Object.keys(value).some(k => k.startsWith('$'));
}

function buildFilter(filterDict, startParam = 1) {
    if (!filterDict || Object.keys(filterDict).length === 0) {
        return { clause: '', params: [], nextParam: startParam };
    }

    const plainKeys = {};
    const opClauses = [];
    const params = [];
    let paramIdx = startParam;

    for (const [key, value] of Object.entries(filterDict)) {
        // Logical operators at top level
        if (key === '$or' || key === '$and') {
            if (!Array.isArray(value) || value.length === 0) {
                throw new Error(`${key} value must be a non-empty array`);
            }
            const joiner = key === '$or' ? ' OR ' : ' AND ';
            const subClauses = [];
            for (const subFilter of value) {
                const sub = buildFilter(subFilter, paramIdx);
                if (sub.clause) {
                    subClauses.push(sub.clause);
                    params.push(...sub.params);
                    paramIdx = sub.nextParam;
                }
            }
            if (subClauses.length) opClauses.push('(' + subClauses.join(joiner) + ')');
            continue;
        }

        if (key === '$not') {
            if (value === null || typeof value !== 'object' || Array.isArray(value)) {
                throw new Error('$not value must be a filter object');
            }
            const sub = buildFilter(value, paramIdx);
            if (sub.clause) {
                opClauses.push(`NOT (${sub.clause})`);
                params.push(...sub.params);
                paramIdx = sub.nextParam;
            }
            continue;
        }

        if (key === '$text') {
            if (value === null || typeof value !== 'object' || Array.isArray(value) || !('$search' in value)) {
                throw new Error('$text requires {$search: "query"}');
            }
            const lang = value.$language || 'english';
            opClauses.push(`to_tsvector($${paramIdx}, data::text) @@ plainto_tsquery($${paramIdx + 1}, $${paramIdx + 2})`);
            params.push(lang, lang, value.$search);
            paramIdx += 3;
            continue;
        }

        if (!fieldKeyPattern.test(key)) {
            throw new Error(`Invalid filter key: ${key}`);
        }

        if (!hasOperators(value)) {
            plainKeys[key] = value;
            continue;
        }

        const fp = fieldPath(key);

        for (const [op, opVal] of Object.entries(value)) {
            if (op in COMPARISON_OPS) {
                opClauses.push(`(${fp})::numeric ${COMPARISON_OPS[op]} $${paramIdx}`);
                params.push(opVal);
                paramIdx++;
            } else if (op === '$in') {
                if (!Array.isArray(opVal) || opVal.length === 0) {
                    throw new Error('$in requires a non-empty array');
                }
                const placeholders = opVal.map((_, i) => `$${paramIdx + i}`);
                opClauses.push(`${fp} IN (${placeholders.join(', ')})`);
                for (const item of opVal) {
                    params.push(String(item));
                    paramIdx++;
                }
            } else if (op === '$nin') {
                if (!Array.isArray(opVal) || opVal.length === 0) {
                    throw new Error('$nin requires a non-empty array');
                }
                const placeholders = opVal.map((_, i) => `$${paramIdx + i}`);
                opClauses.push(`${fp} NOT IN (${placeholders.join(', ')})`);
                for (const item of opVal) {
                    params.push(String(item));
                    paramIdx++;
                }
            } else if (op === '$exists') {
                if (opVal) {
                    opClauses.push(`data ? '${key}'`);
                } else {
                    opClauses.push(`NOT (data ? '${key}')`);
                }
            } else if (op === '$regex') {
                opClauses.push(`${fp} ~ $${paramIdx}`);
                params.push(opVal);
                paramIdx++;
            } else if (op === '$not') {
                if (typeof opVal === 'object' && opVal !== null && !Array.isArray(opVal)) {
                    const inner = buildFilter({ [key]: opVal }, paramIdx);
                    if (inner.clause) {
                        opClauses.push(`NOT (${inner.clause})`);
                        params.push(...inner.params);
                        paramIdx = inner.nextParam;
                    }
                } else {
                    throw new Error('$not requires an operator object');
                }
            } else if (op === '$elemMatch') {
                if (opVal === null || typeof opVal !== 'object' || Array.isArray(opVal)) {
                    throw new Error('$elemMatch value must be an object');
                }
                const fj = fieldPathJson(key);
                const elemClauses = [];
                for (const [subOp, subVal] of Object.entries(opVal)) {
                    if (subOp in COMPARISON_OPS) {
                        const sqlOp = COMPARISON_OPS[subOp];
                        if (typeof subVal === 'number') {
                            elemClauses.push(`(elem#>>'{}')::numeric ${sqlOp} $${paramIdx}`);
                        } else {
                            elemClauses.push(`elem#>>'{}' ${sqlOp} $${paramIdx}`);
                        }
                        params.push(subVal);
                        paramIdx++;
                    } else if (subOp === '$regex') {
                        elemClauses.push(`elem#>>'{}' ~ $${paramIdx}`);
                        params.push(subVal);
                        paramIdx++;
                    } else {
                        throw new Error(`Unsupported $elemMatch operator: ${subOp}`);
                    }
                }
                if (elemClauses.length) {
                    opClauses.push(
                        `EXISTS (SELECT 1 FROM jsonb_array_elements(${fj}) AS elem WHERE ${elemClauses.join(' AND ')})`
                    );
                }
            } else if (op === '$text') {
                if (opVal === null || typeof opVal !== 'object' || Array.isArray(opVal) || !('$search' in opVal)) {
                    throw new Error('$text requires {$search: "query"}');
                }
                const lang = opVal.$language || 'english';
                opClauses.push(`to_tsvector($${paramIdx}, ${fp}) @@ plainto_tsquery($${paramIdx + 1}, $${paramIdx + 2})`);
                params.push(lang, lang, opVal.$search);
                paramIdx += 3;
            } else {
                throw new Error(`Unknown operator: ${op}`);
            }
        }
    }

    const allClauses = [];

    if (Object.keys(plainKeys).length > 0) {
        allClauses.push(`data @> $${paramIdx}::jsonb`);
        params.push(JSON.stringify(expandDotKeys(plainKeys)));
        paramIdx++;
    }

    allClauses.push(...opClauses);

    return {
        clause: allClauses.join(' AND '),
        params,
        nextParam: paramIdx,
    };
}

// Resolve the canonical doc-store table name from proxy-fetched patterns.
// Returns the FQ table name (e.g. `_goldlapel.doc_users`). Throws if
// `patterns` is missing — the wrapper API never builds DDL itself anymore;
// `gl.documents.<verb>(...)` always supplies patterns.
function _docTable(patterns, fn) {
    if (!patterns || !patterns.tables || !patterns.tables.main) {
        throw new Error(
            `${fn} requires DDL patterns from the proxy — call via ` +
            `\`gl.documents.${fn.replace(/^doc/, '').replace(/^./, c => c.toLowerCase())}(...)\` ` +
            `rather than the utils function directly.`
        );
    }
    return patterns.tables.main;
}

// Build a deterministic index name from a (possibly schema-qualified) table
// reference. Strips any `schema.` prefix so the index name doesn't contain
// a dot — Postgres rejects those without quoting.
function _docIndexName(table, suffix) {
    const bare = table.includes('.') ? table.slice(table.lastIndexOf('.') + 1) : table;
    return `idx_${bare}_${suffix}`;
}

export async function docCreateCollection(client, collection, { patterns } = {}) {
    validateIdentifier(collection);
    // Calling _patterns on DocumentsAPI already issued the create — nothing
    // left to do on the wrapper side. We accept `patterns` undefined so direct
    // `docCreateCollection(client, "users")` calls fail loud instead of
    // silently doing nothing.
    _docTable(patterns, 'docCreateCollection');
    // No commit: the proxy already executed the DDL on its mgmt connection.
}

export async function docInsert(client, collection, document, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docInsert');
    const result = await client.query(
        `INSERT INTO ${table} (data) VALUES ($1::jsonb) RETURNING _id, data, created_at`,
        [JSON.stringify(document)]
    );
    return result.rows[0];
}

export async function docInsertMany(client, collection, documents, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docInsertMany');
    const placeholders = documents.map((_, i) => `($${i + 1}::jsonb)`).join(', ');
    const params = documents.map(d => JSON.stringify(d));
    const result = await client.query(
        `INSERT INTO ${table} (data) VALUES ${placeholders} RETURNING _id, data, created_at`,
        params
    );
    return result.rows;
}

export async function docFind(client, collection, filter, { sort, limit, skip, patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docFind');
    const { clause, params, nextParam } = buildFilter(filter);
    let paramIdx = nextParam;
    let sql = `SELECT _id, data, created_at FROM ${table}`;
    if (clause) {
        sql += ` WHERE ${clause}`;
    }
    if (sort && Object.keys(sort).length > 0) {
        const sortKeyPattern = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;
        const clauses = Object.entries(sort).map(([key, dir]) => {
            if (!sortKeyPattern.test(key)) {
                throw new Error(`Invalid sort key: ${key}`);
            }
            return `data->>'${key}' ${dir === -1 ? 'DESC' : 'ASC'}`;
        });
        sql += ` ORDER BY ${clauses.join(', ')}`;
    }
    if (limit !== undefined) {
        params.push(limit);
        sql += ` LIMIT $${paramIdx}`;
        paramIdx++;
    }
    if (skip !== undefined) {
        params.push(skip);
        sql += ` OFFSET $${paramIdx}`;
        paramIdx++;
    }
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows;
}

export async function* docFindCursor(client, collection, filter = null, { sort, limit, skip, batchSize = 100, patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docFindCursor');
    const { clause, params, nextParam } = buildFilter(filter);
    let paramIdx = nextParam;
    let sql = `SELECT _id, data, created_at FROM ${table}`;
    if (clause) {
        sql += ` WHERE ${clause}`;
    }
    if (sort && Object.keys(sort).length > 0) {
        const sortKeyPattern = /^[a-zA-Z_][a-zA-Z0-9_.]*$/;
        const clauses = Object.entries(sort).map(([key, dir]) => {
            if (!sortKeyPattern.test(key)) {
                throw new Error(`Invalid sort key: ${key}`);
            }
            return `data->>'${key}' ${dir === -1 ? 'DESC' : 'ASC'}`;
        });
        sql += ` ORDER BY ${clauses.join(', ')}`;
    }
    if (limit !== undefined) {
        params.push(limit);
        sql += ` LIMIT $${paramIdx}`;
        paramIdx++;
    }
    if (skip !== undefined) {
        params.push(skip);
        sql += ` OFFSET $${paramIdx}`;
        paramIdx++;
    }

    const cursorName = `gl_cursor_${Date.now()}`;
    await client.query('BEGIN');
    await client.query(`DECLARE ${cursorName} CURSOR FOR ${sql}`, params.length > 0 ? params : undefined);
    try {
        while (true) {
            const result = await client.query(`FETCH ${batchSize} FROM ${cursorName}`);
            if (result.rows.length === 0) break;
            for (const row of result.rows) yield row;
        }
    } finally {
        await client.query(`CLOSE ${cursorName}`);
        await client.query('COMMIT');
    }
}

export async function docFindOne(client, collection, filter, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docFindOne');
    const { clause, params } = buildFilter(filter);
    let sql = `SELECT _id, data, created_at FROM ${table}`;
    if (clause) {
        sql += ` WHERE ${clause}`;
    }
    sql += ' LIMIT 1';
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows[0] || null;
}

export async function docUpdate(client, collection, filter, update, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docUpdate');
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `UPDATE ${table} SET data = ${upd.expr} WHERE ${where}`,
        allParams
    );
    return result.rowCount;
}

export async function docUpdateOne(client, collection, filter, update, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docUpdateOne');
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${table} WHERE ${where} LIMIT 1` +
        `) UPDATE ${table} SET data = ${upd.expr} ` +
        `FROM target WHERE ${table}._id = target._id`,
        allParams
    );
    return result.rowCount;
}

export async function docDelete(client, collection, filter, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docDelete');
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `DELETE FROM ${table} WHERE ${where}`,
        params
    );
    return result.rowCount;
}

export async function docDeleteOne(client, collection, filter, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docDeleteOne');
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${table} WHERE ${where} LIMIT 1` +
        `) DELETE FROM ${table} USING target WHERE ${table}._id = target._id`,
        params
    );
    return result.rowCount;
}

export async function docFindOneAndUpdate(client, collection, filter, update, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docFindOneAndUpdate');
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${table} WHERE ${where} LIMIT 1` +
        `) UPDATE ${table} SET data = ${upd.expr} ` +
        `FROM target WHERE ${table}._id = target._id ` +
        `RETURNING ${table}._id, ${table}.data, ${table}.created_at`,
        allParams
    );
    return result.rows[0] || null;
}

export async function docFindOneAndDelete(client, collection, filter, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docFindOneAndDelete');
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${table} WHERE ${where} LIMIT 1` +
        `) DELETE FROM ${table} USING target ` +
        `WHERE ${table}._id = target._id ` +
        `RETURNING ${table}._id, ${table}.data, ${table}.created_at`,
        params
    );
    return result.rows[0] || null;
}

export async function docDistinct(client, collection, field, filter = null, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docDistinct');
    for (const part of field.split('.')) {
        if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(part)) {
            throw new Error(`Invalid field key: ${field}`);
        }
    }
    const fp = fieldPath(field);
    const whereParts = [`${fp} IS NOT NULL`];
    const { clause, params } = buildFilter(filter);
    if (clause) {
        whereParts.push(clause);
    }
    const sql = `SELECT DISTINCT ${fp} FROM ${table} WHERE ${whereParts.join(' AND ')}`;
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows.map(row => Object.values(row)[0]);
}

export async function docCount(client, collection, filter, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docCount');
    const { clause, params } = buildFilter(filter);
    let sql = `SELECT COUNT(*) FROM ${table}`;
    if (clause) {
        sql += ` WHERE ${clause}`;
    }
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return parseInt(result.rows[0].count);
}

function resolveFieldRef(ref, unwindMap) {
    const field = ref.startsWith('$') ? ref.slice(1) : ref;
    if (!fieldKeyPattern.test(field)) {
        throw new Error(`Invalid field reference: $${field}`);
    }
    if (unwindMap && unwindMap[field]) {
        return unwindMap[field];
    }
    return `data->>'${field}'`;
}

function buildProject(project, groupAliases) {
    const parts = [];
    const aliases = new Set();

    for (const [key, val] of Object.entries(project)) {
        if (!fieldKeyPattern.test(key)) {
            throw new Error(`Invalid project key: ${key}`);
        }

        if (val === 0) {
            // Exclusion — skip this field
            continue;
        }

        if (val === 1) {
            // Inclusion
            if (groupAliases && groupAliases.has(key)) {
                parts.push(key);
            } else {
                parts.push(`data->>'${key}' AS ${key}`);
            }
            aliases.add(key);
        } else if (typeof val === 'string' && val.startsWith('$')) {
            // Rename: {alias: "$field"}
            const field = val.slice(1);
            if (!fieldKeyPattern.test(field)) {
                throw new Error(`Invalid field reference: ${val}`);
            }
            if (groupAliases && groupAliases.has(field)) {
                parts.push(`${field} AS ${key}`);
            } else {
                parts.push(`data->>'${field}' AS ${key}`);
            }
            aliases.add(key);
        } else {
            throw new Error(`Unsupported $project value for ${key}: ${val}`);
        }
    }

    return { parts, aliases };
}

function buildGroup(group, unwindMap) {
    const selectParts = [];
    const supportedAccumulators = new Set(['$sum', '$avg', '$min', '$max', '$count', '$push', '$addToSet']);
    const aliases = new Set();

    // Handle _id (GROUP BY key)
    let groupBy = null;
    if (group._id !== null && group._id !== undefined) {
        const idRef = group._id;
        if (typeof idRef === 'string' && idRef.startsWith('$')) {
            const resolved = resolveFieldRef(idRef, unwindMap);
            selectParts.push(`${resolved} AS _id`);
            groupBy = resolved;
            aliases.add('_id');
        } else if (typeof idRef === 'object' && !Array.isArray(idRef)) {
            const entries = Object.entries(idRef);
            if (entries.length === 0) {
                throw new Error('Composite $group _id must have at least one field');
            }
            const jsonParts = [];
            const groupByParts = [];
            for (const [alias, ref] of entries) {
                if (!fieldKeyPattern.test(alias)) {
                    throw new Error(`Invalid alias: ${alias}`);
                }
                if (typeof ref !== 'string' || !ref.startsWith('$')) {
                    throw new Error(`Composite _id values must be field references: ${ref}`);
                }
                const resolved = resolveFieldRef(ref, unwindMap);
                jsonParts.push(`'${alias}', ${resolved}`);
                groupByParts.push(resolved);
            }
            selectParts.push(`json_build_object(${jsonParts.join(', ')}) AS _id`);
            groupBy = groupByParts.join(', ');
            aliases.add('_id');
        } else {
            throw new Error(`Unsupported $group _id: ${idRef}`);
        }
    }

    for (const [alias, expr] of Object.entries(group)) {
        if (alias === '_id') continue;
        if (!fieldKeyPattern.test(alias)) {
            throw new Error(`Invalid alias: ${alias}`);
        }

        if (typeof expr !== 'object' || expr === null) {
            throw new Error(`Unsupported accumulator for ${alias}`);
        }

        const accKeys = Object.keys(expr);
        if (accKeys.length !== 1) {
            throw new Error(`Unsupported accumulator for ${alias}`);
        }

        const acc = accKeys[0];
        const val = expr[acc];

        if (!supportedAccumulators.has(acc)) {
            throw new Error(`Unsupported accumulator: ${acc}`);
        }

        aliases.add(alias);

        if (acc === '$count') {
            selectParts.push(`COUNT(*) AS ${alias}`);
        } else if (acc === '$sum') {
            if (val === 1) {
                selectParts.push(`COUNT(*) AS ${alias}`);
            } else if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`SUM((${resolved})::numeric) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $sum value: ${val}`);
            }
        } else if (acc === '$avg') {
            if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`AVG((${resolved})::numeric) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $avg value: ${val}`);
            }
        } else if (acc === '$min') {
            if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`MIN((${resolved})::numeric) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $min value: ${val}`);
            }
        } else if (acc === '$max') {
            if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`MAX((${resolved})::numeric) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $max value: ${val}`);
            }
        } else if (acc === '$push') {
            if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`array_agg(${resolved}) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $push value: ${val}`);
            }
        } else if (acc === '$addToSet') {
            if (typeof val === 'string' && val.startsWith('$')) {
                const resolved = resolveFieldRef(val, unwindMap);
                selectParts.push(`array_agg(DISTINCT ${resolved}) AS ${alias}`);
            } else {
                throw new Error(`Unsupported $addToSet value: ${val}`);
            }
        }
    }

    return { selectParts, groupBy, aliases };
}

// `lookupTables` is a `{userCollectionName: canonicalProxyTable}` map used to
// rewrite `$lookup.from` references to their proxy-canonical FQ tables
// (`_goldlapel.doc_<name>`). Supplied by `gl.documents.aggregate` — direct
// util callers may omit it (in which case `from` is used verbatim).
export async function docAggregate(client, collection, pipeline, { patterns, lookupTables } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docAggregate');

    const params = [];
    let paramIdx = 1;
    let whereClauses = [];
    let selectParts = null;
    let groupBy = null;
    let groupAliases = null;
    let orderClauses = [];
    let limitClause = '';
    let offsetClause = '';
    let hasGroup = false;
    let hasProject = false;
    let projectParts = null;
    const unwindMap = {};
    const fromClauses = [];
    const lookupParts = [];

    const supportedStages = new Set(['$match', '$group', '$sort', '$limit', '$skip', '$project', '$unwind', '$lookup']);

    for (const stage of pipeline) {
        const stageKeys = Object.keys(stage);
        if (stageKeys.length !== 1) {
            throw new Error(`Invalid pipeline stage: ${JSON.stringify(stage)}`);
        }
        const op = stageKeys[0];

        if (!supportedStages.has(op)) {
            throw new Error(`Unsupported pipeline stage: ${op}`);
        }

        if (op === '$match') {
            const bf = buildFilter(stage.$match, paramIdx);
            if (bf.clause) {
                whereClauses.push(bf.clause);
                params.push(...bf.params);
                paramIdx = bf.nextParam;
            }
        } else if (op === '$unwind') {
            const spec = stage.$unwind;
            let field;
            if (typeof spec === 'string') {
                if (!spec.startsWith('$')) {
                    throw new Error(`$unwind path must start with $: ${spec}`);
                }
                field = spec.slice(1);
            } else if (typeof spec === 'object' && spec !== null && spec.path) {
                if (!spec.path.startsWith('$')) {
                    throw new Error(`$unwind path must start with $: ${spec.path}`);
                }
                field = spec.path.slice(1);
            } else {
                throw new Error(`Invalid $unwind spec: ${JSON.stringify(spec)}`);
            }
            if (!fieldKeyPattern.test(field)) {
                throw new Error(`Invalid field reference: $${field}`);
            }
            const alias = `_unwound_${field}`;
            fromClauses.push(`jsonb_array_elements_text(data->'${field}') AS ${alias}`);
            unwindMap[field] = alias;
        } else if (op === '$group') {
            hasGroup = true;
            const result = buildGroup(stage.$group, unwindMap);
            selectParts = result.selectParts;
            groupBy = result.groupBy;
            groupAliases = result.aliases;
        } else if (op === '$project') {
            hasProject = true;
            const result = buildProject(stage.$project, groupAliases);
            projectParts = result.parts;
        } else if (op === '$lookup') {
            const spec = stage.$lookup;
            if (!spec.from || !spec.localField || !spec.foreignField || !spec.as) {
                throw new Error('$lookup requires from, localField, foreignField, and as');
            }
            validateIdentifier(spec.from);
            if (!fieldKeyPattern.test(spec.localField)) {
                throw new Error(`Invalid localField: ${spec.localField}`);
            }
            if (!fieldKeyPattern.test(spec.foreignField)) {
                throw new Error(`Invalid foreignField: ${spec.foreignField}`);
            }
            if (!fieldKeyPattern.test(spec.as)) {
                throw new Error(`Invalid as: ${spec.as}`);
            }
            // Resolve `from` to the canonical proxy table when the caller
            // (gl.documents.aggregate) supplied a lookup map. Without a map,
            // direct util callers are responsible for using fully-qualified
            // names if they want anything other than `public.<from>`.
            const fromTable = (lookupTables && lookupTables[spec.from]) || spec.from;
            lookupParts.push(
                `COALESCE((SELECT json_agg(b.data) FROM ${fromTable} b WHERE b.data->>'${spec.foreignField}' = ${table}.data->>'${spec.localField}'), '[]'::json) AS ${spec.as}`
            );
        } else if (op === '$sort') {
            const sortEntries = Object.entries(stage.$sort);
            for (const [key, dir] of sortEntries) {
                if (!fieldKeyPattern.test(key)) {
                    throw new Error(`Invalid sort key: ${key}`);
                }
                const direction = dir === -1 ? 'DESC' : 'ASC';
                if (hasGroup) {
                    orderClauses.push(`${key} ${direction}`);
                } else {
                    orderClauses.push(`data->>'${key}' ${direction}`);
                }
            }
        } else if (op === '$limit') {
            params.push(stage.$limit);
            limitClause = ` LIMIT $${paramIdx}`;
            paramIdx++;
        } else if (op === '$skip') {
            params.push(stage.$skip);
            offsetClause = ` OFFSET $${paramIdx}`;
            paramIdx++;
        }
    }

    // Determine SELECT clause
    let select;
    if (hasProject && projectParts && projectParts.length > 0) {
        select = projectParts.join(', ');
    } else if (selectParts && selectParts.length > 0) {
        const allParts = [...selectParts, ...lookupParts];
        select = allParts.join(', ');
    } else {
        const baseParts = ['_id', 'data', 'created_at', ...lookupParts];
        select = baseParts.join(', ');
    }

    // Build FROM clause
    let fromClause = table;
    if (fromClauses.length > 0) {
        fromClause = `${table}, ${fromClauses.join(', ')}`;
    }

    let sql = `SELECT ${select} FROM ${fromClause}`;

    if (whereClauses.length > 0) {
        sql += ` WHERE ${whereClauses.join(' AND ')}`;
    }

    if (groupBy) {
        sql += ` GROUP BY ${groupBy}`;
    }

    if (orderClauses.length > 0) {
        sql += ` ORDER BY ${orderClauses.join(', ')}`;
    }

    sql += limitClause;
    sql += offsetClause;

    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows;
}

// Helper: derive a "bare" identifier from a possibly schema-qualified table
// name. Used to name triggers/functions that can't carry a schema dot.
function _bareName(table) {
    return table.includes('.') ? table.slice(table.lastIndexOf('.') + 1) : table;
}

export async function docCreateIndex(client, collection, keys, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docCreateIndex');
    if (!keys || Object.keys(keys).length === 0) {
        const idxName = _docIndexName(table, 'gin');
        await client.query(
            `CREATE INDEX IF NOT EXISTS ${idxName} ON ${table} USING GIN (data)`
        );
        return;
    }
    for (const [key, dir] of Object.entries(keys)) {
        if (!/^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(key)) {
            throw new Error(`Invalid index key: ${key}`);
        }
        const order = dir === -1 ? 'DESC' : 'ASC';
        const safeName = key.replace(/\./g, '_');
        const idxName = _docIndexName(table, `${safeName}_idx`);
        await client.query(
            `CREATE INDEX IF NOT EXISTS ${idxName} ON ${table} ((data->>'${key}') ${order})`
        );
    }
}

// ─── Change Streams ───────────────────────────────────────────────────────

export async function docWatch(client, collection, callback, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docWatch');
    const bare = _bareName(table);
    const channel = `${bare}_changes`;
    const funcName = `${bare}_notify_fn`;
    const triggerName = `${bare}_notify_trg`;

    await client.query(`
        CREATE OR REPLACE FUNCTION ${funcName}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            IF TG_OP = 'DELETE' THEN
                PERFORM pg_notify('${channel}', json_build_object('op', TG_OP, '_id', OLD._id::text)::text);
                RETURN OLD;
            ELSE
                PERFORM pg_notify('${channel}', json_build_object('op', TG_OP, '_id', NEW._id::text, 'data', NEW.data)::text);
                RETURN NEW;
            END IF;
        END;
        $$
    `);

    // CREATE OR REPLACE TRIGGER (Postgres 14+) is atomic — avoids the race
    // where a DROP + CREATE pair could have two concurrent docWatch calls
    // replace each other's triggers mid-flight and end up with a partially
    // dropped one. GL targets PG14+ across the product, so this is safe.
    await client.query(`
        CREATE OR REPLACE TRIGGER ${triggerName}
        AFTER INSERT OR UPDATE OR DELETE ON ${table}
        FOR EACH ROW EXECUTE FUNCTION ${funcName}()
    `);

    await client.query(`LISTEN ${channel}`);

    const listener = (msg) => {
        if (msg.channel === channel) {
            let parsed;
            try { parsed = JSON.parse(msg.payload); } catch { parsed = msg.payload; }
            callback(parsed);
        }
    };
    client.on('notification', listener);

    return {
        stop() {
            client.removeListener('notification', listener);
            client.query(`UNLISTEN ${channel}`).catch(() => {});
        },
    };
}

export async function docUnwatch(client, collection, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docUnwatch');
    const bare = _bareName(table);
    const channel = `${bare}_changes`;
    const funcName = `${bare}_notify_fn`;
    const triggerName = `${bare}_notify_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
    await client.query(`UNLISTEN ${channel}`);
}

// ─── TTL Indexes ──────────────────────────────────────────────────────────

export async function docCreateTtlIndex(client, collection, expireAfterSeconds, { field = 'created_at', patterns } = {}) {
    validateIdentifier(collection);
    validateIdentifier(field);
    if (typeof expireAfterSeconds !== 'number' || expireAfterSeconds <= 0) {
        throw new Error('expireAfterSeconds must be a positive number');
    }
    const table = _docTable(patterns, 'docCreateTtlIndex');
    const bare = _bareName(table);

    const idxName = `${bare}_ttl_idx`;
    const funcName = `${bare}_ttl_fn`;
    const triggerName = `${bare}_ttl_trg`;

    await client.query(
        `CREATE INDEX IF NOT EXISTS ${idxName} ON ${table} (${field})`
    );

    await client.query(`
        CREATE OR REPLACE FUNCTION ${funcName}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM ${table} WHERE ${field} < NOW() - INTERVAL '${Number(expireAfterSeconds)} seconds';
            RETURN NEW;
        END;
        $$
    `);

    // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic, avoids the same
    // race documented in docWatch.
    await client.query(`
        CREATE OR REPLACE TRIGGER ${triggerName}
        BEFORE INSERT ON ${table}
        FOR EACH ROW EXECUTE FUNCTION ${funcName}()
    `);
}

export async function docRemoveTtlIndex(client, collection, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docRemoveTtlIndex');
    const bare = _bareName(table);

    const idxName = `${bare}_ttl_idx`;
    const funcName = `${bare}_ttl_fn`;
    const triggerName = `${bare}_ttl_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
    await client.query(`DROP INDEX IF EXISTS ${idxName}`);
}

// ─── Capped Collections ──────────────────────────────────────────────────

export async function docCreateCapped(client, collection, maxDocuments, { patterns } = {}) {
    validateIdentifier(collection);
    if (typeof maxDocuments !== 'number' || maxDocuments <= 0) {
        throw new Error('maxDocuments must be a positive number');
    }
    const table = _docTable(patterns, 'docCreateCapped');
    const bare = _bareName(table);

    // No ensureCollection — proxy already created the table when the
    // sub-API fetched patterns.

    const funcName = `${bare}_cap_fn`;
    const triggerName = `${bare}_cap_trg`;

    await client.query(`
        CREATE OR REPLACE FUNCTION ${funcName}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM ${table} WHERE _id IN (
                SELECT _id FROM ${table}
                ORDER BY created_at ASC, _id ASC
                LIMIT GREATEST((SELECT COUNT(*) FROM ${table}) - ${Number(maxDocuments)}, 0)
            );
            RETURN NEW;
        END;
        $$
    `);

    // CREATE OR REPLACE TRIGGER (Postgres 14+): atomic, avoids the same
    // race documented in docWatch.
    await client.query(`
        CREATE OR REPLACE TRIGGER ${triggerName}
        AFTER INSERT ON ${table}
        FOR EACH ROW EXECUTE FUNCTION ${funcName}()
    `);
}

export async function docRemoveCap(client, collection, { patterns } = {}) {
    validateIdentifier(collection);
    const table = _docTable(patterns, 'docRemoveCap');
    const bare = _bareName(table);

    const funcName = `${bare}_cap_fn`;
    const triggerName = `${bare}_cap_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${table}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
}
