function validateIdentifier(name) {
    if (typeof name !== 'string' || name.length === 0) {
        throw new Error('Identifier must be a non-empty string');
    }
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name)) {
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
    await client.query('SELECT pg_notify($1, $2)', [channel, String(message)]);
}

export async function subscribe(client, channel, callback) {
    await client.query(`LISTEN ${channel}`);
    client.on('notification', (msg) => {
        if (msg.channel === channel) {
            callback(msg.channel, msg.payload);
        }
    });
}

export async function enqueue(client, queueTable, payload) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${queueTable} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await client.query(
        `INSERT INTO ${queueTable} (payload) VALUES ($1)`,
        [JSON.stringify(payload)]
    );
}

export async function dequeue(client, queueTable) {
    const result = await client.query(`
        DELETE FROM ${queueTable}
        WHERE id = (
            SELECT id FROM ${queueTable}
            ORDER BY id
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        RETURNING payload
    `);
    if (result.rows.length === 0) return null;
    const val = result.rows[0].payload;
    return typeof val === 'object' ? val : JSON.parse(val);
}

export async function incr(client, table, key, amount = 1) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
            key TEXT PRIMARY KEY,
            value BIGINT NOT NULL DEFAULT 0
        )
    `);
    const result = await client.query(`
        INSERT INTO ${table} (key, value) VALUES ($1, $2)
        ON CONFLICT (key) DO UPDATE SET value = ${table}.value + $3
        RETURNING value
    `, [key, amount, amount]);
    return Number(result.rows[0].value);
}

export async function getCounter(client, table, key) {
    const result = await client.query(
        `SELECT value FROM ${table} WHERE key = $1`, [key]
    );
    if (result.rows.length === 0) return 0;
    return Number(result.rows[0].value);
}

export async function zadd(client, table, member, score) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    `);
    await client.query(`
        INSERT INTO ${table} (member, score) VALUES ($1, $2)
        ON CONFLICT (member) DO UPDATE SET score = EXCLUDED.score
    `, [String(member), Number(score)]);
}

export async function zincrby(client, table, member, amount = 1) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
            member TEXT PRIMARY KEY,
            score DOUBLE PRECISION NOT NULL
        )
    `);
    const result = await client.query(`
        INSERT INTO ${table} (member, score) VALUES ($1, $2)
        ON CONFLICT (member) DO UPDATE SET score = ${table}.score + $3
        RETURNING score
    `, [String(member), Number(amount), Number(amount)]);
    return result.rows[0].score;
}

export async function zrange(client, table, start = 0, stop = 10, desc = true) {
    const order = desc ? 'DESC' : 'ASC';
    const limit = stop - start;
    const result = await client.query(`
        SELECT member, score FROM ${table}
        ORDER BY score ${order}
        LIMIT $1 OFFSET $2
    `, [limit, start]);
    return result.rows.map(row => [row.member, row.score]);
}

export async function zrank(client, table, member, desc = true) {
    const order = desc ? 'DESC' : 'ASC';
    const result = await client.query(`
        SELECT rank FROM (
            SELECT member, ROW_NUMBER() OVER (ORDER BY score ${order}) - 1 AS rank
            FROM ${table}
        ) ranked
        WHERE member = $1
    `, [String(member)]);
    if (result.rows.length === 0) return null;
    return Number(result.rows[0].rank);
}

export async function zscore(client, table, member) {
    const result = await client.query(
        `SELECT score FROM ${table} WHERE member = $1`, [String(member)]
    );
    if (result.rows.length === 0) return null;
    return result.rows[0].score;
}

export async function zrem(client, table, member) {
    const result = await client.query(
        `DELETE FROM ${table} WHERE member = $1`, [String(member)]
    );
    return result.rowCount > 0;
}

export async function geoadd(client, table, nameColumn, geomColumn, name, lon, lat) {
    await client.query('CREATE EXTENSION IF NOT EXISTS postgis');
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
            id BIGSERIAL PRIMARY KEY,
            ${nameColumn} TEXT NOT NULL,
            ${geomColumn} GEOMETRY(Point, 4326) NOT NULL
        )
    `);
    await client.query(`
        INSERT INTO ${table} (${nameColumn}, ${geomColumn})
        VALUES ($1, ST_SetSRID(ST_MakePoint($2, $3), 4326))
    `, [name, lon, lat]);
}

export async function georadius(client, table, geomColumn, lon, lat, radiusMeters, limit = 50) {
    const result = await client.query(`
        SELECT *, ST_Distance(
            ${geomColumn}::geography,
            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
        ) AS distance_m
        FROM ${table}
        WHERE ST_DWithin(
            ${geomColumn}::geography,
            ST_SetSRID(ST_MakePoint($3, $4), 4326)::geography,
            $5
        )
        ORDER BY distance_m
        LIMIT $6
    `, [lon, lat, lon, lat, radiusMeters, limit]);
    return result.rows;
}

export async function geodist(client, table, geomColumn, nameColumn, nameA, nameB) {
    const result = await client.query(`
        SELECT ST_Distance(a.${geomColumn}::geography, b.${geomColumn}::geography)
        FROM ${table} a, ${table} b
        WHERE a.${nameColumn} = $1 AND b.${nameColumn} = $2
    `, [nameA, nameB]);
    if (result.rows.length === 0) return null;
    return result.rows[0].st_distance;
}

export async function hset(client, table, key, field, value) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${table} (
            key TEXT PRIMARY KEY,
            data JSONB NOT NULL DEFAULT '{}'::jsonb
        )
    `);
    await client.query(`
        INSERT INTO ${table} (key, data) VALUES ($1, jsonb_build_object($2, $3::jsonb))
        ON CONFLICT (key) DO UPDATE SET data = ${table}.data || jsonb_build_object($4, $5::jsonb)
    `, [key, field, JSON.stringify(value), field, JSON.stringify(value)]);
}

export async function hget(client, table, key, field) {
    const result = await client.query(
        `SELECT data->>$1 AS val FROM ${table} WHERE key = $2`, [field, key]
    );
    if (result.rows.length === 0) return null;
    const val = result.rows[0].val;
    if (val === null || val === undefined) return null;
    try {
        return JSON.parse(val);
    } catch {
        return val;
    }
}

export async function hgetall(client, table, key) {
    const result = await client.query(
        `SELECT data FROM ${table} WHERE key = $1`, [key]
    );
    if (result.rows.length === 0) return {};
    const val = result.rows[0].data;
    if (!val) return {};
    return typeof val === 'object' ? val : JSON.parse(val);
}

export async function countDistinct(client, table, column) {
    const result = await client.query(
        `SELECT COUNT(DISTINCT ${column}) AS cnt FROM ${table}`
    );
    return Number(result.rows[0].cnt);
}

export async function hdel(client, table, key, field) {
    const result = await client.query(
        `SELECT data ? $1 AS existed FROM ${table} WHERE key = $2`, [field, key]
    );
    if (result.rows.length === 0 || !result.rows[0].existed) return false;
    await client.query(
        `UPDATE ${table} SET data = data - $1 WHERE key = $2`, [field, key]
    );
    return true;
}

export async function streamAdd(client, stream, payload) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${stream} (
            id BIGSERIAL PRIMARY KEY,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    const result = await client.query(
        `INSERT INTO ${stream} (payload) VALUES ($1) RETURNING id`,
        [JSON.stringify(payload)]
    );
    return Number(result.rows[0].id);
}

export async function streamCreateGroup(client, stream, group) {
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${stream}_groups (
            group_name TEXT PRIMARY KEY,
            last_delivered_id BIGINT NOT NULL DEFAULT 0
        )
    `);
    await client.query(`
        CREATE TABLE IF NOT EXISTS ${stream}_pending (
            message_id BIGINT NOT NULL,
            group_name TEXT NOT NULL,
            consumer TEXT NOT NULL,
            claimed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            delivery_count INT NOT NULL DEFAULT 1,
            PRIMARY KEY (group_name, message_id)
        )
    `);
    await client.query(
        `INSERT INTO ${stream}_groups (group_name) VALUES ($1) ON CONFLICT DO NOTHING`,
        [group]
    );
}

export async function streamRead(client, stream, group, consumer, count = 1) {
    const cursorResult = await client.query(
        `SELECT last_delivered_id FROM ${stream}_groups WHERE group_name = $1 FOR UPDATE`,
        [group]
    );
    if (cursorResult.rows.length === 0) return [];
    const lastId = Number(cursorResult.rows[0].last_delivered_id);
    const msgResult = await client.query(
        `SELECT id, payload, created_at FROM ${stream} WHERE id > $1 ORDER BY id LIMIT $2`,
        [lastId, count]
    );
    const messages = msgResult.rows.map(row => ({
        id: Number(row.id),
        payload: typeof row.payload === 'object' ? row.payload : JSON.parse(row.payload),
        created_at: String(row.created_at),
    }));
    if (messages.length > 0) {
        const newLast = messages[messages.length - 1].id;
        await client.query(
            `UPDATE ${stream}_groups SET last_delivered_id = $1 WHERE group_name = $2`,
            [newLast, group]
        );
        for (const msg of messages) {
            await client.query(
                `INSERT INTO ${stream}_pending (message_id, group_name, consumer)
                 VALUES ($1, $2, $3) ON CONFLICT (group_name, message_id) DO NOTHING`,
                [msg.id, group, consumer]
            );
        }
    }
    return messages;
}

export async function streamAck(client, stream, group, messageId) {
    const result = await client.query(
        `DELETE FROM ${stream}_pending WHERE group_name = $1 AND message_id = $2`,
        [group, messageId]
    );
    return result.rowCount > 0;
}

export async function streamClaim(client, stream, group, consumer, minIdleMs = 60000) {
    const claimResult = await client.query(`
        UPDATE ${stream}_pending
        SET consumer = $1, claimed_at = NOW(), delivery_count = delivery_count + 1
        WHERE group_name = $2 AND claimed_at < NOW() - INTERVAL '${Number(minIdleMs)} milliseconds'
        RETURNING message_id
    `, [consumer, group]);
    const claimedIds = claimResult.rows.map(r => Number(r.message_id));
    const messages = [];
    for (const msgId of claimedIds) {
        const result = await client.query(
            `SELECT id, payload, created_at FROM ${stream} WHERE id = $1`,
            [msgId]
        );
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
        `CREATE INDEX IF NOT EXISTS ${name}_tsq_idx ON ${name} USING GIN (tsquery)`
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

async function ensureCollection(client, collection, { unlogged = false } = {}) {
    const prefix = unlogged ? 'CREATE UNLOGGED TABLE' : 'CREATE TABLE';
    await client.query(
        `${prefix} IF NOT EXISTS ${collection} (` +
        `_id UUID PRIMARY KEY DEFAULT gen_random_uuid(), ` +
        `data JSONB NOT NULL, ` +
        `created_at TIMESTAMPTZ DEFAULT NOW())`
    );
}

export async function docCreateCollection(client, collection, { unlogged = false } = {}) {
    validateIdentifier(collection);
    await ensureCollection(client, collection, { unlogged });
}

export async function docInsert(client, collection, document) {
    validateIdentifier(collection);
    await ensureCollection(client, collection);
    const result = await client.query(
        `INSERT INTO ${collection} (data) VALUES ($1::jsonb) RETURNING _id, data, created_at`,
        [JSON.stringify(document)]
    );
    return result.rows[0];
}

export async function docInsertMany(client, collection, documents) {
    validateIdentifier(collection);
    await ensureCollection(client, collection);
    const placeholders = documents.map((_, i) => `($${i + 1}::jsonb)`).join(', ');
    const params = documents.map(d => JSON.stringify(d));
    const result = await client.query(
        `INSERT INTO ${collection} (data) VALUES ${placeholders} RETURNING _id, data, created_at`,
        params
    );
    return result.rows;
}

export async function docFind(client, collection, filter, { sort, limit, skip } = {}) {
    validateIdentifier(collection);
    const { clause, params, nextParam } = buildFilter(filter);
    let paramIdx = nextParam;
    let sql = `SELECT _id, data, created_at FROM ${collection}`;
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

export async function* docFindCursor(client, collection, filter = null, { sort, limit, skip, batchSize = 100 } = {}) {
    validateIdentifier(collection);
    const { clause, params, nextParam } = buildFilter(filter);
    let paramIdx = nextParam;
    let sql = `SELECT _id, data, created_at FROM ${collection}`;
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

export async function docFindOne(client, collection, filter) {
    validateIdentifier(collection);
    const { clause, params } = buildFilter(filter);
    let sql = `SELECT _id, data, created_at FROM ${collection}`;
    if (clause) {
        sql += ` WHERE ${clause}`;
    }
    sql += ' LIMIT 1';
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows[0] || null;
}

export async function docUpdate(client, collection, filter, update) {
    validateIdentifier(collection);
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `UPDATE ${collection} SET data = ${upd.expr} WHERE ${where}`,
        allParams
    );
    return result.rowCount;
}

export async function docUpdateOne(client, collection, filter, update) {
    validateIdentifier(collection);
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${collection} WHERE ${where} LIMIT 1` +
        `) UPDATE ${collection} SET data = ${upd.expr} ` +
        `FROM target WHERE ${collection}._id = target._id`,
        allParams
    );
    return result.rowCount;
}

export async function docDelete(client, collection, filter) {
    validateIdentifier(collection);
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `DELETE FROM ${collection} WHERE ${where}`,
        params
    );
    return result.rowCount;
}

export async function docDeleteOne(client, collection, filter) {
    validateIdentifier(collection);
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${collection} WHERE ${where} LIMIT 1` +
        `) DELETE FROM ${collection} USING target WHERE ${collection}._id = target._id`,
        params
    );
    return result.rowCount;
}

export async function docFindOneAndUpdate(client, collection, filter, update) {
    validateIdentifier(collection);
    const { clause, params: filterParams, nextParam } = buildFilter(filter);
    const upd = buildUpdate(update, nextParam);
    const allParams = [...filterParams, ...upd.params];
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${collection} WHERE ${where} LIMIT 1` +
        `) UPDATE ${collection} SET data = ${upd.expr} ` +
        `FROM target WHERE ${collection}._id = target._id ` +
        `RETURNING ${collection}._id, ${collection}.data, ${collection}.created_at`,
        allParams
    );
    return result.rows[0] || null;
}

export async function docFindOneAndDelete(client, collection, filter) {
    validateIdentifier(collection);
    const { clause, params } = buildFilter(filter);
    const where = clause || 'TRUE';
    const result = await client.query(
        `WITH target AS (` +
        `SELECT _id FROM ${collection} WHERE ${where} LIMIT 1` +
        `) DELETE FROM ${collection} USING target ` +
        `WHERE ${collection}._id = target._id ` +
        `RETURNING ${collection}._id, ${collection}.data, ${collection}.created_at`,
        params
    );
    return result.rows[0] || null;
}

export async function docDistinct(client, collection, field, filter = null) {
    validateIdentifier(collection);
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
    const sql = `SELECT DISTINCT ${fp} FROM ${collection} WHERE ${whereParts.join(' AND ')}`;
    const result = await client.query(sql, params.length > 0 ? params : undefined);
    return result.rows.map(row => Object.values(row)[0]);
}

export async function docCount(client, collection, filter) {
    validateIdentifier(collection);
    const { clause, params } = buildFilter(filter);
    let sql = `SELECT COUNT(*) FROM ${collection}`;
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

export async function docAggregate(client, collection, pipeline) {
    validateIdentifier(collection);

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
            lookupParts.push(
                `COALESCE((SELECT json_agg(b.data) FROM ${spec.from} b WHERE b.data->>'${spec.foreignField}' = ${collection}.data->>'${spec.localField}'), '[]'::json) AS ${spec.as}`
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
    let fromClause = collection;
    if (fromClauses.length > 0) {
        fromClause = `${collection}, ${fromClauses.join(', ')}`;
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

export async function docCreateIndex(client, collection, keys) {
    validateIdentifier(collection);
    if (!keys || Object.keys(keys).length === 0) {
        await client.query(
            `CREATE INDEX IF NOT EXISTS ${collection}_data_gin ON ${collection} USING GIN (data)`
        );
        return;
    }
    for (const [key, dir] of Object.entries(keys)) {
        if (!/^[a-zA-Z_][a-zA-Z0-9_.]*$/.test(key)) {
            throw new Error(`Invalid index key: ${key}`);
        }
        const order = dir === -1 ? 'DESC' : 'ASC';
        const safeName = key.replace(/\./g, '_');
        await client.query(
            `CREATE INDEX IF NOT EXISTS ${collection}_${safeName}_idx ON ${collection} ((data->>'${key}') ${order})`
        );
    }
}

// ─── Change Streams ───────────────────────────────────────────────────────

export async function docWatch(client, collection, callback) {
    validateIdentifier(collection);
    const channel = `${collection}_changes`;
    const funcName = `${collection}_notify_fn`;
    const triggerName = `${collection}_notify_trg`;

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

    await client.query(`
        DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}
    `);

    await client.query(`
        CREATE TRIGGER ${triggerName}
        AFTER INSERT OR UPDATE OR DELETE ON ${collection}
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

export async function docUnwatch(client, collection) {
    validateIdentifier(collection);
    const channel = `${collection}_changes`;
    const funcName = `${collection}_notify_fn`;
    const triggerName = `${collection}_notify_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
    await client.query(`UNLISTEN ${channel}`);
}

// ─── TTL Indexes ──────────────────────────────────────────────────────────

export async function docCreateTtlIndex(client, collection, expireAfterSeconds, { field = 'created_at' } = {}) {
    validateIdentifier(collection);
    validateIdentifier(field);
    if (typeof expireAfterSeconds !== 'number' || expireAfterSeconds <= 0) {
        throw new Error('expireAfterSeconds must be a positive number');
    }

    const idxName = `${collection}_ttl_idx`;
    const funcName = `${collection}_ttl_fn`;
    const triggerName = `${collection}_ttl_trg`;

    await client.query(
        `CREATE INDEX IF NOT EXISTS ${idxName} ON ${collection} (${field})`
    );

    await client.query(`
        CREATE OR REPLACE FUNCTION ${funcName}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM ${collection} WHERE ${field} < NOW() - INTERVAL '${Number(expireAfterSeconds)} seconds';
            RETURN NEW;
        END;
        $$
    `);

    await client.query(`
        DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}
    `);

    await client.query(`
        CREATE TRIGGER ${triggerName}
        BEFORE INSERT ON ${collection}
        FOR EACH ROW EXECUTE FUNCTION ${funcName}()
    `);
}

export async function docRemoveTtlIndex(client, collection) {
    validateIdentifier(collection);

    const idxName = `${collection}_ttl_idx`;
    const funcName = `${collection}_ttl_fn`;
    const triggerName = `${collection}_ttl_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
    await client.query(`DROP INDEX IF EXISTS ${idxName}`);
}

// ─── Capped Collections ──────────────────────────────────────────────────

export async function docCreateCapped(client, collection, maxDocuments) {
    validateIdentifier(collection);
    if (typeof maxDocuments !== 'number' || maxDocuments <= 0) {
        throw new Error('maxDocuments must be a positive number');
    }

    await ensureCollection(client, collection);

    const funcName = `${collection}_cap_fn`;
    const triggerName = `${collection}_cap_trg`;

    await client.query(`
        CREATE OR REPLACE FUNCTION ${funcName}()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$
        BEGIN
            DELETE FROM ${collection} WHERE _id IN (
                SELECT _id FROM ${collection}
                ORDER BY created_at ASC, _id ASC
                LIMIT GREATEST((SELECT COUNT(*) FROM ${collection}) - ${Number(maxDocuments)}, 0)
            );
            RETURN NEW;
        END;
        $$
    `);

    await client.query(`
        DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}
    `);

    await client.query(`
        CREATE TRIGGER ${triggerName}
        AFTER INSERT ON ${collection}
        FOR EACH ROW EXECUTE FUNCTION ${funcName}()
    `);
}

export async function docRemoveCap(client, collection) {
    validateIdentifier(collection);

    const funcName = `${collection}_cap_fn`;
    const triggerName = `${collection}_cap_trg`;

    await client.query(`DROP TRIGGER IF EXISTS ${triggerName} ON ${collection}`);
    await client.query(`DROP FUNCTION IF EXISTS ${funcName}()`);
}
