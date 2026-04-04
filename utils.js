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
