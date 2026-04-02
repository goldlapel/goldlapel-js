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
