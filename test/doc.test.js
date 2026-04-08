import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
    docInsert,
    docInsertMany,
    docFind,
    docFindOne,
    docUpdate,
    docUpdateOne,
    docDelete,
    docDeleteOne,
    docFindOneAndUpdate,
    docFindOneAndDelete,
    docDistinct,
    docCount,
    docCreateIndex,
    docAggregate,
    docWatch,
    docUnwatch,
    docCreateTtlIndex,
    docRemoveTtlIndex,
    docCreateCapped,
    docRemoveCap,
} from '../utils.js';

function mockClient(queryResult) {
    const calls = [];
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], rowCount: 0 };
        },
        _calls: calls,
    };
}

// ─── docInsert ─────────────────────────────────────────────────────────────

describe('docInsert', () => {
    it('ensures collection and inserts document', async () => {
        const row = { _id: 'abc-123', data: { name: 'Alice' }, created_at: '2026-01-01' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await docInsert(client, 'users', { name: 'Alice' });
        assert.equal(client._calls.length, 2);
        assert.ok(client._calls[0].text.includes('CREATE TABLE IF NOT EXISTS users'));
        assert.ok(client._calls[0].text.includes('_id UUID PRIMARY KEY'));
        assert.ok(client._calls[0].text.includes('data JSONB NOT NULL'));
        assert.ok(client._calls[0].text.includes('created_at TIMESTAMPTZ'));
        const sql = client._calls[1].text;
        assert.ok(sql.includes('INSERT INTO users'));
        assert.ok(sql.includes('$1::jsonb'));
        assert.ok(sql.includes('RETURNING _id, data, created_at'));
        assert.deepEqual(client._calls[1].values, [JSON.stringify({ name: 'Alice' })]);
        assert.deepEqual(result, row);
    });

    it('returns single row', async () => {
        const row = { _id: 'x', data: { a: 1 }, created_at: 'now' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await docInsert(client, 'items', { a: 1 });
        assert.deepEqual(result, row);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docInsert(client, 'DROP TABLE x; --', { a: 1 }),
            /Invalid identifier/
        );
    });
});

// ─── docInsertMany ─────────────────────────────────────────────────────────

describe('docInsertMany', () => {
    it('ensures collection and batch inserts', async () => {
        const rows = [
            { _id: 'a', data: { x: 1 }, created_at: 'now' },
            { _id: 'b', data: { x: 2 }, created_at: 'now' },
        ];
        const client = mockClient({ rows, rowCount: 2 });
        const result = await docInsertMany(client, 'items', [{ x: 1 }, { x: 2 }]);
        assert.equal(client._calls.length, 2);
        assert.ok(client._calls[0].text.includes('CREATE TABLE IF NOT EXISTS items'));
        const sql = client._calls[1].text;
        assert.ok(sql.includes('INSERT INTO items'));
        assert.ok(sql.includes('($1::jsonb), ($2::jsonb)'));
        assert.ok(sql.includes('RETURNING _id, data, created_at'));
        assert.deepEqual(client._calls[1].values, [
            JSON.stringify({ x: 1 }),
            JSON.stringify({ x: 2 }),
        ]);
        assert.deepEqual(result, rows);
    });

    it('handles single document', async () => {
        const client = mockClient({ rows: [{ _id: 'a' }], rowCount: 1 });
        await docInsertMany(client, 'items', [{ name: 'solo' }]);
        const sql = client._calls[1].text;
        assert.ok(sql.includes('($1::jsonb)'));
        assert.ok(!sql.includes('$2'));
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docInsertMany(client, '1bad', [{ a: 1 }]),
            /Invalid identifier/
        );
    });
});

// ─── docFind ───────────────────────────────────────────────────────────────

describe('docFind', () => {
    it('generates SELECT with filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { active: true });
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM users'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ active: true })]);
    });

    it('generates SELECT without filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM users'));
        assert.ok(!sql.includes('WHERE'));
        assert.equal(client._calls[0].values, undefined);
    });

    it('handles empty filter object', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', {});
        const sql = client._calls[0].text;
        assert.ok(!sql.includes('WHERE'));
    });

    it('applies sort, limit, and skip', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { active: true }, { sort: { name: 1, age: -1 }, limit: 10, skip: 20 });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("ORDER BY data->>'name' ASC, data->>'age' DESC"));
        assert.ok(sql.includes('LIMIT $2'));
        assert.ok(sql.includes('OFFSET $3'));
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ active: true }),
            10,
            20,
        ]);
    });

    it('applies limit and skip without filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', null, { limit: 5, skip: 0 });
        const sql = client._calls[0].text;
        assert.ok(!sql.includes('WHERE'));
        assert.ok(sql.includes('LIMIT $1'));
        assert.ok(sql.includes('OFFSET $2'));
        assert.deepEqual(client._calls[0].values, [5, 0]);
    });

    it('rejects invalid sort key', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', {}, { sort: { 'DROP;--': 1 } }),
            /Invalid sort key/
        );
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'bad table', {}),
            /Invalid identifier/
        );
    });

    it('returns rows', async () => {
        const rows = [{ _id: 'a', data: { name: 'Alice' } }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await docFind(client, 'users', { name: 'Alice' });
        assert.deepEqual(result, rows);
    });
});

// ─── docFindOne ────────────────────────────────────────────────────────────

describe('docFindOne', () => {
    it('generates SELECT with filter and LIMIT 1', async () => {
        const client = mockClient({ rows: [{ _id: 'a', data: { name: 'Bob' } }], rowCount: 1 });
        const result = await docFindOne(client, 'users', { name: 'Bob' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM users'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.ok(sql.includes('LIMIT 1'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ name: 'Bob' })]);
        assert.deepEqual(result, { _id: 'a', data: { name: 'Bob' } });
    });

    it('returns null when no match', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docFindOne(client, 'users', { name: 'Nobody' });
        assert.equal(result, null);
    });

    it('handles empty filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFindOne(client, 'users', {});
        const sql = client._calls[0].text;
        assert.ok(!sql.includes('WHERE'));
        assert.ok(sql.includes('LIMIT 1'));
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFindOne(client, 'bad;table', {}),
            /Invalid identifier/
        );
    });
});

// ─── docUpdate ─────────────────────────────────────────────────────────────

describe('docUpdate', () => {
    it('generates UPDATE with containment filter', async () => {
        const client = mockClient({ rows: [], rowCount: 3 });
        const result = await docUpdate(client, 'users', { active: false }, { active: true });
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('UPDATE users SET data = data || $2::jsonb'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ active: false }),
            JSON.stringify({ active: true }),
        ]);
        assert.equal(result, 3);
    });

    it('returns rowCount', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docUpdate(client, 'users', { x: 1 }, { x: 2 });
        assert.equal(result, 0);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docUpdate(client, '1bad', {}, {}),
            /Invalid identifier/
        );
    });
});

// ─── docUpdateOne ──────────────────────────────────────────────────────────

describe('docUpdateOne', () => {
    it('generates CTE with LIMIT 1', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        const result = await docUpdateOne(client, 'users', { name: 'Alice' }, { age: 30 });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WITH target AS'));
        assert.ok(sql.includes('SELECT _id FROM users WHERE data @> $1::jsonb LIMIT 1'));
        assert.ok(sql.includes('UPDATE users SET data = data || $2::jsonb'));
        assert.ok(sql.includes('FROM target WHERE users._id = target._id'));
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ name: 'Alice' }),
            JSON.stringify({ age: 30 }),
        ]);
        assert.equal(result, 1);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docUpdateOne(client, 'DROP; --', {}, {}),
            /Invalid identifier/
        );
    });
});

// ─── docDelete ─────────────────────────────────────────────────────────────

describe('docDelete', () => {
    it('generates DELETE with containment filter', async () => {
        const client = mockClient({ rows: [], rowCount: 5 });
        const result = await docDelete(client, 'users', { archived: true });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('DELETE FROM users'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ archived: true })]);
        assert.equal(result, 5);
    });

    it('returns rowCount', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docDelete(client, 'users', { x: 1 });
        assert.equal(result, 0);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docDelete(client, 'bad table', {}),
            /Invalid identifier/
        );
    });
});

// ─── docDeleteOne ──────────────────────────────────────────────────────────

describe('docDeleteOne', () => {
    it('generates CTE with LIMIT 1', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        const result = await docDeleteOne(client, 'users', { name: 'Alice' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WITH target AS'));
        assert.ok(sql.includes('SELECT _id FROM users WHERE data @> $1::jsonb LIMIT 1'));
        assert.ok(sql.includes('DELETE FROM users USING target'));
        assert.ok(sql.includes('WHERE users._id = target._id'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ name: 'Alice' })]);
        assert.equal(result, 1);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docDeleteOne(client, '1no', {}),
            /Invalid identifier/
        );
    });
});

// ─── docCount ──────────────────────────────────────────────────────────────

describe('docCount', () => {
    it('generates COUNT with filter', async () => {
        const client = mockClient({ rows: [{ count: '42' }], rowCount: 1 });
        const result = await docCount(client, 'users', { active: true });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT COUNT(*) FROM users'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ active: true })]);
        assert.equal(result, 42);
    });

    it('generates COUNT without filter', async () => {
        const client = mockClient({ rows: [{ count: '100' }], rowCount: 1 });
        const result = await docCount(client, 'users');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT COUNT(*) FROM users'));
        assert.ok(!sql.includes('WHERE'));
        assert.equal(client._calls[0].values, undefined);
        assert.equal(result, 100);
    });

    it('handles empty filter object', async () => {
        const client = mockClient({ rows: [{ count: '0' }], rowCount: 1 });
        await docCount(client, 'users', {});
        const sql = client._calls[0].text;
        assert.ok(!sql.includes('WHERE'));
    });

    it('returns parsed integer', async () => {
        const client = mockClient({ rows: [{ count: '999' }], rowCount: 1 });
        const result = await docCount(client, 'items');
        assert.equal(typeof result, 'number');
        assert.equal(result, 999);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docCount(client, 'bad;name', {}),
            /Invalid identifier/
        );
    });
});

// ─── docCreateIndex ────────────────────────────────────────────────────────

describe('docCreateIndex', () => {
    it('creates GIN index when no keys provided', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docCreateIndex(client, 'users');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('CREATE INDEX IF NOT EXISTS users_data_gin'));
        assert.ok(sql.includes('ON users USING GIN (data)'));
    });

    it('creates GIN index for empty keys object', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docCreateIndex(client, 'users', {});
        assert.equal(client._calls.length, 1);
        assert.ok(client._calls[0].text.includes('USING GIN'));
    });

    it('creates B-tree expression indexes per key', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docCreateIndex(client, 'users', { name: 1, age: -1 });
        assert.equal(client._calls.length, 2);
        const sql0 = client._calls[0].text;
        assert.ok(sql0.includes('CREATE INDEX IF NOT EXISTS users_name_idx'));
        assert.ok(sql0.includes("(data->>'name') ASC"));
        const sql1 = client._calls[1].text;
        assert.ok(sql1.includes('CREATE INDEX IF NOT EXISTS users_age_idx'));
        assert.ok(sql1.includes("(data->>'age') DESC"));
    });

    it('handles dotted key names in index name', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docCreateIndex(client, 'events', { 'user.email': 1 });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('users_email_idx') || sql.includes('events_user_email_idx'));
        assert.ok(sql.includes("(data->>'user.email') ASC"));
    });

    it('rejects invalid index key', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docCreateIndex(client, 'users', { 'DROP;--': 1 }),
            /Invalid index key/
        );
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docCreateIndex(client, 'bad table'),
            /Invalid identifier/
        );
    });

    it('returns void (undefined)', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docCreateIndex(client, 'users');
        assert.equal(result, undefined);
    });
});

// ─── docAggregate ─────────────────────────────────────────────────────────

describe('docAggregate', () => {
    it('full pipeline: $match + $group + $sort + $limit', async () => {
        const rows = [{ _id: 'electronics', total: 500 }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await docAggregate(client, 'orders', [
            { $match: { status: 'shipped' } },
            { $group: { _id: '$category', total: { $sum: '$amount' } } },
            { $sort: { total: -1 } },
            { $limit: 10 },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("SELECT data->>'category' AS _id, SUM((data->>'amount')::numeric) AS total FROM orders"));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.ok(sql.includes("GROUP BY data->>'category'"));
        assert.ok(sql.includes('ORDER BY total DESC'));
        assert.ok(sql.includes('LIMIT $2'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ status: 'shipped' }), 10]);
        assert.deepEqual(result, rows);
    });

    it('$group with $avg uses numeric cast', async () => {
        const client = mockClient({ rows: [{ _id: 'A', avg_price: 25.5 }], rowCount: 1 });
        await docAggregate(client, 'products', [
            { $group: { _id: '$brand', avg_price: { $avg: '$price' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("AVG((data->>'price')::numeric) AS avg_price"));
        assert.ok(sql.includes("GROUP BY data->>'brand'"));
    });

    it('$group with null _id produces no GROUP BY', async () => {
        const client = mockClient({ rows: [{ count: 42 }], rowCount: 1 });
        await docAggregate(client, 'events', [
            { $group: { _id: null, count: { $sum: 1 } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('COUNT(*) AS count'));
        assert.ok(!sql.includes('GROUP BY'));
    });

    it('$match only behaves like find', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'users', [
            { $match: { active: true } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM users'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ active: true })]);
    });

    it('$sort after $group uses aliases', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'sales', [
            { $group: { _id: '$region', revenue: { $sum: '$amount' } } },
            { $sort: { revenue: -1 } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('ORDER BY revenue DESC'));
    });

    it('$sort without $group uses data->>key', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'users', [
            { $sort: { name: 1 } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("ORDER BY data->>'name' ASC"));
    });

    it('throws on unsupported pipeline stage', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'users', [{ $graphLookup: { from: 'other' } }]),
            /Unsupported pipeline stage: \$graphLookup/
        );
    });

    it('throws on unsupported accumulator', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'users', [
                { $group: { _id: null, vals: { $first: '$x' } } },
            ]),
            /Unsupported accumulator: \$first/
        );
    });

    it('empty pipeline returns all rows', async () => {
        const rows = [{ _id: 'a', data: { x: 1 }, created_at: 'now' }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await docAggregate(client, 'items', []);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT _id, data, created_at FROM items'));
        assert.ok(!sql.includes('WHERE'));
        assert.ok(!sql.includes('GROUP BY'));
        assert.ok(!sql.includes('ORDER BY'));
        assert.deepEqual(result, rows);
    });

    it('$match with comparison operators', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $match: { amount: { $gte: 100 } } },
            { $limit: 10 },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'amount')::numeric >= $1"));
        assert.ok(sql.includes('LIMIT $2'));
        assert.deepEqual(client._calls[0].values, [100, 10]);
    });
});

// ─── Comparison Operators ─────────────────────────────────────────────────

describe('comparison operators', () => {
    it('$gt generates numeric comparison', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'products', { price: { $gt: 100 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'price')::numeric > $1"));
        assert.deepEqual(client._calls[0].values, [100]);
    });

    it('$gte and $lte generate range', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'products', { price: { $gte: 10, $lte: 50 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'price')::numeric >= $1"));
        assert.ok(sql.includes("(data->>'price')::numeric <= $2"));
        assert.deepEqual(client._calls[0].values, [10, 50]);
    });

    it('$lt generates less-than', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'events', { age: { $lt: 18 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'age')::numeric < $1"));
        assert.deepEqual(client._calls[0].values, [18]);
    });

    it('$ne generates not-equal', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { status: { $ne: 0 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'status')::numeric != $1"));
        assert.deepEqual(client._calls[0].values, [0]);
    });

    it('$in generates IN clause with numbered params', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { role: { $in: ['admin', 'editor'] } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'role' IN ($1, $2)"));
        assert.deepEqual(client._calls[0].values, ['admin', 'editor']);
    });

    it('$nin generates NOT IN clause', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { status: { $nin: ['banned', 'suspended'] } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'status' NOT IN ($1, $2)"));
        assert.deepEqual(client._calls[0].values, ['banned', 'suspended']);
    });

    it('$exists true checks key presence', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { email: { $exists: true } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data ? 'email'"));
        assert.deepEqual(client._calls[0].values, undefined);
    });

    it('$exists false checks key absence', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { deleted_at: { $exists: false } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("NOT (data ? 'deleted_at')"));
    });

    it('$regex generates pattern match', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { name: { $regex: '^A' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'name' ~ $1"));
        assert.deepEqual(client._calls[0].values, ['^A']);
    });

    it('$not negates inner operator', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'products', { price: { $not: { $gt: 1000 } } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("NOT ((data->>'price')::numeric > $1)"));
        assert.deepEqual(client._calls[0].values, [1000]);
    });

    it('mixed plain values and operators', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'products', { category: 'electronics', price: { $gt: 50 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data @> $'));
        assert.ok(sql.includes("(data->>'price')::numeric > $"));
        // plain key should be in containment param, operator should be separate
        const containmentIdx = sql.match(/data @> \$(\d+)::jsonb/)[1];
        const gtIdx = sql.match(/::numeric > \$(\d+)/)[1];
        assert.notEqual(containmentIdx, gtIdx);
        // containment param should be the category JSON
        const cIdx = Number(containmentIdx) - 1;
        assert.equal(client._calls[0].values[cIdx], JSON.stringify({ category: 'electronics' }));
    });

    it('operators work with docCount', async () => {
        const client = mockClient({ rows: [{ count: '5' }], rowCount: 1 });
        const result = await docCount(client, 'products', { price: { $gte: 100 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT COUNT(*)'));
        assert.ok(sql.includes("(data->>'price')::numeric >= $1"));
        assert.equal(result, 5);
    });

    it('operators work with docUpdate', async () => {
        const client = mockClient({ rows: [], rowCount: 3 });
        const result = await docUpdate(client, 'products', { price: { $lt: 10 } }, { clearance: true });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('UPDATE products SET data = data || $2::jsonb'));
        assert.ok(sql.includes("(data->>'price')::numeric < $1"));
        assert.deepEqual(client._calls[0].values, [10, JSON.stringify({ clearance: true })]);
        assert.equal(result, 3);
    });

    it('operators work with docDelete', async () => {
        const client = mockClient({ rows: [], rowCount: 2 });
        const result = await docDelete(client, 'logs', { age: { $gt: 90 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('DELETE FROM logs'));
        assert.ok(sql.includes("(data->>'age')::numeric > $1"));
        assert.deepEqual(client._calls[0].values, [90]);
        assert.equal(result, 2);
    });

    it('$in rejects empty array', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', { role: { $in: [] } }),
            /\$in requires a non-empty array/
        );
    });
});

// ─── Composite $group._id + $push/$addToSet ──────────────────────────────

describe('composite $group._id', () => {
    it('object _id builds json_build_object + multi-key GROUP BY', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $group: { _id: { region: '$region', status: '$status' }, total: { $sum: '$amount' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("json_build_object('region', data->>'region', 'status', data->>'status') AS _id"));
        assert.ok(sql.includes("SUM((data->>'amount')::numeric) AS total"));
        assert.ok(sql.includes("GROUP BY data->>'region', data->>'status'"));
    });

    it('single-key object _id', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'events', [
            { $group: { _id: { category: '$category' }, count: { $sum: 1 } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("json_build_object('category', data->>'category') AS _id"));
        assert.ok(sql.includes("GROUP BY data->>'category'"));
    });

    it('composite _id rejects non-field-reference value', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'orders', [
                { $group: { _id: { region: 'literal' }, total: { $sum: '$amount' } } },
            ]),
            /Composite _id values must be field references/
        );
    });

    it('composite _id rejects empty object', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'orders', [
                { $group: { _id: {}, total: { $sum: '$amount' } } },
            ]),
            /Composite \$group _id must have at least one field/
        );
    });
});

describe('$push and $addToSet accumulators', () => {
    it('$push generates array_agg', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $group: { _id: '$category', items: { $push: '$name' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("array_agg(data->>'name') AS items"));
        assert.ok(sql.includes("GROUP BY data->>'category'"));
    });

    it('$addToSet generates array_agg DISTINCT', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $group: { _id: '$category', unique_tags: { $addToSet: '$tag' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("array_agg(DISTINCT data->>'tag') AS unique_tags"));
    });

    it('$push with null _id (no GROUP BY)', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'logs', [
            { $group: { _id: null, all_levels: { $push: '$level' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("array_agg(data->>'level') AS all_levels"));
        assert.ok(!sql.includes('GROUP BY'));
    });

    it('$addToSet with composite _id', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'sales', [
            { $group: { _id: { region: '$region', year: '$year' }, brands: { $addToSet: '$brand' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("json_build_object('region', data->>'region', 'year', data->>'year') AS _id"));
        assert.ok(sql.includes("array_agg(DISTINCT data->>'brand') AS brands"));
        assert.ok(sql.includes("GROUP BY data->>'region', data->>'year'"));
    });
});

// ─── $project ─────────────────────────────────────────────────────────────

describe('$project', () => {
    it('include fields with 1', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'users', [
            { $project: { name: 1, email: 1 } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'name' AS name"));
        assert.ok(sql.includes("data->>'email' AS email"));
        assert.ok(!sql.includes('_id, data, created_at'));
    });

    it('exclude _id with 0', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'users', [
            { $project: { _id: 0, name: 1 } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'name' AS name"));
        assert.ok(!sql.includes("data->>'_id'"));
    });

    it('rename via field reference', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'users', [
            { $project: { username: '$name', age: 1 } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->>'name' AS username"));
        assert.ok(sql.includes("data->>'age' AS age"));
    });

    it('$project after $group uses aliases', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $group: { _id: '$category', total: { $sum: '$amount' } } },
            { $project: { _id: 1, total: 1 } },
        ]);
        const sql = client._calls[0].text;
        // After $group, _id and total are aliases — project should reference them directly
        assert.ok(sql.includes('_id'));
        assert.ok(sql.includes('total'));
        assert.ok(sql.includes("GROUP BY data->>'category'"));
    });
});

// ─── $unwind ──────────────────────────────────────────────────────────────

describe('$unwind', () => {
    it('string path adds jsonb_array_elements_text to FROM', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $unwind: '$items' },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("jsonb_array_elements_text(data->'items') AS _unwound_items"));
        assert.ok(sql.includes('FROM orders,'));
    });

    it('object path with path key', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $unwind: { path: '$tags' } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("jsonb_array_elements_text(data->'tags') AS _unwound_tags"));
    });

    it('$unwind + $group resolves field via unwind alias', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $unwind: '$items' },
            { $group: { _id: '$items', count: { $sum: 1 } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('_unwound_items AS _id'));
        assert.ok(sql.includes('GROUP BY _unwound_items'));
        assert.ok(sql.includes('COUNT(*) AS count'));
    });

    it('$unwind + $group with $sum on unwound field', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $unwind: '$scores' },
            { $group: { _id: null, total: { $sum: '$scores' } } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SUM((_unwound_scores)::numeric) AS total'));
        assert.ok(!sql.includes('GROUP BY'));
    });

    it('throws on invalid $unwind path', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'orders', [{ $unwind: 'no_dollar' }]),
            /\$unwind path must start with \$/
        );
    });
});

// ─── $lookup ──────────────────────────────────────────────────────────────

describe('$lookup', () => {
    it('generates correlated subquery', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $lookup: { from: 'customers', localField: 'customer_id', foreignField: 'cid', as: 'customer' } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes("COALESCE((SELECT json_agg(b.data) FROM customers b WHERE b.data->>'cid' = orders.data->>'customer_id'), '[]'::json) AS customer"));
    });

    it('$match + $lookup combined', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docAggregate(client, 'orders', [
            { $match: { status: 'active' } },
            { $lookup: { from: 'products', localField: 'product_id', foreignField: 'pid', as: 'product' } },
        ]);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.ok(sql.includes("COALESCE((SELECT json_agg(b.data) FROM products b WHERE b.data->>'pid' = orders.data->>'product_id'), '[]'::json) AS product"));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ status: 'active' })]);
    });

    it('throws on missing $lookup fields', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docAggregate(client, 'orders', [
                { $lookup: { from: 'customers' } },
            ]),
            /\$lookup requires from, localField, foreignField, and as/
        );
    });
});

// ─── Dot-notation expansion in plain containment filters ─────────────────

describe('dot-notation expansion', () => {
    it('single dotted key expands to nested object', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { 'addr.city': 'NY' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ addr: { city: 'NY' } })]);
    });

    it('multi-level dotted key expands to deeply nested object', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { 'a.b.c': 42 });
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ a: { b: { c: 42 } } })]);
    });

    it('non-dotted key passes through unchanged', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { status: 'active' });
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ status: 'active' })]);
    });

    it('mixed dotted and plain keys', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { 'addr.city': 'NY', active: true });
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ addr: { city: 'NY' }, active: true }),
        ]);
    });

    it('multiple dotted keys sharing a prefix merge correctly', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { 'addr.city': 'NY', 'addr.zip': '10001' });
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ addr: { city: 'NY', zip: '10001' } }),
        ]);
    });

    it('dot expansion works in docCount', async () => {
        const client = mockClient({ rows: [{ count: '3' }], rowCount: 1 });
        await docCount(client, 'orders', { 'ship.country': 'US' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ ship: { country: 'US' } })]);
    });

    it('dot expansion works in docUpdate', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { 'profile.verified': true }, { level: 'pro' });
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ profile: { verified: true } }),
            JSON.stringify({ level: 'pro' }),
        ]);
    });

    it('dot expansion works in docDelete', async () => {
        const client = mockClient({ rows: [], rowCount: 2 });
        await docDelete(client, 'logs', { 'meta.source': 'test' });
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ meta: { source: 'test' } }),
        ]);
    });
});

// ─── docWatch ─────────────────────────────────────────────────────────────

function mockListenClient(queryResult) {
    const calls = [];
    const listeners = {};
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], rowCount: 0 };
        },
        on(event, fn) {
            if (!listeners[event]) listeners[event] = [];
            listeners[event].push(fn);
        },
        removeListener(event, fn) {
            if (listeners[event]) {
                listeners[event] = listeners[event].filter(f => f !== fn);
            }
        },
        _calls: calls,
        _listeners: listeners,
    };
}

describe('docWatch', () => {
    it('creates trigger, function, and LISTEN, returns stop handle', async () => {
        const client = mockListenClient();
        const events = [];
        const watcher = await docWatch(client, 'orders', (ev) => events.push(ev));

        // Should have: CREATE FUNCTION, DROP TRIGGER IF EXISTS, CREATE TRIGGER, LISTEN
        assert.equal(client._calls.length, 4);
        assert.ok(client._calls[0].text.includes('CREATE OR REPLACE FUNCTION orders_notify_fn'));
        assert.ok(client._calls[0].text.includes('pg_notify'));
        assert.ok(client._calls[0].text.includes('TG_OP'));
        assert.ok(client._calls[1].text.includes('DROP TRIGGER IF EXISTS orders_notify_trg ON orders'));
        assert.ok(client._calls[2].text.includes('CREATE TRIGGER orders_notify_trg'));
        assert.ok(client._calls[2].text.includes('AFTER INSERT OR UPDATE OR DELETE ON orders'));
        assert.ok(client._calls[2].text.includes('EXECUTE FUNCTION orders_notify_fn'));
        assert.ok(client._calls[3].text.includes('LISTEN orders_changes'));

        // Listener registered
        assert.equal(client._listeners.notification.length, 1);

        // stop() removes listener
        assert.equal(typeof watcher.stop, 'function');
        watcher.stop();
        assert.equal(client._listeners.notification.length, 0);
    });

    it('validates collection identifier', async () => {
        const client = mockListenClient();
        await assert.rejects(
            () => docWatch(client, 'DROP;--', () => {}),
            /Invalid identifier/
        );
    });
});

// ─── docUnwatch ───────────────────────────────────────────────────────────

describe('docUnwatch', () => {
    it('drops trigger, function, and UNLISTEN', async () => {
        const client = mockClient();
        await docUnwatch(client, 'orders');
        assert.equal(client._calls.length, 3);
        assert.ok(client._calls[0].text.includes('DROP TRIGGER IF EXISTS orders_notify_trg ON orders'));
        assert.ok(client._calls[1].text.includes('DROP FUNCTION IF EXISTS orders_notify_fn'));
        assert.ok(client._calls[2].text.includes('UNLISTEN orders_changes'));
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docUnwatch(client, '1bad'),
            /Invalid identifier/
        );
    });
});

// ─── docCreateTtlIndex ───────────────────────────────────────────────────

describe('docCreateTtlIndex', () => {
    it('creates index, trigger function, and trigger with default field', async () => {
        const client = mockClient();
        await docCreateTtlIndex(client, 'sessions', 3600);
        assert.equal(client._calls.length, 4);
        assert.ok(client._calls[0].text.includes('CREATE INDEX IF NOT EXISTS sessions_ttl_idx ON sessions (created_at)'));
        assert.ok(client._calls[1].text.includes('CREATE OR REPLACE FUNCTION sessions_ttl_fn'));
        assert.ok(client._calls[1].text.includes("INTERVAL '3600 seconds'"));
        assert.ok(client._calls[1].text.includes('DELETE FROM sessions WHERE created_at'));
        assert.ok(client._calls[2].text.includes('DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions'));
        assert.ok(client._calls[3].text.includes('CREATE TRIGGER sessions_ttl_trg'));
        assert.ok(client._calls[3].text.includes('BEFORE INSERT ON sessions'));
    });

    it('uses custom field', async () => {
        const client = mockClient();
        await docCreateTtlIndex(client, 'tokens', 86400, { field: 'expires_at' });
        assert.ok(client._calls[0].text.includes('ON tokens (expires_at)'));
        assert.ok(client._calls[1].text.includes('WHERE expires_at'));
    });

    it('rejects non-positive expireAfterSeconds', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docCreateTtlIndex(client, 'items', 0),
            /expireAfterSeconds must be a positive number/
        );
        await assert.rejects(
            () => docCreateTtlIndex(client, 'items', -10),
            /expireAfterSeconds must be a positive number/
        );
    });
});

// ─── docRemoveTtlIndex ──────────────────────────────────────────────────

describe('docRemoveTtlIndex', () => {
    it('drops trigger, function, and index', async () => {
        const client = mockClient();
        await docRemoveTtlIndex(client, 'sessions');
        assert.equal(client._calls.length, 3);
        assert.ok(client._calls[0].text.includes('DROP TRIGGER IF EXISTS sessions_ttl_trg ON sessions'));
        assert.ok(client._calls[1].text.includes('DROP FUNCTION IF EXISTS sessions_ttl_fn'));
        assert.ok(client._calls[2].text.includes('DROP INDEX IF EXISTS sessions_ttl_idx'));
    });
});

// ─── docCreateCapped ────────────────────────────────────────────────────

describe('docCreateCapped', () => {
    it('ensures collection and creates cap trigger', async () => {
        const client = mockClient();
        await docCreateCapped(client, 'logs', 1000);
        // ensureCollection (1) + CREATE FUNCTION (2) + DROP TRIGGER (3) + CREATE TRIGGER (4)
        assert.equal(client._calls.length, 4);
        assert.ok(client._calls[0].text.includes('CREATE TABLE IF NOT EXISTS logs'));
        assert.ok(client._calls[1].text.includes('CREATE OR REPLACE FUNCTION logs_cap_fn'));
        assert.ok(client._calls[1].text.includes('DELETE FROM logs'));
        assert.ok(client._calls[1].text.includes('ORDER BY created_at ASC'));
        assert.ok(client._calls[1].text.includes('LIMIT GREATEST'));
        assert.ok(client._calls[1].text.includes('1000'));
        assert.ok(client._calls[2].text.includes('DROP TRIGGER IF EXISTS logs_cap_trg ON logs'));
        assert.ok(client._calls[3].text.includes('CREATE TRIGGER logs_cap_trg'));
        assert.ok(client._calls[3].text.includes('AFTER INSERT ON logs'));
    });

    it('rejects non-positive maxDocuments', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docCreateCapped(client, 'logs', 0),
            /maxDocuments must be a positive number/
        );
        await assert.rejects(
            () => docCreateCapped(client, 'logs', -5),
            /maxDocuments must be a positive number/
        );
    });
});

// ─── docRemoveCap ───────────────────────────────────────────────────────

describe('docRemoveCap', () => {
    it('drops trigger and function', async () => {
        const client = mockClient();
        await docRemoveCap(client, 'logs');
        assert.equal(client._calls.length, 2);
        assert.ok(client._calls[0].text.includes('DROP TRIGGER IF EXISTS logs_cap_trg ON logs'));
        assert.ok(client._calls[1].text.includes('DROP FUNCTION IF EXISTS logs_cap_fn'));
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docRemoveCap(client, 'bad table'),
            /Invalid identifier/
        );
    });
});

// ─── Logical Operators ($or, $and, $not) ─────────────────────────────────

describe('logical operators', () => {
    it('$or generates OR clause', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { $or: [{ status: 'active' }, { role: 'admin' }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WHERE'));
        assert.ok(sql.includes('OR'));
        assert.ok(sql.includes('data @>'));
    });

    it('$and generates AND clause', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { $and: [{ status: 'active' }, { role: 'admin' }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WHERE'));
        assert.ok(sql.includes('AND'));
    });

    it('$not at top level negates filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { $not: { status: 'banned' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('NOT ('));
        assert.ok(sql.includes('data @>'));
    });

    it('nested $or inside $and', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', {
            $and: [
                { $or: [{ role: 'admin' }, { role: 'editor' }] },
                { active: true },
            ],
        });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('AND'));
        assert.ok(sql.includes('OR'));
    });

    it('$or with comparison operators threads paramIdx', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { $or: [{ age: { $gt: 18 } }, { age: { $lt: 5 } }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('$1'));
        assert.ok(sql.includes('$2'));
        assert.deepEqual(client._calls[0].values, [18, 5]);
    });

    it('mixed logical operators with field filters', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFind(client, 'users', { name: 'Alice', $or: [{ age: { $gt: 18 } }, { role: 'admin' }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data @>'));
        assert.ok(sql.includes('OR'));
    });

    it('$or rejects non-array', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', { $or: { a: 1 } }),
            /\$or value must be a non-empty array/
        );
    });

    it('$or rejects empty array', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', { $or: [] }),
            /\$or value must be a non-empty array/
        );
    });

    it('$and rejects non-array', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', { $and: 'bad' }),
            /\$and value must be a non-empty array/
        );
    });

    it('$not rejects non-object', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFind(client, 'users', { $not: [1, 2] }),
            /\$not value must be a filter object/
        );
    });

    it('logical operators work with docCount', async () => {
        const client = mockClient({ rows: [{ count: '3' }], rowCount: 1 });
        const result = await docCount(client, 'users', { $or: [{ active: true }, { role: 'admin' }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT COUNT(*)'));
        assert.ok(sql.includes('OR'));
        assert.equal(result, 3);
    });

    it('logical operators work with docDelete', async () => {
        const client = mockClient({ rows: [], rowCount: 2 });
        const result = await docDelete(client, 'users', { $or: [{ status: 'banned' }, { status: 'spam' }] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('DELETE FROM users'));
        assert.ok(sql.includes('OR'));
        assert.equal(result, 2);
    });
});

// ─── Update Operators ($set, $inc, $unset, $mul, $rename) ────────────────

describe('update operators', () => {
    it('$set generates jsonb merge', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $set: { age: 30 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SET data = (data || $2::jsonb)'));
        assert.ok(sql.includes('WHERE data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ name: 'Alice' }),
            JSON.stringify({ age: 30 }),
        ]);
    });

    it('$inc generates numeric increment', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $inc: { score: 10 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_set(data'));
        assert.ok(sql.includes('::text[]'));
        assert.ok(sql.includes('COALESCE'));
        assert.ok(sql.includes('+ $'));
    });

    it('$unset top-level field', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $unset: ['temp'] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('(data - $2)'));
    });

    it('$unset nested field uses #-', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $unset: ['addr.temp'] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('#- $2::text[]'));
        assert.ok(client._calls[0].values.includes('{addr,temp}'));
    });

    it('$mul generates multiply', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $mul: { score: 2 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('* $'));
    });

    it('$rename moves field', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $rename: { old_name: 'new_name' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_set'));
        assert.ok(sql.includes('- $'));
    });

    it('combined $set and $inc', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $set: { status: 'active' }, $inc: { loginCount: 1 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('||'));
        assert.ok(sql.includes('jsonb_set'));
        assert.ok(sql.includes('COALESCE'));
    });

    it('nested field in $inc', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $inc: { 'stats.views': 1 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_set'));
        assert.ok(client._calls[0].values.some(v => v === '{stats,views}'));
    });

    it('plain update (no $operators) still works', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { age: 31 });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data || $2::jsonb'));
        assert.deepEqual(client._calls[0].values, [
            JSON.stringify({ name: 'Alice' }),
            JSON.stringify({ age: 31 }),
        ]);
    });

    it('update operators work with docUpdateOne', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdateOne(client, 'users', { name: 'Alice' }, { $set: { age: 30 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WITH target AS'));
        assert.ok(sql.includes('(data || $2::jsonb)'));
    });
});

// ─── Array Update Operators ($push, $pull, $addToSet) ────────────────────

describe('array update operators', () => {
    it('$push appends to array', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $push: { tags: 'vip' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_set'));
        assert.ok(sql.includes('COALESCE'));
        assert.ok(sql.includes("'[]'::jsonb"));
        assert.ok(sql.includes('to_jsonb'));
    });

    it('$pull removes from array', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $pull: { tags: 'temp' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_agg'));
        assert.ok(sql.includes('jsonb_array_elements'));
        assert.ok(sql.includes('WHERE elem !='));
    });

    it('$addToSet adds only if not present', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $addToSet: { tags: 'unique' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('CASE WHEN'));
        assert.ok(sql.includes('@>'));
        assert.ok(sql.includes('ELSE'));
    });

    it('$push with numeric value', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'items', {}, { $push: { scores: 99 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('to_jsonb($2::numeric)'));
    });

    it('combined $set and $push', async () => {
        const client = mockClient({ rows: [], rowCount: 1 });
        await docUpdate(client, 'users', { name: 'Alice' }, { $set: { updated: true }, $push: { log: 'action' } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('||'));
        assert.ok(sql.includes('jsonb_set'));
    });
});

// ─── docFindOneAndUpdate ─────────────────────────────────────────────────

describe('docFindOneAndUpdate', () => {
    it('returns updated document with RETURNING', async () => {
        const row = { _id: 'abc', data: { name: 'Alice', age: 30 }, created_at: '2026-01-01' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await docFindOneAndUpdate(client, 'users', { name: 'Alice' }, { $set: { age: 30 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WITH target AS'));
        assert.ok(sql.includes('RETURNING'));
        assert.ok(sql.includes('users._id'));
        assert.ok(sql.includes('users.data'));
        assert.ok(sql.includes('users.created_at'));
        assert.deepEqual(result, row);
    });

    it('returns null when no match', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docFindOneAndUpdate(client, 'users', { name: 'Nobody' }, { $set: { age: 0 } });
        assert.equal(result, null);
    });

    it('works with plain update (no $operators)', async () => {
        const row = { _id: 'x', data: { a: 1 }, created_at: 'now' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await docFindOneAndUpdate(client, 'items', { a: 1 }, { a: 2 });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('data || $2::jsonb'));
        assert.ok(sql.includes('RETURNING'));
        assert.deepEqual(result, row);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFindOneAndUpdate(client, 'bad table', {}, {}),
            /Invalid identifier/
        );
    });

    it('works with $inc operator', async () => {
        const row = { _id: 'abc', data: { score: 11 }, created_at: 'now' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        await docFindOneAndUpdate(client, 'users', { name: 'Alice' }, { $inc: { score: 1 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('jsonb_set'));
        assert.ok(sql.includes('RETURNING'));
    });
});

// ─── docFindOneAndDelete ─────────────────────────────────────────────────

describe('docFindOneAndDelete', () => {
    it('returns deleted document with RETURNING', async () => {
        const row = { _id: 'abc', data: { name: 'Alice' }, created_at: '2026-01-01' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await docFindOneAndDelete(client, 'users', { name: 'Alice' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('WITH target AS'));
        assert.ok(sql.includes('DELETE FROM users USING target'));
        assert.ok(sql.includes('RETURNING'));
        assert.ok(sql.includes('users._id'));
        assert.ok(sql.includes('users.data'));
        assert.ok(sql.includes('users.created_at'));
        assert.deepEqual(result, row);
    });

    it('returns null when no match', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docFindOneAndDelete(client, 'users', { name: 'Nobody' });
        assert.equal(result, null);
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docFindOneAndDelete(client, '1bad', {}),
            /Invalid identifier/
        );
    });

    it('uses filter with comparison operators', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docFindOneAndDelete(client, 'logs', { age: { $gt: 90 } });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("(data->>'age')::numeric > $1"));
        assert.ok(sql.includes('RETURNING'));
    });
});

// ─── docDistinct ─────────────────────────────────────────────────────────

describe('docDistinct', () => {
    it('generates SELECT DISTINCT', async () => {
        const client = mockClient({ rows: [{ '?column?': 'admin' }, { '?column?': 'user' }], rowCount: 2 });
        const result = await docDistinct(client, 'users', 'role');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT DISTINCT'));
        assert.ok(sql.includes("data->>'role'"));
        assert.ok(sql.includes('IS NOT NULL'));
        assert.deepEqual(result, ['admin', 'user']);
    });

    it('handles dot-notation field', async () => {
        const client = mockClient({ rows: [{ '?column?': 'NY' }], rowCount: 1 });
        const result = await docDistinct(client, 'users', 'addr.city');
        const sql = client._calls[0].text;
        assert.ok(sql.includes("data->'addr'->>'city'"));
        assert.deepEqual(result, ['NY']);
    });

    it('applies filter when provided', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await docDistinct(client, 'users', 'role', { active: true });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT DISTINCT'));
        assert.ok(sql.includes('IS NOT NULL'));
        assert.ok(sql.includes('data @> $1::jsonb'));
        assert.deepEqual(client._calls[0].values, [JSON.stringify({ active: true })]);
    });

    it('returns empty array when no results', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await docDistinct(client, 'users', 'role');
        assert.deepEqual(result, []);
    });

    it('works without filter', async () => {
        const client = mockClient({ rows: [{ '?column?': 'a' }], rowCount: 1 });
        const result = await docDistinct(client, 'items', 'category');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT DISTINCT'));
        assert.ok(sql.includes('IS NOT NULL'));
        // No filter-related clause besides IS NOT NULL
        assert.ok(!sql.includes('data @>'));
    });

    it('validates collection identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => docDistinct(client, 'bad table', 'field'),
            /Invalid identifier/
        );
    });
});
