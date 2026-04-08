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
    docCount,
    docCreateIndex,
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
