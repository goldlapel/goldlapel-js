import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { analyze, explainScore } from '../utils.js';

function mockClient(queryResult) {
    const calls = [];
    return {
        query: async (text, values) => {
            calls.push({ text, values });
            return queryResult ?? { rows: [], fields: [], rowCount: 0, command: 'SELECT' };
        },
        _calls: calls,
    };
}

describe('analyze', () => {
    it('sends correct SQL and params', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await analyze(client, 'hello world');
        assert.equal(client._calls.length, 1);
        assert.ok(client._calls[0].text.includes('ts_debug'));
        assert.deepEqual(client._calls[0].values, ['english', 'hello world']);
    });

    it('uses custom lang', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await analyze(client, 'bonjour', { lang: 'french' });
        assert.deepEqual(client._calls[0].values, ['french', 'bonjour']);
    });

    it('returns rows from result', async () => {
        const rows = [
            { alias: 'asciiword', description: 'Word', token: 'hello', dictionaries: '{english_stem}', dictionary: 'english_stem', lexemes: '{hello}' },
        ];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await analyze(client, 'hello');
        assert.deepEqual(result, rows);
    });

    it('returns empty array when no tokens', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await analyze(client, '');
        assert.deepEqual(result, []);
    });

    it('selects expected columns', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await analyze(client, 'test');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('alias'));
        assert.ok(sql.includes('description'));
        assert.ok(sql.includes('token'));
        assert.ok(sql.includes('dictionaries'));
        assert.ok(sql.includes('dictionary'));
        assert.ok(sql.includes('lexemes'));
    });
});

describe('explainScore', () => {
    it('sends correct SQL and params', async () => {
        const client = mockClient({ rows: [{ document_text: 'test', matches: true, score: 0.5 }], rowCount: 1 });
        await explainScore(client, 'articles', 'body', 'search term', 'id', 42);
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('FROM articles'));
        assert.ok(sql.includes('WHERE id = $3'));
        assert.deepEqual(client._calls[0].values, ['english', 'search term', 42]);
    });

    it('uses custom lang', async () => {
        const client = mockClient({ rows: [{ score: 0.1 }], rowCount: 1 });
        await explainScore(client, 'articles', 'body', 'recherche', 'id', 1, { lang: 'french' });
        assert.equal(client._calls[0].values[0], 'french');
    });

    it('returns single row object', async () => {
        const row = { document_text: 'hello world', matches: true, score: 0.6, headline: '**hello** world' };
        const client = mockClient({ rows: [row], rowCount: 1 });
        const result = await explainScore(client, 'articles', 'body', 'hello', 'id', 1);
        assert.deepEqual(result, row);
    });

    it('returns null when no matching row', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await explainScore(client, 'articles', 'body', 'nonexistent', 'id', 999);
        assert.equal(result, null);
    });

    it('validates table identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => explainScore(client, 'DROP TABLE users; --', 'body', 'q', 'id', 1),
            /Invalid identifier/
        );
    });

    it('validates column identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => explainScore(client, 'articles', 'body; DROP', 'q', 'id', 1),
            /Invalid identifier/
        );
    });

    it('validates idColumn identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => explainScore(client, 'articles', 'body', 'q', '1=1; --', 1),
            /Invalid identifier/
        );
    });

    it('includes headline with ** markers in SQL', async () => {
        const client = mockClient({ rows: [{ headline: '**match** context' }], rowCount: 1 });
        await explainScore(client, 'posts', 'content', 'match', 'id', 5);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('StartSel=**'));
        assert.ok(sql.includes('StopSel=**'));
    });

    it('SQL includes document_tokens and query_tokens casts', async () => {
        const client = mockClient({ rows: [{}], rowCount: 1 });
        await explainScore(client, 'posts', 'content', 'test', 'id', 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('document_tokens'));
        assert.ok(sql.includes('query_tokens'));
        assert.ok(sql.includes('::text'));
    });

    it('interpolates column name into tsvector expressions', async () => {
        const client = mockClient({ rows: [{}], rowCount: 1 });
        await explainScore(client, 'articles', 'title', 'test', 'slug', 'abc');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('to_tsvector($1, title)'));
        assert.ok(sql.includes('title AS document_text'));
    });
});
