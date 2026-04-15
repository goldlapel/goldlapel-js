import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
    search,
    searchFuzzy,
    searchPhonetic,
    similar,
    suggest,
    facets,
    aggregate,
    createSearchConfig,
    percolateAdd,
    percolate,
    percolateDelete,
    analyze,
    explainScore,
} from '../utils.js';

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

// ─── search ─────────────────────────────────────────────────────────────────

describe('search', () => {
    it('generates correct SQL for single column', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await search(client, 'articles', 'body', 'hello world');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('FROM articles'));
        assert.ok(sql.includes("to_tsvector($1, coalesce(body, ''))"));
        assert.ok(sql.includes('plainto_tsquery($2, $3)'));
        assert.ok(sql.includes('ORDER BY _score DESC'));
        assert.ok(sql.includes('LIMIT $4'));
        assert.deepEqual(client._calls[0].values, ['english', 'english', 'hello world', 50]);
    });

    it('coalesces multiple columns', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await search(client, 'articles', ['title', 'body'], 'query');
        const sql = client._calls[0].text;
        assert.ok(sql.includes("coalesce(title, '') || ' ' || coalesce(body, '')"));
    });

    it('uses custom limit and lang', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await search(client, 'posts', 'content', 'test', { limit: 10, lang: 'french' });
        assert.deepEqual(client._calls[0].values, ['french', 'french', 'test', 10]);
    });

    it('includes highlight fields when highlight=true', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await search(client, 'articles', 'body', 'test', { highlight: true });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('ts_headline'));
        assert.ok(sql.includes('_highlight'));
        assert.ok(sql.includes('StartSel=<mark>'));
        assert.ok(sql.includes('StopSel=</mark>'));
        assert.ok(sql.includes('LIMIT $5'));
        assert.deepEqual(client._calls[0].values, ['english', 'english', 'test', 'english', 50]);
    });

    it('returns rows from result', async () => {
        const rows = [{ id: 1, body: 'hello', _score: 0.5 }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await search(client, 'articles', 'body', 'hello');
        assert.deepEqual(result, rows);
    });

    it('validates table identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => search(client, 'DROP TABLE x; --', 'body', 'q'),
            /Invalid identifier/
        );
    });

    it('validates column identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => search(client, 'articles', ['good', 'bad; DROP'], 'q'),
            /Invalid identifier/
        );
    });
});

// ─── searchFuzzy ────────────────────────────────────────────────────────────

describe('searchFuzzy', () => {
    it('queries with pg_trgm similarity', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await searchFuzzy(client, 'products', 'name', 'widget');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('similarity(name, $1)'));
        assert.ok(sql.includes('FROM products'));
        assert.ok(sql.includes('> $2'));
        assert.ok(sql.includes('LIMIT $3'));
        assert.deepEqual(client._calls[0].values, ['widget', 0.3, 50]);
    });

    it('uses custom threshold and limit', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await searchFuzzy(client, 'products', 'name', 'widgt', { threshold: 0.5, limit: 5 });
        assert.deepEqual(client._calls[0].values, ['widgt', 0.5, 5]);
    });

    it('returns rows', async () => {
        const rows = [{ id: 1, name: 'widget', _score: 0.8 }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await searchFuzzy(client, 'products', 'name', 'widget');
        assert.deepEqual(result, rows);
    });

    it('validates table identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => searchFuzzy(client, '1badtable', 'name', 'q'),
            /Invalid identifier/
        );
    });

    it('validates column identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => searchFuzzy(client, 'products', 'name; DROP', 'q'),
            /Invalid identifier/
        );
    });
});

// ─── searchPhonetic ─────────────────────────────────────────────────────────

describe('searchPhonetic', () => {
    it('queries with phonetic matching', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await searchPhonetic(client, 'users', 'name', 'john');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('soundex(name) = soundex($1)'));
        assert.ok(sql.includes('similarity(name, $1) AS _score'));
        assert.ok(sql.includes('FROM users'));
        assert.ok(sql.includes('LIMIT $2'));
        assert.deepEqual(client._calls[0].values, ['john', 50]);
    });

    it('uses custom limit', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await searchPhonetic(client, 'users', 'name', 'jon', { limit: 5 });
        assert.deepEqual(client._calls[0].values, ['jon', 5]);
    });

    it('returns rows ordered by score then column', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await searchPhonetic(client, 'users', 'name', 'john');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('ORDER BY _score DESC, name'));
    });

    it('validates identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => searchPhonetic(client, 'users', '1bad', 'q'),
            /Invalid identifier/
        );
    });
});

// ─── similar (vector similarity) ────────────────────────────────────────────

describe('similar', () => {
    it('queries with vector similarity', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await similar(client, 'docs', 'embedding', [0.1, 0.2, 0.3]);
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('(embedding <=> $1::vector)'));
        assert.ok(sql.includes('AS _score'));
        assert.ok(sql.includes('FROM docs'));
        assert.ok(sql.includes('ORDER BY _score'));
        assert.ok(sql.includes('LIMIT $2'));
        assert.deepEqual(client._calls[0].values, ['[0.1,0.2,0.3]', 10]);
    });

    it('formats vector literal correctly', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await similar(client, 'docs', 'embedding', [1, 2, 3, 4, 5]);
        assert.equal(client._calls[0].values[0], '[1,2,3,4,5]');
    });

    it('uses custom limit', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await similar(client, 'docs', 'embedding', [0.1], { limit: 3 });
        assert.equal(client._calls[0].values[1], 3);
    });

    it('returns rows', async () => {
        const rows = [{ id: 1, _score: 0.05 }, { id: 2, _score: 0.12 }];
        const client = mockClient({ rows, rowCount: 2 });
        const result = await similar(client, 'docs', 'embedding', [0.1]);
        assert.deepEqual(result, rows);
    });

    it('validates identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => similar(client, 'docs', 'bad col', [0.1]),
            /Invalid identifier/
        );
    });
});

// ─── suggest (autocomplete) ─────────────────────────────────────────────────

describe('suggest', () => {
    it('generates ILIKE query for autocomplete', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await suggest(client, 'cities', 'name', 'san f');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('similarity(name, $1) AS _score'));
        assert.ok(sql.includes('WHERE name ILIKE $2'));
        assert.ok(sql.includes('LIMIT $3'));
        assert.deepEqual(client._calls[0].values, ['san f', 'san f%', 10]);
    });

    it('uses custom limit', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await suggest(client, 'cities', 'name', 'new', { limit: 5 });
        assert.equal(client._calls[0].values[2], 5);
    });

    it('returns rows ordered by score then column', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await suggest(client, 'cities', 'name', 'san');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('ORDER BY _score DESC, name'));
    });

    it('validates identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => suggest(client, 'cities', 'DROP; --', 'q'),
            /Invalid identifier/
        );
    });
});

// ─── facets ─────────────────────────────────────────────────────────────────

describe('facets', () => {
    it('generates simple facet count without filter', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await facets(client, 'products', 'category');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SELECT category AS value, COUNT(*) AS count'));
        assert.ok(sql.includes('FROM products'));
        assert.ok(sql.includes('GROUP BY category'));
        assert.ok(sql.includes('ORDER BY count DESC, category'));
        assert.ok(sql.includes('LIMIT $1'));
        assert.deepEqual(client._calls[0].values, [50]);
    });

    it('adds tsquery filter when query and queryColumn provided', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await facets(client, 'products', 'category', { query: 'organic', queryColumn: 'description' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('to_tsvector($1'));
        assert.ok(sql.includes('plainto_tsquery($2, $3)'));
        assert.ok(sql.includes("coalesce(description, '')"));
        assert.deepEqual(client._calls[0].values, ['english', 'english', 'organic', 50]);
    });

    it('supports multi-column queryColumn', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await facets(client, 'products', 'category', { query: 'red', queryColumn: ['title', 'description'] });
        const sql = client._calls[0].text;
        assert.ok(sql.includes("coalesce(title, '') || ' ' || coalesce(description, '')"));
    });

    it('uses custom limit and lang', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await facets(client, 'products', 'brand', { limit: 10, query: 'test', queryColumn: 'name', lang: 'french' });
        assert.deepEqual(client._calls[0].values, ['french', 'french', 'test', 10]);
    });

    it('returns rows', async () => {
        const rows = [{ value: 'electronics', count: 42 }, { value: 'books', count: 17 }];
        const client = mockClient({ rows, rowCount: 2 });
        const result = await facets(client, 'products', 'category');
        assert.deepEqual(result, rows);
    });

    it('validates table and column identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => facets(client, 'bad table', 'category'),
            /Invalid identifier/
        );
        await assert.rejects(
            () => facets(client, 'products', 'bad col'),
            /Invalid identifier/
        );
    });
});

// ─── aggregate ──────────────────────────────────────────────────────────────

describe('aggregate', () => {
    it('generates simple aggregate (sum)', async () => {
        const client = mockClient({ rows: [{ value: 1500 }], rowCount: 1 });
        await aggregate(client, 'orders', 'total', 'sum');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('SUM(total) AS value'));
        assert.ok(sql.includes('FROM orders'));
        assert.ok(!sql.includes('GROUP BY'));
        assert.equal(client._calls[0].values, undefined);
    });

    it('uses COUNT(*) for count func', async () => {
        const client = mockClient({ rows: [{ value: 100 }], rowCount: 1 });
        await aggregate(client, 'orders', 'id', 'count');
        const sql = client._calls[0].text;
        assert.ok(sql.includes('COUNT(*)'));
        assert.ok(!sql.includes('COUNT(id)'));
    });

    it('generates grouped aggregate', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await aggregate(client, 'orders', 'total', 'avg', { groupBy: 'category' });
        const sql = client._calls[0].text;
        assert.ok(sql.includes('AVG(total) AS value'));
        assert.ok(sql.includes('GROUP BY category'));
        assert.ok(sql.includes('ORDER BY value DESC'));
        assert.ok(sql.includes('LIMIT $1'));
    });

    it('uses custom limit for grouped', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await aggregate(client, 'orders', 'total', 'sum', { groupBy: 'region', limit: 5 });
        assert.deepEqual(client._calls[0].values, [5]);
    });

    it('rejects invalid func', async () => {
        const client = mockClient();
        await assert.rejects(
            () => aggregate(client, 'orders', 'total', 'DROP'),
            /func must be one of/
        );
    });

    it('accepts all valid funcs', async () => {
        for (const func of ['count', 'sum', 'avg', 'min', 'max']) {
            const client = mockClient({ rows: [{ value: 1 }], rowCount: 1 });
            await aggregate(client, 'orders', 'total', func);
            assert.equal(client._calls.length, 1);
        }
    });

    it('validates identifiers', async () => {
        const client = mockClient();
        await assert.rejects(
            () => aggregate(client, 'orders', 'total; DROP', 'sum'),
            /Invalid identifier/
        );
        await assert.rejects(
            () => aggregate(client, 'orders', 'total', 'sum', { groupBy: '1=1' }),
            /Invalid identifier/
        );
    });

    it('returns rows', async () => {
        const rows = [{ value: 1500 }];
        const client = mockClient({ rows, rowCount: 1 });
        const result = await aggregate(client, 'orders', 'total', 'sum');
        assert.deepEqual(result, rows);
    });
});

// ─── createSearchConfig ─────────────────────────────────────────────────────

describe('createSearchConfig', () => {
    it('checks pg_ts_config and creates when missing', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await createSearchConfig(client, 'my_english');
        assert.equal(client._calls.length, 2);
        const checkSql = client._calls[0].text;
        assert.ok(checkSql.includes('pg_ts_config'));
        assert.ok(checkSql.includes('cfgname = $1'));
        assert.deepEqual(client._calls[0].values, ['my_english']);
        const createSql = client._calls[1].text;
        assert.ok(createSql.includes('CREATE TEXT SEARCH CONFIGURATION my_english'));
        assert.ok(createSql.includes('COPY = english'));
    });

    it('skips creation when config already exists', async () => {
        const client = mockClient({ rows: [{ '?column?': 1 }], rowCount: 1 });
        await createSearchConfig(client, 'existing_config');
        assert.equal(client._calls.length, 1);
    });

    it('uses custom copyFrom', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await createSearchConfig(client, 'my_french', { copyFrom: 'french' });
        const createSql = client._calls[1].text;
        assert.ok(createSql.includes('COPY = french'));
    });

    it('validates name identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => createSearchConfig(client, 'bad name'),
            /Invalid identifier/
        );
    });

    it('validates copyFrom identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => createSearchConfig(client, 'my_config', { copyFrom: 'bad; DROP' }),
            /Invalid identifier/
        );
    });
});

// ─── percolateAdd ───────────────────────────────────────────────────────────

describe('percolateAdd', () => {
    it('creates table, index, and inserts query', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await percolateAdd(client, 'alerts', 'q1', 'breaking news');
        assert.equal(client._calls.length, 3);
        const createSql = client._calls[0].text;
        assert.ok(createSql.includes('CREATE TABLE IF NOT EXISTS alerts'));
        assert.ok(createSql.includes('query_id TEXT PRIMARY KEY'));
        assert.ok(createSql.includes('tsquery TSQUERY NOT NULL'));
        const indexSql = client._calls[1].text;
        assert.ok(indexSql.includes('CREATE INDEX IF NOT EXISTS alerts_tsq_idx'));
        assert.ok(indexSql.includes('USING GIST (tsquery)'));
        const insertSql = client._calls[2].text;
        assert.ok(insertSql.includes('INSERT INTO alerts'));
        assert.ok(insertSql.includes('plainto_tsquery($3, $2)'));
        assert.ok(insertSql.includes('ON CONFLICT (query_id) DO UPDATE'));
        assert.deepEqual(client._calls[2].values, ['q1', 'breaking news', 'english', null]);
    });

    it('uses custom lang and metadata', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const meta = { priority: 'high', userId: 42 };
        await percolateAdd(client, 'alerts', 'q2', 'urgent', { lang: 'french', metadata: meta });
        assert.deepEqual(client._calls[2].values, ['q2', 'urgent', 'french', JSON.stringify(meta)]);
    });

    it('passes null metadata when not provided', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await percolateAdd(client, 'alerts', 'q3', 'sports');
        assert.equal(client._calls[2].values[3], null);
    });

    it('validates name identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => percolateAdd(client, 'bad name', 'q1', 'test'),
            /Invalid identifier/
        );
    });
});

// ─── percolate ──────────────────────────────────────────────────────────────

describe('percolate', () => {
    it('matches document against stored queries', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await percolate(client, 'alerts', 'breaking news about sports');
        assert.equal(client._calls.length, 1);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('FROM alerts'));
        assert.ok(sql.includes('to_tsvector($1, $2) @@ tsquery'));
        assert.ok(sql.includes('ts_rank'));
        assert.ok(sql.includes('query_id'));
        assert.ok(sql.includes('query_text'));
        assert.ok(sql.includes('metadata'));
        assert.ok(sql.includes('ORDER BY _score DESC'));
        assert.ok(sql.includes('LIMIT $3'));
        assert.deepEqual(client._calls[0].values, ['english', 'breaking news about sports', 50]);
    });

    it('uses custom lang and limit', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        await percolate(client, 'alerts', 'nouvelles', { lang: 'french', limit: 5 });
        assert.deepEqual(client._calls[0].values, ['french', 'nouvelles', 5]);
    });

    it('returns matching rows', async () => {
        const rows = [
            { query_id: 'q1', query_text: 'breaking news', metadata: null, _score: 0.6 },
            { query_id: 'q2', query_text: 'sports', metadata: '{"priority":"low"}', _score: 0.3 },
        ];
        const client = mockClient({ rows, rowCount: 2 });
        const result = await percolate(client, 'alerts', 'breaking news about sports');
        assert.deepEqual(result, rows);
    });

    it('validates name identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => percolate(client, 'DROP TABLE x; --', 'test'),
            /Invalid identifier/
        );
    });
});

// ─── percolateDelete ────────────────────────────────────────────────────────

describe('percolateDelete', () => {
    it('deletes by query_id and returns true when found', async () => {
        const client = mockClient({ rows: [{ query_id: 'q1' }], rowCount: 1 });
        const result = await percolateDelete(client, 'alerts', 'q1');
        assert.equal(result, true);
        const sql = client._calls[0].text;
        assert.ok(sql.includes('DELETE FROM alerts'));
        assert.ok(sql.includes('WHERE query_id = $1'));
        assert.ok(sql.includes('RETURNING query_id'));
        assert.deepEqual(client._calls[0].values, ['q1']);
    });

    it('returns false when query_id not found', async () => {
        const client = mockClient({ rows: [], rowCount: 0 });
        const result = await percolateDelete(client, 'alerts', 'nonexistent');
        assert.equal(result, false);
    });

    it('validates name identifier', async () => {
        const client = mockClient();
        await assert.rejects(
            () => percolateDelete(client, 'bad; name', 'q1'),
            /Invalid identifier/
        );
    });
});

// ─── analyze ────────────────────────────────────────────────────────────────

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

// ─── explainScore ───────────────────────────────────────────────────────────

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
