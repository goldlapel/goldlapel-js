# Changelog

## Unreleased

### Breaking changes

**Doc-store and stream methods moved under nested namespaces.** The flat
`gl.doc*` and `gl.stream*` methods are gone; document and stream operations
now live under `gl.documents.<verb>` and `gl.streams.<verb>`. No
backwards-compat aliases — search and replace once.

Migration map:

| Old (flat)                                | New (nested)                              |
| ----------------------------------------- | ----------------------------------------- |
| `gl.docInsert(name, doc)`                 | `gl.documents.insert(name, doc)`          |
| `gl.docInsertMany(name, docs)`            | `gl.documents.insertMany(name, docs)`     |
| `gl.docFind(name, filter)`                | `gl.documents.find(name, filter)`         |
| `gl.docFindOne(name, filter)`             | `gl.documents.findOne(name, filter)`      |
| `gl.docFindCursor(name, ...)`             | `gl.documents.findCursor(name, ...)`      |
| `gl.docUpdate(name, f, u)`                | `gl.documents.update(name, f, u)`         |
| `gl.docUpdateOne(name, f, u)`             | `gl.documents.updateOne(name, f, u)`      |
| `gl.docDelete(name, f)`                   | `gl.documents.delete(name, f)`            |
| `gl.docDeleteOne(name, f)`                | `gl.documents.deleteOne(name, f)`         |
| `gl.docFindOneAndUpdate(...)`             | `gl.documents.findOneAndUpdate(...)`      |
| `gl.docFindOneAndDelete(...)`             | `gl.documents.findOneAndDelete(...)`      |
| `gl.docDistinct(name, field, f)`          | `gl.documents.distinct(name, field, f)`   |
| `gl.docCount(name, filter)`               | `gl.documents.count(name, filter)`        |
| `gl.docCreateIndex(name, keys)`           | `gl.documents.createIndex(name, keys)`    |
| `gl.docAggregate(name, pipeline)`         | `gl.documents.aggregate(name, pipeline)`  |
| `gl.docWatch(name, cb)`                   | `gl.documents.watch(name, cb)`            |
| `gl.docUnwatch(name)`                     | `gl.documents.unwatch(name)`              |
| `gl.docCreateTtlIndex(name, n)`           | `gl.documents.createTtlIndex(name, n)`    |
| `gl.docRemoveTtlIndex(name)`              | `gl.documents.removeTtlIndex(name)`       |
| `gl.docCreateCapped(name, max)`           | `gl.documents.createCapped(name, max)`    |
| `gl.docRemoveCap(name)`                   | `gl.documents.removeCap(name)`            |
| `gl.docCreateCollection(name, opts)`      | `gl.documents.createCollection(name, opts)` |
| `gl.streamAdd(name, payload)`             | `gl.streams.add(name, payload)`           |
| `gl.streamCreateGroup(name, group)`       | `gl.streams.createGroup(name, group)`     |
| `gl.streamRead(name, g, c, count)`        | `gl.streams.read(name, g, c, count)`      |
| `gl.streamAck(name, group, id)`           | `gl.streams.ack(name, group, id)`         |
| `gl.streamClaim(name, g, c, ...)`         | `gl.streams.claim(name, g, c, ...)`       |

Other namespaces (`gl.search`, `gl.publish` / `gl.subscribe`, `gl.incr`,
`gl.zadd`, `gl.hset`, `gl.geoadd`, …) remain flat and will migrate to
nested form in subsequent releases (one namespace per schema-to-core
phase).

The standalone `utils.js` exports (`docInsert`, `docFind`, …) still exist
but now require a `patterns` argument supplied by the proxy; direct callers
will hit a clear `requires DDL patterns from the proxy` error pointing them
at `gl.documents.<verb>`.

**Doc-store DDL is now owned by the proxy.** The wrapper no longer emits
`CREATE TABLE _goldlapel.doc_<name>` SQL when a collection is first used.
Instead, `gl.documents.<verb>` calls `POST /api/ddl/doc_store/create`
against the proxy's dashboard port; the proxy runs the canonical DDL on its
management connection and returns the table reference + query patterns.
The wrapper caches `(tables, query_patterns)` per session — one HTTP
round-trip per (family, name) per session.

Canonical doc-store schema (v1) standardizes the column shape across every
Gold Lapel wrapper:

```
_id        UUID PRIMARY KEY DEFAULT gen_random_uuid()
data       JSONB NOT NULL
created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
```

Both timestamps are `NOT NULL` — kills the `created_at NOT NULL` /
`updated_at` drift surfaced in the v0.2 cross-wrapper compat audit. Any
wrapper (Python, JS, Ruby, Java, PHP, Go, .NET) writing to a doc-store
collection now produces the same table.

**Upgrade path for dev databases:** wipe and recreate. There is no
in-place migration. Pre-1.0, dev databases get rebuilt freely.

```bash
goldlapel clean   # drops _goldlapel.* tables
# ...drop/recreate your DB if needed...
```

If you have a v0.2-pre wrapper running against a v0.2-post proxy, the
wrapper's first `gl.documents.<verb>` call surfaces a clear
`version_mismatch` error pointing to this CHANGELOG.

### New exports

`DocumentsAPI` and `StreamsAPI` classes are now exported from
`'goldlapel'` for users who want to type-check or mock the sub-API surface
directly.
