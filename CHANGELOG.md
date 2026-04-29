# Changelog

## Unreleased

### Breaking changes

**Phase 5 — Redis-compat helpers moved under nested namespaces.** The flat
counter / zset / hash / queue / geo methods are gone; each family lives
under its own sub-API. No backwards-compat aliases — search and replace
once. The new schemas have richer semantics than the old fire-and-forget
helpers, so a one-time migration is the right cost: `gl.queues.claim` /
`gl.queues.ack` replaces `gl.dequeue` (at-least-once with visibility
timeout, not lossy delete-on-fetch); `gl.zsets.add(name, zsetKey, …)` adds
a `zsetKey` column so one namespace table holds many sorted sets;
`gl.hashes` is row-per-field instead of JSONB-blob-per-key; `gl.geos` is
GEOGRAPHY-native and idempotent on the member name.

Migration map (Phase 5):

| Old (flat)                                | New (nested)                              |
| ----------------------------------------- | ----------------------------------------- |
| `gl.incr(name, key, n)`                   | `gl.counters.incr(name, key, n)`          |
| `gl.getCounter(name, key)`                | `gl.counters.get(name, key)`              |
| `gl.zadd(name, member, score)`            | `gl.zsets.add(name, zsetKey, member, score)` |
| `gl.zincrby(name, member, delta)`         | `gl.zsets.incrBy(name, zsetKey, member, delta)` |
| `gl.zrange(name, start, stop, desc)`      | `gl.zsets.range(name, zsetKey, { start, stop, desc })` |
| `gl.zrank(name, member, desc)`            | `gl.zsets.rank(name, zsetKey, member, { desc })` |
| `gl.zscore(name, member)`                 | `gl.zsets.score(name, zsetKey, member)`   |
| `gl.zrem(name, member)`                   | `gl.zsets.remove(name, zsetKey, member)`  |
| `gl.hset(name, key, field, value)`        | `gl.hashes.set(name, hashKey, field, value)` |
| `gl.hget(name, key, field)`               | `gl.hashes.get(name, hashKey, field)`     |
| `gl.hgetall(name, key)`                   | `gl.hashes.getAll(name, hashKey)`         |
| `gl.hdel(name, key, field)`               | `gl.hashes.delete(name, hashKey, field)`  |
| `gl.enqueue(name, payload)`               | `gl.queues.enqueue(name, payload)`        |
| `gl.dequeue(name)` (delete-on-fetch)      | `gl.queues.claim(name)` + `gl.queues.ack(name, id)` (at-least-once) |
| `gl.geoadd(name, …)` (GEOMETRY, BIGSERIAL pk) | `gl.geos.add(name, member, lon, lat)` (GEOGRAPHY, member pk, idempotent) |
| `gl.georadius(name, …)`                   | `gl.geos.radius(name, lon, lat, radius, { unit, limit })` |
| `gl.geodist(name, …)`                     | `gl.geos.dist(name, memberA, memberB, { unit })` |

Schema/contract changes worth knowing about:

- **counter**: every UPDATE path stamps `updated_at = NOW()` (proxy-side).
- **zset**: schema gains `zset_key` column; one namespace table now holds
  many sorted sets, partitioned by `zset_key`.
- **hash**: storage flips from JSONB-blob-per-key to row-per-(hash_key,
  field). `set` is a single-row UPSERT; `getAll` rebuilds a JS object from
  rows.
- **queue**: at-least-once delivery with visibility timeout. `dequeue`
  (delete-on-fetch, lossy on consumer crash) is gone. Use `claim` →
  process → `ack` (or `abandon` to release the lease without ack). No
  `dequeue` compat shim — explicit by design.
- **geo**: column type is GEOGRAPHY (not GEOMETRY); `member` is the
  primary key. Re-adding a member updates its location (idempotent).
  Distances are meters-native; methods accept `{ unit: 'm' | 'km' | 'mi' |
  'ft' }`.

**Phase 4 — Doc-store and stream methods moved under nested namespaces.**
The flat `gl.doc*` and `gl.stream*` methods are gone; document and stream
operations now live under `gl.documents.<verb>` and `gl.streams.<verb>`.
No backwards-compat aliases — search and replace once.

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

Other namespaces (`gl.search`, `gl.publish` / `gl.subscribe`,
`gl.percolate*`, `gl.analyze`, …) remain flat and will migrate to nested
form in subsequent releases (one namespace per schema-to-core phase).

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
