# Changelog

## Unreleased

### Breaking changes

**Flat `doc*` utility re-exports removed.** The plugin used to re-export
`docInsert`, `docFind`, `docUpdate`, `docDelete`, `docCount`,
`docCreateIndex`, `docAggregate`, `docWatch`, `docUnwatch`,
`docCreateTtlIndex`, `docRemoveTtlIndex`, `docCreateCapped`, and
`docRemoveCap` from the core `goldlapel` package. These were broken after
the v0.2 schema-to-core nesting refactor — they require a `patterns`
argument resolved from the proxy dashboard, and only the new
`gl.documents.<verb>` sub-API knows how to fetch it.

Migration:

```javascript
// Before — broken after v0.2
import { docInsert } from '@goldlapel/prisma'
await docInsert(client, 'users', { name: 'Alice' })

// After
import { start } from '@goldlapel/prisma'
const gl = await start(process.env.DATABASE_URL)
await gl.documents.insert('users', { name: 'Alice' })
```

`start`, `GoldLapel`, and `NativeCache` continue to be re-exported. The
ORM-focused entry points (`withGoldLapel()` and `init()`) are unchanged.

### New exports

`DocumentsAPI` and `StreamsAPI` classes are now re-exported for users who
want to type-check or extend the sub-API surface.
