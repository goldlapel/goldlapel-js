# Changelog

## Unreleased

### Breaking changes

**Every `goldlapel` start option is forwarded.** Besides `proxyPort`,
`config` and `extraArgs`, the plugin now passes `dashboardPort`, `logLevel`,
`mode`, `license`, `client`, `configFile`, `silent`, `mesh`, `meshTag` and
the `disable*` switches through to `start()` as given (and `proxyPort` only
when you set one, so several databases get their own ports). Unknown and
removed options are an error, from `start()`. Options that aren't Gold Lapel's still go to `drizzle-orm`; removed Gold Lapel options (`nativeCache`, `invalidationPort`) no longer leak into it. `init()` rejects options that aren't Gold Lapel's.

**The in-process cache is gone.** `drizzle()` hands Drizzle a plain
`pg.Pool` pointed at the proxy instead of a cache-wrapped one, and no
longer takes `invalidationPort` or `nativeCache`. The `wrap` and
`NativeCache` re-exports are removed. Caching now happens in the Gold
Lapel proxy for every client.

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
import { docInsert } from '@goldlapel/drizzle'
await docInsert(client, 'users', { name: 'Alice' })

// After
import { start } from '@goldlapel/drizzle'
const gl = await start(process.env.DATABASE_URL)
await gl.documents.insert('users', { name: 'Alice' })
```

`start` and `GoldLapel` continue to be re-exported.

### New exports

`DocumentsAPI` and `StreamsAPI` classes are now re-exported for users who
want to type-check or extend the sub-API surface.
