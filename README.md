# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-js/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-js/actions/workflows/test.yml)

The Node.js wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns, caches results, and creates indexes automatically. Zero code changes beyond the connection string.

The wrapper runs the proxy as a managed subprocess: it finds the binary, starts it with your app and stops it on exit, translates options into proxy flags, and hands back a driver-ready URL. It also carries Postgres-backed helpers (search, documents, streams, counters, sorted sets, hashes, queues, geo, pub/sub). Caching happens in the proxy, for every client alike — the wrapper adds no in-process cache, and the connection you get is a plain driver connection.

## Install

```bash
npm install goldlapel

# Plus any Postgres driver you like:
npm install pg                  # node-postgres
npm install postgres            # postgres.js
npm install @vercel/postgres    # Vercel / Neon
```

Skip the driver entirely with `{ noConnect: true }` if you only need the proxy URL (e.g. for Prisma or Drizzle).

## Quickstart

```js
import * as goldlapel from 'goldlapel';
import pg from 'pg';

// Spawn the proxy in front of your upstream DB
const gl = await goldlapel.start('postgresql://user:pass@localhost:5432/mydb');

// Point any Postgres driver at gl.url
const client = new pg.Client({ connectionString: gl.url });
await client.connect();
const { rows } = await client.query('SELECT * FROM users WHERE id = $1', [42]);

await gl.stop();  // (also cleaned up automatically on process exit)
```

Point your Postgres driver at `gl.url`. Gold Lapel sits between your app and your DB, caching results and creating indexes as it learns your query patterns. Connections opened through `gl.url` are tagged `application_name=goldlapel:js:<version>` so you can spot them in `pg_stat_activity`.

`await using` auto-cleanup, scoped connections via `gl.using(conn, cb)`, driver auto-detection, and framework integrations are in the docs.

## Dashboard

The proxy listens on two ports: the proxy port (default `7932`) and a dashboard on the next port up. Each database you `start()` in one process gets its own proxy and the next free pair — `7932`/`7933`, then `7934`/`7935`, … — unless you pass `proxyPort`; starting the same database again shares its proxy. Gold Lapel exposes the live dashboard at `gl.dashboardUrl`:

```js
console.log(gl.dashboardUrl);
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, framework integrations (Prisma, Drizzle, Next.js, SvelteKit, Express), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/javascript

## Uninstalling

Before removing the package, drop Gold Lapel's helper schema and the indexes it created from your Postgres:

```bash
goldlapel clean
```

Then remove the package and any local state:

```bash
npm uninstall goldlapel
rm -rf ~/.goldlapel
rm -f goldlapel.toml     # only if you wrote one
```

Cancelling your subscription does not delete your data — only Gold Lapel's helper schema and the indexes it created go away.

## License

MIT. See `LICENSE`.
