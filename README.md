# goldlapel

[![Tests](https://github.com/goldlapel/goldlapel-js/actions/workflows/test.yml/badge.svg)](https://github.com/goldlapel/goldlapel-js/actions/workflows/test.yml)

The Node.js wrapper for [Gold Lapel](https://goldlapel.com) — a self-optimizing Postgres proxy that watches query patterns and creates materialized views + indexes automatically. Zero code changes beyond the connection string.

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

Point your Postgres driver at `gl.url`. Gold Lapel sits between your app and your DB, watching query patterns and creating materialized views + indexes automatically. Zero code changes beyond the connection string.

`await using` auto-cleanup, scoped connections via `gl.using(conn, cb)`, driver auto-detection, and framework integrations are in the docs.

## Dashboard

Gold Lapel exposes a live dashboard at `gl.dashboardUrl`:

```js
console.log(gl.dashboardUrl);
// -> http://127.0.0.1:7933
```

## Documentation

Full API reference, configuration, framework integrations (Prisma, Drizzle, Next.js, SvelteKit, Express), upgrading from v0.1, and production deployment: https://goldlapel.com/docs/javascript

## License

MIT. See `LICENSE`.
