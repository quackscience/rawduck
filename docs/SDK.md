# TypeScript SDK + RawDuck

Use [`@rawtree/sdk`](https://www.npmjs.com/package/@rawtree/sdk) against RawDuck's HTTP API (`raw_serve`). Point the client at your local server with `baseUrl` — insert, query, and table metadata responses match the public API shape.

**Requirements:** DuckDB 1.5.3 or higher with the RawDuck extension loaded, Node.js with `fetch` (≥ 18).

## Setup

**1. Start the API** (keep this session open):

```sh
duckdb my.db
```

```sql
LOAD rawduck;
SELECT * FROM raw_serve(
    host  := '127.0.0.1',
    port  := 9999,
    token := 'rt_secret'
);
```

Ingestion is synchronous by default, so a row is queryable the moment `insert`
returns — exactly what insert-then-query scripts need. Pass `async := true`
only for high-rate fire-and-forget producers (then call `raw_flush()` before
reading).

**2. Create a client** — `baseUrl` is required; the default is the hosted API URL. `apiKey` must match `token` above.

```ts
import { RawTree } from "@rawtree/sdk";

const rawtree = new RawTree({
  apiKey: process.env.RAWTREE_API_KEY!,
  baseUrl: "http://127.0.0.1:9999",
});
```

Stop the server with `SELECT * FROM raw_serve_stop();`.

## API surface

| Method | RawDuck |
|---|---|
| `insert(table, rows)` | ✅ `POST /v1/tables/{table}` |
| `query(sql)` | ✅ `POST /v1/query` |
| `tables.list()` | ✅ `GET /v1/tables` |
| `tables.describe(table)` | ✅ `GET /v1/tables/{table}` |
| Table delete | ❌ not in SDK — use `DELETE /v1/tables/{table}` (curl) |
| `GET /health` | ❌ not wrapped — call `fetch` directly |

## Usage

```ts
await rawtree.insert("events", [
  { event: "signup", user_id: "u_123" },
]);

const result = await rawtree.query<{ event: string; count: number }>(
  "SELECT event, count() AS count FROM events GROUP BY event",
);

const tables = await rawtree.tables.list();
const schema = await rawtree.tables.describe("events");
```

Auth uses `Authorization: Bearer <apiKey>` (same as curl examples in the README).

## Caveats

- **Synchronous by default.** `insert` commits before it returns, so a follow-up `query` / `tables.describe` sees the rows immediately. Only if you start the server with `async := true` does `insert` return `{"queued": true}` and become visible after the background flusher runs (~200 ms) or an explicit `raw_flush()` — same behavior as MCP, see [MCP.md](MCP.md).
- **`query` is not read-only** on RawDuck — any SQL DuckDB accepts will run.
- **`created_at` / `total_bytes`** on list/describe are placeholders (epoch timestamp, estimated bytes from row count). Hosted projects track real creation time and storage size.
- **`project` / `organization`** are always `{ name: "default" }` for local single-tenant databases.
- **Ingest transforms** (`?transform=otlp-traces`, etc.) work on the HTTP query string. OTLP JSON envelopes (`resourceSpans`, `resourceLogs`, `resourceMetrics`) are auto-detected when `transform` is omitted — so `@rawtree/otel` shredding works even though the SDK does not yet append `?transform=`.

## `@rawtree/otel`

[`@rawtree/otel`](https://www.npmjs.com/package/@rawtree/otel) registers a tracer that posts OTLP JSON via `RawTree.insert()`. Against RawDuck, RawDuck auto-detects the OTLP envelope and applies the matching transform, so spans are shredded into trace columns. You can also use the HTTP OTLP routes (`POST /otlp/v1/traces`) or `raw_ingest(..., transform := 'otlp-traces')` in SQL.

```ts
import { registerOTel } from "@rawtree/otel";

const rawtree = registerOTel({
  serviceName: "my-app",
  apiKey: process.env.RAWTREE_API_KEY!,
  baseUrl: "http://127.0.0.1:9999",
});
// ... run app ...
await rawtree.shutdown();
```

## Quick local smoke test

With the API running:

```sh
npm install @rawtree/sdk
```

```js
import { RawTree } from "@rawtree/sdk";

const client = new RawTree({ apiKey: "rt_secret", baseUrl: "http://127.0.0.1:9999" });
await client.insert("events", [{ action: "click" }]);
console.log(await client.tables.describe("events"));
console.log(await client.query("SELECT action FROM events"));
```

HTTP response shapes are also checked by `test/http/raw_api_compat.sh` (curl + python).
