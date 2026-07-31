# RawTree MCP + RawDuck

Use [`@rawtree/mcp`](https://github.com/rawtreedb/rawtree-mcp) as an MCP client for RawDuck's HTTP API (`raw_serve`). Point it at your local server with `--api-url` — the core ingest/query/table tools work; hosted-only tools (logs, API keys, URL ingest) do not.

**Requirements:** DuckDB 1.5.5 or higher with the RawDuck extension loaded, Node.js ≥ 22.

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
    token := 'rt_secret',
    async := false   -- required for MCP: default async queues inserts ~200 ms
);
```

**2. Configure the MCP client** — `--api-url` is required; without it the MCP talks to `api.rawtree.com`. `--api-key` must match the `token` above.

```json
{
  "mcpServers": {
    "rawduck": {
      "command": "npx",
      "args": [
        "-y",
        "@rawtree/mcp",
        "--api-url=http://127.0.0.1:9999",
        "--api-key=rt_secret"
      ]
    }
  }
}
```

Stop the server with `SELECT * FROM raw_serve_stop();`.

## Tools

| Tool | RawDuck |
|---|---|
| `check-health`, `insert-json`, `run-query` | ✅ |
| `list-tables`, `describe-table`, `delete-table` | ✅ |
| `insert-json` with transform | ✅ (`otlp-traces`, `otlp-logs`, `otlp-metrics`, `cloudwatch-logs`, `cloudtrail`, `firehose`) |
| `get_project`, `list-logs`, `list-api-keys`, `create-api-key`, `delete-api-key` | ❌ |
| `insert-from-url` | ❌ — use `insert-json` or SQL (`CALL raw_ingest_file(...)`) |

## Usage

Typical flow in your MCP client:

1. **`insert-json`** — `{"table": "events", "data": [{"action": "click", "user": "alice"}]}`
2. **`describe-table`** — inspect the evolved schema
3. **`run-query`** — `{"sql": "SELECT action, count(*) FROM events GROUP BY action"}`
4. **`delete-table`** — requires `"confirm": true`

## Caveats

- **`async := false` is required for MCP workflows.** With the default `async := true`, inserts return HTTP 202 `{"queued": true}` and data is not visible until the background flusher runs (~200 ms later). An agent calling `insert-json` then immediately `run-query` or `describe-table` will get *table does not exist* errors. With `async := false`, inserts commit synchronously and follow-up tools work instantly.
- **`run-query` is not read-only** on RawDuck — DuckDB accepts any SQL, unlike hosted RawTree.
- **No `RAWTREE_URL` env var** — only the `--api-url` CLI flag overrides the API endpoint.
