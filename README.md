<img width="120" alt="rawduck" src="https://github.com/user-attachments/assets/e44ee764-7639-433c-b904-03a2d4ee38e2" /> 

# RawDuck

**Schema-less JSON analytics for DuckDB, RawMergeTree style**

RawDuck brings the RawMergeTree *"ingest first, schema later"* model to DuckDB: point raw JSON,
NDJSON files, or OTLP telemetry at tables that don't exist yet — RawDuck creates them, types them,
flattens nested objects into real columns, transforms and evolves the schema as the data changes. 

### ⚡ Benefits
No `CREATE TABLE`, no schema declarations, no `json_extract` at query time. Because data lands
shredded into native typed columns instead of opaque JSON strings, analytical queries run
**45–265× faster** on **40% smaller** than the JSON-column approach (see benchmark).

### ⚙️ Under the hood
RawDuck delivers a complete engine rather than a parser: ingestion is transactional, pipelined, and
multi-threaded through DuckDB's own catalog and storage APIs (`BEGIN`/`ROLLBACK`) and the optimizer 
observes the workload and adapts — physically re-sorting tables by the columns queries actually 
filter on (incrementally, MergeTree-parts style) and answering recurring aggregations from projections. 

## Usage

```sql
ATTACH 'rawduck:store.db' AS raw;

-- no table 'events' exists yet
SELECT * FROM raw_ingest('raw.events', '[
    {"id": 1, "action": "click", "ts": "2024-01-15T10:30:00", "user": {"name": "alice", "plan": "pro"}},
    {"id": 2, "action": "view",  "ts": "2024-01-15T10:31:00", "user": {"name": "bob"}}
]');

DESCRIBE raw.events;
-- id BIGINT, action VARCHAR, ts TIMESTAMP, user.name VARCHAR, user.plan VARCHAR
```

RawMergeTree tables are regular DuckDB tables — every query, statement, and tool works
transparently at native speed:

```sql
SELECT "user.name", count(*) FROM raw.events GROUP BY 1;
INSERT INTO raw.events VALUES (3, 'buy', now(), 'carol', 'pro');
UPDATE raw.events SET "user.plan" = 'enterprise' WHERE id = 1;
CREATE TABLE raw.daily AS SELECT date_trunc('day', ts) AS day, count(*) FROM raw.events GROUP BY 1;
```

Ingest again with a different shape and the table follows the data: new keys become columns,
conflicting types widen, missing keys read as `NULL` — nothing is ever dropped.

## Benchmark: one hour of GitHub, three ways

Real [GH Archive](https://www.gharchive.org/) data — **247,199 GitHub events, 956 MB of NDJSON,
wildly heterogeneous payloads** (the dataset RawBench uses). One `raw_ingest_file` call shredded it
into **914 typed columns**, schema evolution included. The baseline is the standard DuckDB JSON
extension pattern: a single `JSON` column queried with `->>` paths.

Same machine (Apple Silicon, DuckDB v1.5.3), best of 3:

| RawBench-style query | JSON column (`->>`) | RawDuck typed columns | speedup |
|---|---:|---:|---:|
| count by event type | 231 ms | 1 ms | **231×** |
| top repos by pushes | 268 ms | 3 ms | **89×** |
| distinct repos per actor | 457 ms | 10 ms | **46×** |
| sum of push payload sizes | 265 ms | 1 ms | **265×** |
| events per minute | 236 ms | 3 ms | **79×** |
| *all five combined* | *1.46 s* | *18 ms* | **~80×** |

| | JSON column | RawDuck |
|---|---:|---:|
| ingest (full hour, 956 MB) | 1.4 s | **9.8 s** |
| storage on disk | 1.05 GB | **627 MB** |

Ingest runs at ~25,000 events/s (~98 MB/s), with parsing pipelined on a background thread — a one-time cost within an order of magnitude of
loading opaque JSON strings, in exchange for every later query being 45–265× faster and the data
40% smaller on disk.

```sql
SELECT * FROM raw_ingest_file('gh_events', '/data/2024-01-15-10.json.gz');   -- 9.8s, 914 columns

SELECT type, count(*) FROM gh_events GROUP BY type ORDER BY 2 DESC;          -- 1 ms
SELECT "repo.name", count(*) AS pushes FROM gh_events
WHERE type = 'PushEvent' GROUP BY 1 ORDER BY pushes DESC LIMIT 10;           -- 3 ms
```

## Functions

| Function | Kind | Description |
|---|---|---|
| `raw_ingest(table, payload)` | table | Schema-less ingest: auto-creates the table, adds new columns, widens conflicting types, appends — natively, inside your transaction. Accepts a JSON array, a single object, scalars, or NDJSON. Returns `(table, created, columns_added, columns_widened, rows, errors)`. |
| `raw_ingest_file(table, path, batch_size := 30000)` | table | Streaming ingest of NDJSON files (gzip auto-detected, any DuckDB filesystem) in bounded-memory batches, evolving the schema between batches. The whole file is one atomic operation. |
| `raw_records(payload)` | table | Parse + infer + flatten a JSON payload into typed rows without touching any table. |
| `raw_stats()` | table | Observed usage statistics per column: pushed-down filters and GROUP BY keys, collected automatically by an optimizer hook. |
| `raw_optimize(table)` | table | RawMergeTree-style adaptive layout: physically reorders the table by its hottest columns. Incremental: append-only growth since the last optimize sorts only the new tail into a fresh sorted run (`mode` = `full` / `incremental` / `noop`). |
| `raw_transforms()` / `raw_transform_define(name, path)` | table / scalar | List and register ingest-time transforms; definitions compose with `read_json`, tables, or any query. |
| `raw_stats_save(catalog?)` / `raw_stats_load(catalog?)` | table | Persist observed statistics into a store (`__rawduck_stats` table) and merge them back after restart. |
| `raw_projections()` | table | The projection advisor: GROUP BY shapes queries actually run, with observation counts and materialization status. |
| `raw_project(table)` | table | RawMergeTree auto-projections: materializes the hottest observed aggregation as a lightweight `<table>__proj` summary table. |
| `raw_serve(host, port, token)` / `raw_serve_stop()` | table | Start/stop the in-process HTTP API (see below). |
| `raw_type(json)` | scalar | Concrete type of a JSON value (RawMergeTree's `dynamicType()`): `Null`, `Bool`, `Int64`, `UInt64`, `Double`, `String`, `Array`, `Object`. |
| `raw_infer(json)` | scalar | The DuckDB type RawDuck assigns to a value, e.g. `BIGINT`, `DOUBLE[]`, or the flattened layout for objects: `OBJECT(a BIGINT, b.c VARCHAR)`. |

All ingest functions accept `transform := '...'`, `explode := '...'` and `ignore_errors := true`.

## HTTP API

RawDuck can serve an in-process HTTP API for RawMergeTree-style ingestion and querying

```sql
SELECT * FROM raw_serve(host := '127.0.0.1', port := 9999, token := 'rt_secret');
SELECT * FROM raw_serve_stop();
```

```sh
curl -X POST localhost:9999/v1/tables/events -H "Authorization: Bearer rt_secret" \
     -d '[{"action":"click","user":"alice","value":42}]'
# {"table":"events","inserted":1,"created":true,"columns_added":3,"errors":0}

curl -X POST localhost:9999/v1/query -H "Authorization: Bearer rt_secret" \
     -d '{"sql":"SELECT action, count(*) FROM events GROUP BY action"}'
# {"meta":[...],"data":[["click",1]],"rows":1,"statistics":{"elapsed":0.0016}}
```

| Endpoint | Behavior |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `POST /v1/query` | `{"sql": "..."}` → `meta` / `data` / `rows` / `statistics` |
| `GET /v1/tables`, `GET /v1/tables/{t}` | list tables / describe schema |
| `POST /v1/tables/{t}` | schema-less ingest (`?transform=`, `?explode=`, `?ignore_errors=true`) |
| `DELETE /v1/tables/{t}` | drop table |
| `POST /otlp/v1/{traces,logs,metrics}` | OTLP/HTTP JSON ingest with envelope unwrapping and spec-shaped `partialSuccess` responses |

Requests run on their own connections/transactions; a bearer `token` guards everything except
`/health`; CORS is enabled for browser clients. Binds to localhost by default.

### OpenTelemetry SDKs

The OTLP routes follow the standard signal paths, so SDKs only need the endpoint base and the
JSON encoding:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:9999/otlp
export OTEL_EXPORTER_OTLP_PROTOCOL=http/json
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer rt_secret"
```

Signals land in `otel_traces`, `otel_logs`, and `otel_metrics` by default; route them to custom
tables with the `x-rawduck-traces-table`, `x-rawduck-logs-table`, or `x-rawduck-metrics-table`
headers (the generic `x-rawduck-table` also works). Responses are OTLP-conformant: an empty
`partialSuccess` on full acceptance, with signal-specific rejected counts otherwise.

### OTLP/gRPC

Builds with gRPC available (vcpkg in CI, system packages locally; not wasm) also serve the
standard OpenTelemetry collector services natively:

```sql
SELECT * FROM raw_serve_grpc(port := 4317, token := 'rt_secret');   -- TraceService/LogsService/MetricsService
SELECT * FROM raw_serve_grpc_stop();
```

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer rt_secret"
```

Requests are converted through protobuf's canonical OTLP/JSON mapping and flow through the same
native ingestion path as the HTTP routes, with the same `x-rawduck-*-table` routing (sent as gRPC
metadata) and `partialSuccess` semantics. On builds without gRPC the functions explain themselves;
OTLP/HTTP protobuf bodies still get a 415 pointing at `http/json` or the gRPC endpoint.

## ATTACH: RawMergeTree stores

```sql
ATTACH 'rawduck:store.db' AS raw;
```

A RawDuck store is a native DuckDB database under a RawDuck-typed catalog: everything DuckDB can
do — joins, window functions, updates, exports, other extensions — works on RawMergeTree tables
transparently and at full native speed, while the store identifies itself for RawDuck's ingestion
and adaptive-layout machinery. Stores persist and reattach like any database file.

Note on transparency limits: DuckDB's binder resolves `INSERT` column lists against the current
schema, so a plain `INSERT` cannot create tables or add columns (no catalog can change that —
binding happens before any catalog hook). Schema-less creation and evolution flow through the
ingest functions, which run in the same transaction as the rest of your statements.

## Transforms

RawDuck reshapes envelope-style telemetry at ingest time: one row per nested event,
with the wrapper's fields merged into each row.

```sql
-- {"owner":"123","logGroup":"/aws/lambda/api","logEvents":[{"id":"1","message":"started"},...]}
SELECT * FROM raw_ingest('logs', payload, transform := 'cloudwatch-logs');
-- one row per log event, with owner and logGroup columns on each

-- the generic form works for any envelope shape
SELECT * FROM raw_ingest('events', payload, explode := 'batch.items');
```

Built-in transforms: `cloudwatch-logs`, `cloudtrail`, `firehose`, `otlp-traces`, `otlp-logs`,
`otlp-metrics` (multi-level envelopes like `resourceSpans[].scopeSpans[].spans[]` are unwrapped
with resource/scope fields merged into every row). Transforms are user-extensible — definitions
are data, so they load from files or tables like anything else in DuckDB:

```sql
SELECT raw_transform_define('my-batch', 'data.items');                          -- one-off
SELECT raw_transform_define(name, explode) FROM read_json('transforms.json');   -- from a file
SELECT raw_transform_define(name, explode) FROM raw.transform_config;           -- from a table
SELECT * FROM raw_transforms();                                                 -- list them all
```

Dirty NDJSON streams can be ingested with `ignore_errors := true`; skipped lines are counted in the `errors` column.

## The type lattice

Per JSON path, RawDuck infers the narrowest type that holds everything seen, widening monotonically
as data arrives (existing columns are `ALTER`ed in place, never rewritten destructively):

```
            BOOLEAN   BIGINT ──> DOUBLE     DATE ──> TIMESTAMP
                │        │          │          │          │
                └────────┴────> VARCHAR <──────┴──────────┘      (scalar conflicts)

            object vs scalar, mixed arrays, arrays of objects ──> JSON   (structural conflicts)
```

- integers out of `BIGINT` range degrade to `DOUBLE`
- ISO `DATE` / `TIMESTAMP` strings are sniffed into temporal columns
- homogeneous scalar arrays become typed `LIST`s (`BIGINT[]`, nested `BIGINT[][]`, …)
- nothing is ever dropped: structurally conflicting values are preserved verbatim as `JSON`

## Adaptive layout from observed workloads

An optimizer hook records every filter DuckDB pushes into a table scan and every GROUP BY
column set. `raw_optimize` turns filter/group usage into a physical sort order
(RawMergeTree adaptive primary keys); `raw_project` materializes the hottest aggregation
as a summary table (RawMergeTree lightweight projections):

```sql
SELECT count(*) FROM gh_events WHERE type = 'PushEvent';
SELECT sum("payload.size") FROM gh_events WHERE type = 'PushEvent' AND "repo.id" > 700000000;

SELECT * FROM raw_stats();
-- gh_events | type    | 2
-- gh_events | repo.id | 1

SELECT * FROM raw_optimize('gh_events');
-- gh_events | "type", "repo.id" | 247199

SELECT type, count(*) FROM gh_events GROUP BY type;        -- observed by the advisor
SELECT * FROM raw_project('gh_events');
-- gh_events | gh_events__proj | type | 15                 -- pre-aggregated summary table

SET rawduck_use_projections = true;
SELECT type, count(*) FROM gh_events GROUP BY type;        -- now answered from the projection
```

With `rawduck_use_projections` enabled (off by default), eligible `count(*)` aggregations are
rewritten onto fresh projections transparently — result types and values are identical, and a
physical-row-count staleness token guarantees a changed base table always falls back to a full
scan. Intended for append-only analytics; in-place `UPDATE`s of group columns require re-running
`raw_project`. Statistics persist across sessions with `raw_stats_save('store')` /
`raw_stats_load('store')`.

## DuckLake as a backend

For non-native catalogs RawDuck falls back to catalog-level SQL, so schema-less ingestion also
works against [DuckLake](https://ducklake.select) — straight into a lakehouse with snapshots and
schema evolution tracked in the metadata:

```sql
ATTACH 'ducklake:metadata.ducklake' AS lake (DATA_PATH 's3://bucket/raw');
SELECT * FROM raw_ingest('lake.main.events', payload);
```

Catalogs that cannot rewrite columns with expressions (DuckLake rejects `ALTER ... USING`)
degrade gracefully: RawDuck keeps the existing column type and converts incoming values instead.

## Building

```sh
git submodule update --init
GEN=ninja make release
```

The OTLP/gRPC server compiles in automatically when gRPC and protobuf are available (vcpkg in CI,
system packages locally) and is always skipped for wasm. To build without it even when available
(OTLP/HTTP keeps working): `make release RAWDUCK_DISABLE_GRPC=1` (the flag is cached per build
directory; run `make clean` when toggling it).

Artifacts:

```sh
./build/release/duckdb                                          # shell with rawduck linked in
./build/release/test/unittest                                   # test runner
./build/release/extension/rawduck/rawduck.duckdb_extension      # loadable extension
```

## Tests

```sh
make test
```

The sqllogictests in `test/sql/` cover all standard JSON types, nested flattening, NDJSON, type
widening, schema evolution, structural conflicts, streaming file ingestion, multi-threaded appends, transforms, projections,
error-tolerant ingestion, RawDuck stores (`ATTACH 'rawduck:...'`), transactional rollback,
predicate statistics + adaptive reordering, and DuckLake catalogs (`test/sql/ducklake.test`).

## Status

All RawMergeTree concepts are implemented: schema-less evolving ingestion
(native, transactional, pipelined, multi-threaded), adaptive physical layout from observed
predicates with incremental re-sorting, the projection advisor with automatic aggregate rewriting,
extensible ingest-time transforms, persisted statistics, RawDuck stores, DuckLake fallback, and
an in-process HTTP API for ingestion and querying.

See [BENCHMARK.md](BENCHMARK.md) to reproduce the numbers and [AGENTS.md](AGENTS.md) for the
design guide.

---

Based on the [DuckDB extension template](https://github.com/duckdb/extension-template).
JSON parsing via DuckDB's vendored [yyjson](https://github.com/ibireme/yyjson).
