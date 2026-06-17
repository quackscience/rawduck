<img width="120" alt="rawduck" src="https://github.com/user-attachments/assets/e44ee764-7639-433c-b904-03a2d4ee38e2" /> 

# RawDuck

**Schema-less JSON analytics for DuckDB, RawMergeTree style**

RawDuck brings the RawMergeTree *"ingest first, schema later"* model to DuckDB: point raw JSON,
NDJSON files, or OTLP telemetry at tables that don't exist yet — RawDuck creates them, types them,
flattens nested objects into real columns, transforms and evolves the schema as the data changes. 

### ⚡ Benefits
No `CREATE TABLE`, no schema declarations, no `json_extract` at query time. Because data lands
shredded into native typed columns instead of opaque JSON strings, analytical queries run
**15–38× faster** on telemetry queries, **3.5× smaller** on disk — see benchmark.

### ⚙️ Under the hood
RawDuck delivers a complete engine rather than a parser: ingestion is transactional, pipelined, and
multi-threaded through DuckDB's own catalog and storage APIs (`BEGIN`/`ROLLBACK`) and the optimizer 
observes the workload and adapts — physically re-sorting tables by the columns queries actually 
filter on (incrementally, MergeTree-parts style) and answering recurring aggregations from projections. 

## Usage

Attach a store and `INSERT` raw JSON — tables, typed columns, and schema all emerge from the data:

```sql
ATTACH 'rawduck:store.db' AS raw;

-- no table 'events' exists yet
INSERT INTO raw.ingest.events VALUES
    ('{"id": 1, "action": "click", "ts": "2024-01-15T10:30:00", "user": {"name": "alice", "plan": "pro"}}'),
    ('{"id": 2, "action": "view",  "ts": "2024-01-15T10:31:00", "user": {"name": "bob"}}');

DESCRIBE raw.events;
-- id BIGINT, action VARCHAR, ts TIMESTAMP, user.name VARCHAR, user.plan VARCHAR

SELECT "user.name", count(*) FROM raw.events GROUP BY 1;
```

The `ingest` schema accepts any SQL source through a fully parallel zero-copy sink
(**6.1M rows/s** on narrow JSON; a 956 MB heterogeneous NDJSON file in ~6 s):

```sql
INSERT INTO raw.ingest.events SELECT json FROM read_json('events.ndjson',
    format='newline_delimited', records='false', columns={json: 'JSON'});

CALL raw_ingest_file('raw.events', 'events.ndjson.gz');   -- or the one-call file loader
```

Ingest a different shape and the table follows the data: new keys become columns, conflicting
types widen, missing keys read as `NULL` — nothing is ever dropped. And RawMergeTree tables stay
regular DuckDB tables, so every statement and tool works at native speed:

```sql
UPDATE raw.events SET "user.plan" = 'enterprise' WHERE id = 1;
CREATE TABLE raw.daily AS SELECT date_trunc('day', ts) AS day, count(*) FROM raw.events GROUP BY 1;
```

For ingestion outside a RawDuck store (the default in-memory catalog, DuckLake, the async buffer),
`CALL raw_ingest('table', payload)` is the equivalent with the same engine underneath. All RawDuck
commands are table functions: invoke them with `CALL`, or use `SELECT ... FROM fn(...)` when you
want to project or filter their result columns.

## Benchmark: OTEL at line speed

Real OTLP/JSON export envelopes — logs, metrics, traces — shredded into typed columns on
ingest. Apple Silicon, DuckDB v1.5.3, 1M records per signal:

| signal | ingest | records/s | on disk |
|---|---:|---:|---:|
| traces | 1.65 s | **604k** | 72 MB |
| logs | 1.71 s | **586k** | 61 MB |
| metrics | 1.30 s | **771k** | 56 MB |

**3M telemetry records in 4.7 s.** Queries on shredded spans run **15–38× faster** than a JSON
column with identical results; storage is **3.5× smaller**. One call handles envelope explode,
KeyValue attribute flattening, and byte-id normalization — no schema upfront:

```sql
CALL raw_ingest_file('traces', 'export.ndjson', transform := 'otlp-traces');

SELECT "resource.service.name", count(*) FROM traces
WHERE "http.status_code" >= 500 GROUP BY 1;   -- 3 ms
```

As a wide-schema stress test, one hour of [GH Archive](https://www.gharchive.org/) data (914
columns, 247k events) still lands in ~13 s with **45–265×** query speedup over JSON columns.
Full methodology, query suites, and reproduction steps: [BENCHMARK.md](BENCHMARK.md).

## Functions

| Function | Kind | Description |
|---|---|---|
| `INSERT INTO <store>.ingest.<table> ...` | SQL | The primary lane: any VALUES or SELECT source streams through a parallel zero-copy sink into `<table>`, auto-creation and evolution included. |
| `raw_ingest(table, payload)` | table | Schema-less ingest: auto-creates the table, adds new columns, widens conflicting types, appends — natively, inside your transaction. Accepts a JSON array, a single object, scalars, or NDJSON. Returns `(table, created, columns_added, columns_widened, rows, errors)`. |
| `raw_ingest_file(table, path, batch_size := 30000)` | table | Streaming ingest of NDJSON files (gzip auto-detected, any DuckDB filesystem) in bounded-memory batches, evolving the schema between batches. The whole file is one atomic operation. |
| `raw_records(payload)` | table | Parse + infer + flatten a JSON payload into typed rows without touching any table. |
| `raw_stats()` | table | Observed usage statistics per column: pushed-down filters and GROUP BY keys, collected automatically by an optimizer hook. |
| `raw_optimize(table)` | table | RawMergeTree adaptive layout: physically reorders the table by its hottest columns. Incremental: append-only growth since the last optimize sorts only the new tail into a fresh sorted run (`mode` = `full` / `incremental` / `noop`). |
| `raw_transforms()` / `raw_transform_define(name, path)` | table / scalar | List and register ingest-time transforms; definitions compose with `read_json`, tables, or any query. |
| `raw_stats_save(catalog?)` / `raw_stats_load(catalog?)` | table | Persist observed statistics into a store (`__rawduck_stats` table) and merge them back after restart. |
| `raw_projections()` | table | The projection advisor: GROUP BY shapes queries actually run, with observation counts and materialization status. |
| `raw_project(table)` | table | RawMergeTree auto-projections: materializes the hottest observed aggregation as a lightweight `<table>__proj` summary table. |
| `raw_serve(host, port, token)` / `raw_serve_stop()` | table | Start/stop the in-process HTTP API (see below). |
| `ATTACH 'rawduck:quack:host:port' AS … (TOKEN '…')` | SQL | Remote RawDuck store over Quack (requires `quack` on client and RawDuck on server). |
| `raw_serve_grpc(host, port, token)` / `raw_serve_grpc_stop()` | table | Start/stop the OTLP/gRPC collector (opt-in build, see Building). |
| `raw_flush()` | table | Synchronously drain the async-insert buffers. |
| `raw_type(json)` | scalar | Concrete type of a JSON value (RawMergeTree's `dynamicType()`): `Null`, `Bool`, `Int64`, `UInt64`, `Double`, `String`, `Array`, `Object`. |
| `raw_infer(json)` | scalar | The DuckDB type RawDuck assigns to a value, e.g. `BIGINT`, `DOUBLE[]`, or the flattened layout for objects: `OBJECT(a BIGINT, b.c VARCHAR)`. |

All ingest functions accept `transform := '...'`, `explode := '...'` and `ignore_errors := true`.

## Asynchronous inserts

By default every `raw_ingest` call parses, evolves the schema, appends, and commits before
returning — callers immediately see their rows. Under many concurrent writers issuing small
payloads, that means one transaction per call. Asynchronous mode trades immediate visibility for
throughput: calls enqueue the payload into a per-table buffer and return instantly, and a
background flusher ingests each buffer as a single batch.

```sql
SET rawduck_async_insert = true;

CALL raw_ingest('events', '[{"id": 1, "action": "click"}]');  -- returns immediately, rows = 0
CALL raw_ingest('events', '[{"id": 2, "action": "view"}]');

-- a buffer flushes when it exceeds the size threshold or its oldest entry exceeds the age
-- threshold; force it when you need the data now:
CALL raw_flush();
-- ┌─────────┬──────┐
-- │ targets │ rows │
-- │       1 │    2 │
-- └─────────┴──────┘

SELECT count(*) FROM events;   -- 2
```

| Setting / function | Default | Meaning |
|---|---|---|
| `rawduck_async_insert` | `false` | Enable buffered ingestion for `raw_ingest` / `raw_ingest_file`. |
| `rawduck_async_max_data_size` | `1048576` | Flush a table's buffer once it holds this many bytes. |
| `rawduck_async_busy_timeout_ms` | `200` | Flush a buffer once its oldest payload is this old. |
| `raw_flush()` | — | Drain all buffers synchronously; returns `(targets, rows)`. |

Semantics to know before enabling it:

- Buffered payloads commit in the flusher's own transactions — a `ROLLBACK` in the calling
  session does not un-enqueue them, and a failed background flush drops that batch.
- Data buffered for less than the age threshold is lost if the database closes first; call
  `raw_flush()` before shutdown.
- The HTTP and gRPC servers ingest **synchronously by default**: a row is queryable the moment
  the insert call returns, which is what insert-then-query clients expect. Pass `async := true`
  to opt into buffered ingestion when the workload is many concurrent fire-and-forget producers
  (a single flusher then also serializes schema evolution instead of letting per-request
  transactions race on it); call `raw_flush()` to drain before reading.

## HTTP API

RawDuck can serve an in-process HTTP API for ingestion and querying

```sql
CALL raw_serve(host := '127.0.0.1', port := 9999, token := 'rt_secret');
CALL raw_serve_stop();
```

```sh
curl -X POST localhost:9999/v1/tables/events -H "Authorization: Bearer rt_secret" \
     -d '[{"action":"click","user":"alice","value":42}]'
# {"inserted":1}

curl -X POST localhost:9999/v1/query -H "Authorization: Bearer rt_secret" \
     -d '{"sql":"SELECT action, count(*) FROM events GROUP BY action"}'
# {"meta":[...],"data":[{"action":"click","count_star":1}],"rows":1,"statistics":{"elapsed":0.0016,"rows_read":1,"bytes_read":16},"hints":[]}
```

| Endpoint | Behavior |
|---|---|
| `GET /health` | `{"status":"ok"}` |
| `POST /v1/query` | `{"sql": "..."}` → `meta` / `data` / `rows` / `statistics` |
| `GET /v1/tables`, `GET /v1/tables/{t}` | list tables / describe schema |
| `POST /v1/tables/{t}` | schema-less ingest (`?transform=`, `?explode=`, `?ignore_errors=true`) |
| `DELETE /v1/tables/{t}` | drop table |
| `POST /otlp/v1/{traces,logs,metrics}` | OTLP/HTTP ingest (JSON or protobuf bodies) with envelope unwrapping and spec-shaped `partialSuccess` responses |

Requests run on their own connections/transactions; a bearer `token` guards everything except
`/health`; CORS is enabled for browser clients; gzip is supported both ways (request bodies with
`Content-Encoding: gzip`, compressed responses for `Accept-Encoding: gzip` clients). Binds to
localhost by default.

### OpenTelemetry SDKs

The OTLP routes follow the standard signal paths and accept both wire encodings — `http/protobuf`
(the SDK default) and `http/json` — so SDKs only need the endpoint base:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:9999/otlp
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer rt_secret"
```

Signals land in `otel_traces`, `otel_logs`, and `otel_metrics` by default; route them to custom
tables with the `x-rawduck-traces-table`, `x-rawduck-logs-table`, or `x-rawduck-metrics-table`
headers (the generic `x-rawduck-table` also works). Responses are OTLP-conformant in the request's
encoding: an empty `partialSuccess` on full acceptance, signal-specific rejected counts otherwise.
Both encodings produce identical columns — trace/span ids stored as hex, enum fields as integers,
`*UnixNano` timestamps as `BIGINT` — so mixed fleets of exporters share tables cleanly.

### OTLP/gRPC

Builds made with `make release RAWDUCK_ENABLE_GRPC=1` (see Building) also serve the standard
OpenTelemetry collector services natively:

```sql
CALL raw_serve_grpc(port := 4317, token := 'rt_secret');   -- TraceService/LogsService/MetricsService
CALL raw_serve_grpc_stop();
```

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
export OTEL_EXPORTER_OTLP_HEADERS="authorization=Bearer rt_secret"
```

Requests are converted through protobuf's canonical OTLP/JSON mapping and flow through the same
native ingestion path as the HTTP routes, with the same `x-rawduck-*-table` routing (sent as gRPC
metadata) and `partialSuccess` semantics. On builds without gRPC the functions explain themselves;
OTLP/HTTP (both encodings) stays fully functional.

## ATTACH: RawMergeTree stores

```sql
ATTACH 'rawduck:store.db' AS raw;
```

A RawDuck store is a native DuckDB database under a RawDuck-typed catalog: everything DuckDB can
do — joins, window functions, updates, exports, other extensions — works on RawMergeTree tables
transparently and at full native speed, while the store identifies itself for RawDuck's ingestion
and adaptive-layout machinery. Stores persist and reattach like any database file.

Two kinds of `INSERT` coexist: typed inserts into the real tables behave exactly like DuckDB
(fixed columns, binder-validated), while inserts into the virtual `ingest` schema take raw JSON
payloads and handle creation and evolution. Both run in your transaction.

### Remote stores via Quack

Attach a RawDuck server over the [Quack](https://github.com/duckdb/duckdb-quack) RPC protocol.
The **server** must run DuckDB with RawDuck loaded (ingestion, schema evolution, and the ingest
lane live on the server). The **client** needs both extensions:

```sql
INSTALL quack; LOAD quack; LOAD rawduck;
ATTACH 'rawduck:quack:127.0.0.1:19920' AS raw (TOKEN 'secret');
```

| Attach URI | Catalog type | `raw.ingest.*` | Typical use |
|---|---|---|---|
| `rawduck:store.db` | `rawduck` | yes | local on-disk store |
| `rawduck:quack:host:port` | `rawduck` | yes | remote RawDuck over Quack |
| `quack:host:port` | `quack` | no | remote reads / typed DML only |

On the server, start a Quack listener and ingest as usual:

```sql
LOAD rawduck; LOAD quack;
SELECT * FROM quack_serve('quack:127.0.0.1:19920', token := 'secret');
CALL raw_ingest('events', '[{"action":"click"}]');
```

From the client, queries and the ingest lane work like a local attach:

```sql
SELECT * FROM raw.events;
INSERT INTO raw.ingest.events VALUES ('{"action":"view","user":"bob"}');
INSERT INTO raw.ingest.events SELECT json::VARCHAR FROM read_json('batch.ndjson',
    format='newline_delimited', records='false', columns={json: 'JSON'});
```

`rawduck:quack:` delegates table scans and DML to Quack while preserving RawDuck catalog
identity — including `PlanInsert` routing for the virtual `ingest` schema (remote `raw_ingest`
under the hood). Plain `ATTACH 'quack:…'` is fine for ad-hoc reads or `quack_query`, but does
not expose `raw.ingest.*`. For stateless one-off SQL, `quack_query('quack:host:port', $$…$$,
token := 'secret')` works without attaching.

## Transforms

RawDuck reshapes envelope-style telemetry at ingest time: one row per nested event,
with the wrapper's fields merged into each row.

```sql
-- {"owner":"123","logGroup":"/aws/lambda/api","logEvents":[{"id":"1","message":"started"},...]}
CALL raw_ingest('logs', payload, transform := 'cloudwatch-logs');
-- one row per log event, with owner and logGroup columns on each

-- the generic form works for any envelope shape
CALL raw_ingest('events', payload, explode := 'batch.items');
```

Transforms also apply to the INSERT lane through a session setting (a transform name or a
dotted explode path):

```sql
SET rawduck_insert_transform = 'otlp-traces';
INSERT INTO raw.ingest.spans SELECT json FROM read_json('traces.ndjson', ...);
RESET rawduck_insert_transform;
```

Built-in transforms: `cloudwatch-logs`, `cloudtrail`, `firehose`, `otlp-traces`, `otlp-logs`,
`otlp-metrics` (multi-level envelopes like `resourceSpans[].scopeSpans[].spans[]` are unwrapped
with resource/scope fields merged into every row). Transforms are user-extensible — definitions
are data, so they load from files or tables like anything else in DuckDB:

```sql
SELECT raw_transform_define('my-batch', 'data.items');                          -- one-off
SELECT raw_transform_define(name, explode) FROM read_json('transforms.json');   -- from a file
SELECT raw_transform_define(name, explode) FROM raw.transform_config;           -- from a table
CALL raw_transforms();                                                 -- list them all
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

CALL raw_stats();
-- gh_events | type    | 2
-- gh_events | repo.id | 1

CALL raw_optimize('gh_events');
-- gh_events | "type", "repo.id" | 247199

SELECT type, count(*) FROM gh_events GROUP BY type;        -- observed by the advisor
CALL raw_project('gh_events');
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
CALL raw_ingest('lake.main.events', payload);
```

Catalogs that cannot rewrite columns with expressions (DuckLake rejects `ALTER ... USING`)
degrade gracefully: RawDuck keeps the existing column type and converts incoming values instead.

## Building

```sh
git submodule update --init
GEN=ninja make release
```

OTLP/HTTP protobuf decoding is **on by default**: builds pick up protobuf from the vcpkg
manifest's default `protobuf` feature (or a system package locally) and skip it gracefully when
unavailable (wasm builds, or `make release RAWDUCK_DISABLE_OTLP_PROTOBUF=1`); without it, protobuf
bodies get a 415 pointing at `http/json`.

The OTLP/gRPC server is **opt-in at build time** (it pulls the full gRPC stack, which
significantly lengthens builds): `make release RAWDUCK_ENABLE_GRPC=1` enables it, using the
`grpc` vcpkg manifest feature in CI or system gRPC/protobuf locally. Default builds skip it —
OTLP/HTTP stays fully functional and `raw_serve_grpc()` explains how to enable support. The flags
are cached per build directory; run `make clean` when toggling them. wasm builds never include them.

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
error-tolerant ingestion, RawDuck stores (`ATTACH 'rawduck:...'` and `ATTACH 'rawduck:quack:...'`),
Quack remote attach (`test/sql/raw_quack.test`), transactional rollback,
predicate statistics + adaptive reordering, and DuckLake catalogs (`test/sql/ducklake.test`).

## Status

All RawMergeTree concepts are implemented: schema-less evolving ingestion
(native, transactional, pipelined, multi-threaded), adaptive physical layout from observed
predicates with incremental re-sorting, the projection advisor with automatic aggregate rewriting,
extensible ingest-time transforms, persisted statistics, RawDuck stores, Quack remote attach,
DuckLake fallback, and an in-process HTTP API for ingestion and querying.

See [BENCHMARK.md](BENCHMARK.md) to reproduce the numbers and [AGENTS.md](AGENTS.md) for the
design guide.

---

Based on the [DuckDB extension template](https://github.com/duckdb/extension-template).
JSON parsing via DuckDB's vendored [yyjson](https://github.com/ibireme/yyjson).
