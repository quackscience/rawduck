# AGENTS.md — RawDuck design guide

Orientation for LLMs, agents and developers continuing RawDuck. Read this before changing code.

## What RawDuck is

A DuckDB extension implementing the RawMergeTree concept (loosely inspired by the design
described at rawtree.com — keep mentions of that project to this single reference):
**ingest schema-less JSON first, let the schema emerge**. The core bet: eagerly *shred* JSON into
real typed columns at ingest (one-time cost) so every query runs at native columnar speed —
instead of storing opaque JSON strings and paying `->>` extraction on every scan (45–265× slower,
see BENCHMARK.md).

Pinned to DuckDB **v1.5.3**: the `duckdb/` submodule commit and the versions in
`.github/workflows/MainDistributionPipeline.yml` must stay in sync. Build with `GEN=ninja make
release`; test with `./build/release/test/unittest --test-dir . "test/sql/*"`; format with
`make format-fix` (CI enforces it).

## Source map (src/)

| File | Responsibility |
|---|---|
| `raw_json.cpp/.hpp` | **Pure, context-free core**: payload parsing (JSON/NDJSON), schema inference tree (`RawNode`), type lattice, flattening, multi-level explode transform, vectorized extraction (`RawExtractor` + `FillVector`). Never touches `ClientContext` — this is what makes parse threads safe. |
| `raw_records.cpp` | `raw_records()` table function; shared named-parameter handling (`RawBindParseOptions`). |
| `raw_ingest.cpp` | `RawIngestor` (native catalog/storage ingestion + DuckLake SQL fallback), `RawAppendPool` (multi-threaded appends), `RawIngestPipeline` (reader → parse threads → consumer), `raw_ingest()` / `raw_ingest_file()`. The reader closes a batch at whichever comes first: `batch_size` *lines* or `RAW_BATCH_BYTE_TARGET` *bytes* — the byte cap is essential for *container* payloads (OTLP export envelopes, CloudWatch log groups) where one NDJSON line explodes into many records: without it the whole file lands in a single line-count batch and parsing serializes onto one thread. Write-path tuning lives in `raw_write_settings.cpp` (`rawduck_pipeline_threads`, `rawduck_pool_min_rows`, …); parallel ingest consumers default to **1** so wide-schema churn stays serial. |
| `raw_optimize.cpp` | Optimizer hooks (predicate/group observation, projection rewrite), `raw_stats()`, `raw_optimize()`, `raw_projections()`, `raw_project()`, `raw_stats_save/load()`. |
| `raw_transforms.cpp` | User-defined transform registry, `raw_transform_define()` scalar, `raw_transforms()`. |
| `raw_attach.cpp` | `ATTACH 'rawduck:...'` storage extension (`RawDuckCatalog : DuckCatalog`). |
| `raw_scalars.cpp` | `raw_type()`, `raw_infer()`. |
| `raw_async.cpp` | Opt-in async inserts: per-table buffers + background flusher owned by an `ObjectCache` entry (joins on teardown; `weak_ptr` database guard; fire-and-forget semantics — `raw_flush()` is the synchronous drain). |
| `raw_api.cpp` | `raw_serve()` / `raw_serve_stop()`: in-process HTTP API on DuckDB's vendored `duckdb_httplib`. |
| `raw_otlp_pb.cpp` | OTLP/HTTP protobuf bodies: `Export*ServiceRequest` → `MessageToJsonString` → the regular OTLP/JSON ingestion path; protobuf `Export*ServiceResponse` / hand-encoded `google.rpc.Status` replies. Compiles to graceful stubs (HTTP 415) without protobuf (`RAWDUCK_WITH_OTLP_PROTOBUF` unset: wasm, `RAWDUCK_DISABLE_OTLP_PROTOBUF=1`). |
| `raw_grpc.cpp` | Opt-in OTLP/gRPC collector (`RAWDUCK_ENABLE_GRPC=1` builds; `RAWDUCK_WITH_GRPC`); shares the protobuf→JSON conversion approach and ingestion path with `raw_otlp_pb.cpp`. |

## Design invariants — do not break these

1. **The type lattice widens monotonically, never destructively.**
   `BOOLEAN | BIGINT→DOUBLE | DATE→TIMESTAMP` → scalar conflicts sink to `VARCHAR`; structural
   conflicts (object vs scalar, mixed arrays) sink to `JSON`. Nothing is ever dropped; widening is
   `ALTER ... SET DATA TYPE`, with `to_json()` for JSON rewrites (a plain VARCHAR→JSON cast rejects
   bare strings).

2. **Native ingestion runs in the caller's transaction.** DDL goes through
   `Catalog::CreateTable`/`Alter`, appends through `DataTable::LocalAppend` / optimistic
   collections. **Never run the INSERT through a second `Connection` while the calling query is
   executing** — concurrent transactions disable DuckDB's optimistic appends (~20× slower; this was
   v0.1's mistake). The second-connection SQL path exists *only* as the fallback for non-duck
   catalogs (DuckLake), which can't share the caller's transaction anyway.
   Catalog writes from inside a table function require
   `MetaTransaction::Get(context).ModifyDatabase(...)` first.

3. **Parse/inference is thread-pure; context work stays on the main thread.** The parse pipeline
   thread and append-pool workers only call `raw_json` code plus thread-confined storage objects
   (`OptimisticDataWriter`, per-worker `RowGroupCollection`). Anything touching `ClientContext`,
   the catalog, or `LocalAppend`/`LocalMerge` happens on the executing thread.

4. **Pool evolution is drain-free and barrier-free.** The consumer publishes new columns to an
   append-only list; each worker pads its own collection to the published prefix via
   `RowGroupCollection::AddColumn` (metadata-only NULL columns). Worker layouts must remain a
   prefix of the table layout (merges are positional); flush writers must be rebuilt against the
   CURRENT storage at drain (compression metadata and per-column partial block managers must match
   the evolved layout); the merge target must be the current catalog entry (ALTER swaps the
   storage object). Only type widening drains the pool.
   By default collections stay in memory and flush in one parallel burst at drain (lowest peak
   memory, optimal for churn). `rawduck_overlap_flush` (off by default) opts into flushing
   completed row groups mid-append to overlap parse with compression/IO — but **only while the
   worker's schema is stable** (`pending_columns.empty()`): churning batches keep deferring, so a
   freshly checkpointed row group is never re-extended. With it on, a worker that evolves must
   rebuild its flush writer against the post-ALTER storage (published via `PublishColumns`) and
   carry forward already-flushed partial blocks (`OptimisticDataWriter::Merge`); the worker's
   writer must always target a storage whose column count ≥ the worker's local prefix. Trade-off:
   ~10–15% faster on large stable imports, higher peak memory.

5. **Extraction is row-major.** Each row's JSON tree is traversed once and values are routed to
   column slots via schema-tree node identity (`RawExtractor`). Do not reintroduce per-column path
   walks (`yyjson_obj_getn` per column was the v0.1 hotspot: rows are sparse, schemas are wide).

6. **Projection rewriting must never return stale or differently-typed results.** The
   `pre_optimize` hook only rewrites `PROJECTION→AGGREGATE(count(*))→GET` when: the group set
   matches a registered projection exactly, the scan has no filters, the base table's
   `GetTotalRows()` equals the staleness token recorded at `raw_project` time, and the parent's
   colrefs are plain. The rewrite is `CAST(sum(count) AS BIGINT)` so result types are identical.
   Gated by `rawduck_use_projections` (default false; in-place UPDATEs of group columns are not
   detected by the token — documented limitation). When extending: any doubt → don't rewrite;
   falling back to the base table is always correct.

7. **Observation must never break queries.** The optimizer hooks are wrapped in try/catch and do
   best-effort collection only. Statistics live in `ObjectCache` (per `DatabaseInstance`,
   in-memory; `raw_stats_save/load` persist them explicitly).

8. **The HTTP API never shares a `ClientContext`.** Every request creates its own `Connection`
   (own transaction); ingestion goes through `RawIngestPayload` inside an explicit
   BeginTransaction/Commit with rollback on error. The listener thread is owned by a singleton
   whose destructor stops and joins it — a joinable thread reaching a static destructor calls
   `std::terminate`. The database is held via `weak_ptr` (503 after close, never use-after-free).
   sqllogictests cover lifecycle only (start/double-start/stop/restart); endpoint behavior is
   verified with curl (see README examples).

9. **Configuration is data, DuckDB-style.** User transforms are defined through a *scalar*
   function (`raw_transform_define`) precisely so definitions compose with `read_json()` files,
   tables, or any query. Follow this pattern for new extensible features; don't invent config file
   formats or bespoke DDL.

## Hard-won DuckDB v1.5 facts

- **Transparent `INSERT` auto-evolution is impossible**: `TableCatalogEntry::GetColumnIndex` /
  `GetColumns` are non-virtual and the binder resolves columns before any catalog hook. That's why
  ingest is endpoint-shaped (`raw_ingest`), like hosted RawMergeTree services.
- A storage extension whose catalog has `IsDuckCatalog() == true` gets a native
  `SingleFileStorageManager` automatically (`AttachedDatabase` ctor) — that's all
  `rawduck:` ATTACH needs.
- `LogicalGet::table_filters` is keyed by the table's **logical column index**; `get.names` covers
  the full schema, so `names[filter.first]` is the column name. Group-by bindings must be traced
  through `LogicalProjection`s **and** `__internal_compress_*` (compressed materialization)
  wrappers — which is also why the projection rewrite runs in `pre_optimize` (clean bound plans).
- yyjson is vendored at `duckdb/third_party/yyjson` (namespace `duckdb_yyjson`; `YYJSON_TYPE_*`
  are macros — don't namespace-qualify; read flags are namespaced constants). Link `duckdb_yyjson`.
- `read_json(..., records='false')` still type-infers; add `columns={json:'JSON'}` for raw JSON.
- `ObjectCacheEntry` subclasses must implement `GetEstimatedCacheMemory()` (return invalid
  `optional_idx` to forbid eviction).
- Table function args bind as constants — no subqueries/prepared params as arguments.
- **OTLP transport parity is deliberate**: protobuf's generic JSON conversion deviates from the
  OTLP/JSON mapping in two places — bytes ids (`traceId`/`spanId`) come out base64 (spec says hex)
  and enums come out as names (spec says integers). Fixed by base64→hex conversion in the otlp
  semantic normalization (`raw_json.cpp`, covers gRPC + HTTP-protobuf) and
  `JsonPrintOptions::always_print_enums_as_ints` at both decode sites. All transports must keep
  producing byte-identical columns or mixed exporter fleets fork column types.

## Testing conventions

sqllogictests in `test/sql/`: `rawduck.test` (core types/records), `raw_ingest.test` (evolution),
`raw_advanced.test` (streaming, transforms, pool, optimize, projections), `raw_attach.test`
(stores, transactions, persistence), `raw_api.test` (server lifecycle), `ducklake.test` (`require ducklake`, skips when absent).
Every feature needs: happy path, evolution interaction, error case, and—for anything that can
return wrong data—a proof test (e.g. tampering with a projection to prove the rewrite engaged).
`raw_ingest` output is `(table, created, columns_added, columns_widened, rows, errors)`;
`raw_ingest_file` adds `batches`. Changing these schemas means updating every `query IIIIII` block.

## Known boundaries (documented, not hidden)

- DuckLake fallback can't widen columns with expressions (`ALTER ... USING` unsupported there);
  RawDuck retries with JSON-widening disabled and converts incoming values instead.
- Missing payload keys insert `NULL`, not column defaults.
- Projection rewriting doesn't detect in-place UPDATEs of group columns (token is row-count based).
- Generated columns and indexed/constrained tables use the serial append path.
- Builds without protobuf (wasm, `RAWDUCK_DISABLE_OTLP_PROTOBUF=1`) answer OTLP/HTTP protobuf
  bodies with a 415 pointing at `http/json`; everything else is unaffected.
