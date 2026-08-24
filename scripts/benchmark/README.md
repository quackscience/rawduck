# RawDuck benchmark scripts

Reproducible OTEL ingest timings. Data lands in `benchmark/data/` (gitignored);
JSON results in `benchmark/results/` (gitignored except the committed baseline).

**Policy:** only merge ingest changes that improve or match baseline on every
metric (cold and warm, all signals). Features that regress any default path are
removed, not shipped as opt-in toggles.

## Session model (cold then warm)

Each **session** is one DuckDB process (`run_otel_session.py`):

1. `LOAD rawduck`
2. **Cold** — `raw_ingest_file` + `CHECKPOINT` (timed; schema discovery)
3. `DELETE FROM table` (untimed; keep evolved DDL, empty rows)
4. **Warm** — `raw_ingest_file` on a second NDJSON with shifted timestamps +
   `CHECKPOINT` (timed; must report `columns_added = 0` and `columns_widened = 0`)

Warm keeps the in-memory schema cache hot (same process). This models a collector
hub that already absorbed an OTLP shape and receives a fresh batch — not a new
DuckDB startup on an old database file.

Host timing uses `time.perf_counter()` around each timed window (not SQL
`epoch_ms`, which is too coarse for short runs).

## Remote / new machine (post-build)

```sh
# after: git clone --recurse-submodules … && GEN=ninja make release
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/smoke.json
./scripts/benchmark/compare.sh benchmark/results/smoke.json

# publishable numbers
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5 \
  --output "benchmark/results/otel_1m_$(hostname -s)_$(date -u +%Y%m%dT%H%M%SZ).json"
```

Send back: the JSON file, host CPU/cores/RAM/OS, and `git rev-parse HEAD`.

## Quick smoke (~100k / signal)

```sh
GEN=ninja make release
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/smoke.json
./scripts/benchmark/compare.sh benchmark/results/smoke.json
```

## Full baseline (1M records, best of 5 sessions)

```sh
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5 \
  --output benchmark/results/otel_1m.json
```

### Metric definitions

- **Cold:** first `raw_ingest_file` in a fresh database — CREATE TABLE + column adds.
- **Warm:** second `raw_ingest_file` in the **same** process after `DELETE FROM table`
  — evolved schema, no DDL, fresh timestamps in `*_warm.ndjson`.

## Compare against baseline

`benchmark/results/baseline-otel.json` holds committed thresholds (100k quick run).
Fails if any metric drops below 98% of baseline:

```sh
./scripts/benchmark/compare.sh benchmark/results/smoke.json
./scripts/benchmark/compare.sh result.json --min-ratio 0.95
```

Refresh the committed baseline only after a verified improvement on all six metrics:

```sh
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/baseline-otel.json
# review, then commit benchmark/results/baseline-otel.json
```

## Generate data only

```sh
python3 scripts/benchmark/gen_otlp.py all 1000000
python3 scripts/benchmark/gen_otlp.py traces 1000000 benchmark/data 1700086400000000000 _warm
```

## Files

| Script | Role |
|---|---|
| `lib.sh` | Paths + build checks (bash) |
| `gen_otlp.py` | OTLP/JSON NDJSON generator (`ts_base` + suffix for warm files) |
| `run_otel_session.py` | Single-process cold→warm driver (host timing) |
| `run_otel.sh` | Orchestrator: data gen, N sessions, JSON output (bash) |
| `compare.sh` | Regression gate vs `baseline-otel.json` (bash) |
| `run_variant.sh` / `run_variant.py` | VARIANT (DuckDB v1.5) vs RawDuck ingest + query + storage |
| `run_otel_streaming.sh` / `.py` / `otel_gen_load.py` | Real OTLP/HTTP streaming ingestion via the actual OpenTelemetry SDK (see below) |

Requires: bash, python3, a release build (`build/release/duckdb` + extension).

## Realistic OTEL streaming ingestion (`run_otel_streaming.sh`)

`run_otel.sh` and `run_variant.sh` both bulk-load one big NDJSON file — useful
for measuring the shredding/ingest engine in isolation, but not how OTEL data
actually arrives. This benchmark instead drives real traffic through
`raw_serve()`'s HTTP OTLP endpoint using the **actual OpenTelemetry Python
SDK** (`TracerProvider` + `BatchSpanProcessor` + `OTLPSpanExporter`, real
protobuf wire format — not RawDuck's own NDJSON generator), from multiple
concurrent *processes* (sidesteps Python's GIL, models multiple
services/collectors exporting to the same collector hub concurrently).

```sh
GEN=ninja make release
./scripts/benchmark/run_otel_streaming.sh --quick                              # smoke: 4 workers x 5k spans
./scripts/benchmark/run_otel_streaming.sh --workers 16 --spans-per-worker 60000 # ~180k spans/sec on a 20-core arm64 box
```

First run creates a dedicated venv (`benchmark/work/otel-streaming-venv`) with
the OpenTelemetry SDK — a plain system-wide `pip install` is refused on
externally-managed Python installs (PEP 668), so this is required, not
optional. Reports `spans_per_sec`, `rows_ingested` (checked against
`total_spans_sent` — any mismatch is a real bug, not a benchmark artifact),
and writes JSON to `benchmark/results/`.

This benchmark caught two real concurrency issues under sustained 16-32-way
concurrent OTLP/HTTP load, both fixed in `raw_ingest.cpp`:

- **Concurrent first-insert races.** Multiple exporter processes racing to
  `INSERT` into a table that doesn't exist yet each open their own
  transaction, and DuckDB's catalog allows only one of them to `CREATE TABLE`
  — the rest saw a `TransactionException` ("write-write conflict") surfaced
  as an HTTP 400, which OTLP exporters correctly do not retry (4xx = client
  error), silently dropping that batch. Fixed with `RawIngestSerialized`: a
  table's first-ever insert in this process queues behind an in-process lock
  instead of racing at all; every request afterward (the steady-state case)
  never touches the lock, just a cached membership check.
- **Per-commit WAL/fsync serialization tail latency.** Even with the race
  fixed, DuckDB's single-writer WAL still serializes every commit's fsync
  regardless of how many independent Connections are committing — no single
  commit was ever slow in isolation (~200-450ms), but under a big enough
  pile-up (16+ concurrent committers) the cumulative queueing occasionally
  pushed one unlucky request's total latency past a 10s client read timeout.
  Fixed with `RawIngestGroupCommit`: concurrent requests to the same table
  coalesce into one shared commit (a "leader" merges every currently-queued
  request's already-parsed payload via `MergeParsedPayloads` and runs a
  single transaction for all of them). Lingering to let a batch form is
  gated on an `active` in-flight counter, not unconditional: a lone,
  uncontended request (no sibling already in flight for the same table)
  skips the linger and the whole coalescing dance entirely, going straight
  through at the same latency as a direct, non-batched call. The first cut
  of this fix lingered unconditionally and regressed *every* request's
  latency ~7x (single-digit ms -> ~30ms) even with nobody to batch with —
  caught by explicitly re-benchmarking the solo/no-contention case after
  the fix, not just the concurrent stress test that motivated it.

Verified both ends: 45/45 clean in stress testing (16-way and 32-way
concurrency, 0 failures) with throughput ~130-140k spans/sec while
contention is genuinely engaged, *and* solo-request latency back to the
same ~4-6ms baseline as before any of this (measured directly, sequential
uncontended requests). Regression-tested in `test/http/raw_api_compat.sh`'s
"concurrent create" section (fires 8 concurrent requests at a brand-new table).

## VARIANT vs RawDuck (branch `feat/variant-benchmark`)

Compares DuckDB **VARIANT as of v1.5.5** (not the v2.0 shredded-execution preview)
against RawDuck typed columns on the same OTLP/JSON traces envelopes.

For the same comparison against v2's VARIANT, build on branch `v2.0.0` (both
submodules track DuckDB main / v2.0-dev) and run the identical commands — the
harness reads `pragma_version()` and labels the run, so a v2 result file is never
confused with a v1.5.5 one. See "Running the same comparison on DuckDB v2.0-dev"
in `BENCHMARK.md`.

```sh
# after: git clone --recurse-submodules … && git checkout feat/variant-benchmark
#        GEN=ninja make release
./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 \
  --output "benchmark/results/variant_1m_$(hostname -s)_$(date -u +%Y%m%dT%H%M%SZ).json"
```

`--quick` is 100k records / 1 ingest session (sanity). Publishable numbers use
1M records and best-of-3 ingest sessions.

DuckDB is **CPU-only** (a CUDA GPU does not accelerate this). On many-core ARM
hosts, default `threads = nproc` often makes VARIANT extract / nested `UNNEST`
thrash — pin workers:

```sh
./scripts/benchmark/run_variant.sh --quick --threads 8
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 --threads 8
# still stuck on queries: skip honest KeyValue lookups
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 --threads 8 --skip-kv
```

### Linux / arm64 hang (DuckDB 1.5 CLI)

If the process sits idle with no CPU at `ingest …` (especially right after
startup), that is usually the DuckDB **v1.5 CLI stdin/color-detection stall**,
not RawDuck. The harness now passes `-dark-mode`, sets `DUCKDB_NO_HIGHLIGHT=1`,
pre-creates the v1.5.0 DB with a one-shot `-c` (no interactive `ATTACH`), and
reads stdout from a dedicated thread.

If CPU is pegged on `variant_otlp` / `variant_envelope`, that is VARIANT
shredding fat OTLP rows (slow, not a hang). Envelope ingest stays off by
default; use `--paths rawduck,variant_otlp,json_otlp,variant_flat,json_flat`.

Send back: the JSON file. It already embeds `git_commit`, `duckdb_version`, and a
`host` block (CPU, cores, RAM, OS). Envelope ingest rows are **not** span
records — the JSON labels grain so rec/s is not mixed.

Paths in the result:

| path | what it measures |
|---|---|
| `rawduck` | `raw_ingest_file` + typed columns (`otlp-traces`) |
| `variant_envelope` | *(off by default)* one VARIANT per NDJSON line. Fat OTLP envelopes can hang for hours on Linux aarch64; pass `--paths …,variant_envelope` to include. |
| `variant_otlp` | SQL unnest → one VARIANT `{resource, span}` per span (KeyValue arrays kept) |
| `json_otlp` | same exploded shape stored as JSON |
| `variant_flat` / `json_flat` | query/storage encodings of already-shredded RawDuck rows (not an ingest path) |

Queries: error count by service, p99 latency by route, status-code distribution.
`*_pos` uses generator-stable attribute indexes; `*_kv` does honest key lookup.
