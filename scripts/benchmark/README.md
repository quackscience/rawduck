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

Requires: bash, python3, a release build (`build/release/duckdb` + extension).

## VARIANT vs RawDuck (branch `feat/variant-benchmark`)

Compares DuckDB **VARIANT as of v1.5.5** (not the v2.0 shredded-execution preview)
against RawDuck typed columns on the same OTLP/JSON traces envelopes.

```sh
# after: git clone --recurse-submodules … && git checkout feat/variant-benchmark
#        GEN=ninja make release
./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 \
  --output "benchmark/results/variant_1m_$(hostname -s)_$(date -u +%Y%m%dT%H%M%SZ).json"
```

`--quick` is 100k records / 1 ingest session (sanity). Publishable numbers use
1M records and best-of-3 ingest sessions.

Send back: the JSON file. It already embeds `git_commit`, `duckdb_version`, and a
`host` block (CPU, cores, RAM, OS). Envelope ingest rows are **not** span
records — the JSON labels grain so rec/s is not mixed.

Paths in the result:

| path | what it measures |
|---|---|
| `rawduck` | `raw_ingest_file` + typed columns (`otlp-traces`) |
| `variant_envelope` | one VARIANT per NDJSON line (OTLP export envelope) |
| `variant_otlp` | SQL unnest → one VARIANT `{resource, span}` per span (KeyValue arrays kept) |
| `json_otlp` | same exploded shape stored as JSON |
| `variant_flat` / `json_flat` | query/storage encodings of already-shredded RawDuck rows (not an ingest path) |

Queries: error count by service, p99 latency by route, status-code distribution.
`*_pos` uses generator-stable attribute indexes; `*_kv` does honest key lookup.
