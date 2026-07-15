# RawDuck benchmark scripts

Reproducible OTEL ingest timings for the performance program. Data files land in
`benchmark/data/` (gitignored); JSON results in `benchmark/results/`.

**Policy:** we only merge changes that improve or match baseline on every metric
(cold and warm, all signals). Features that regress any default path are removed,
not shipped as opt-in toggles.

## Performance program (`feat/perf-otel`)

Tracked in [BENCHMARK.md](../../BENCHMARK.md). Phases shipped on this branch:

| Phase | What changed | Benchmark impact |
|---|---|---|
| **1** | `run_otel.sh`, `compare.sh`, `gen_otlp.py`; committed `baseline-otel.json`; removed `rawduck_overlap_flush_auto` | Harness + regression gate; dropped opt-in that regressed warm traces ~30% |
| **2** | `RawSchemaCache` absorbed-shape plans; skip `InferSchema`/`FlattenSchema` on stable shapes within file ingest | ~1.5–2× warm OTEL; parallel cold inference preserved |
| **3** | OTLP explode keeps rows in mut form (no bulk `mut→imut` copy) | +30% OTLP parse on traces/metrics |
| **4** | Alloc-free `RawExtractor` key match; thread-local JSON write bump pool; evolution classify pass | Wide-schema / JSON-heavy paths; no OTEL regression |
| **5** | DuckLake native append (eliminate SQL `raw_records` fallback) | *next* — multi-node throughput |
| **6** | Multi-node evolution mutex, postgres DuckLake metadata | *planned* |

After each phase: `./scripts/benchmark/run_otel.sh --records 1000000 --runs 5` and
`./scripts/benchmark/compare.sh <results.json>`.

## Quick smoke (CI-friendly, ~100k records/signal)

```sh
GEN=ninja make release
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/smoke.json
./scripts/benchmark/compare.sh benchmark/results/smoke.json
```

## Full baseline (1M records, best of 5 runs)

```sh
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5 \
  --output benchmark/results/otel_1m.json
```

Reports **cold** (fresh DB, schema discovery) and **warm** (re-ingest into an
evolved empty table — schema cached, rows deleted between timed runs) for traces,
logs, and metrics.

### Metric definitions

- **Cold:** new database, first ingest of each signal — includes CREATE TABLE and any column adds.
- **Warm:** same database after cold run; `DELETE FROM <table>` then timed re-ingest — measures
  steady-state collector rate with an evolved schema (no DDL during the timed window).

## Compare against baseline

`benchmark/results/baseline-otel.json` holds committed thresholds (100k quick
run). Fail if any metric drops below 98% of baseline:

```sh
./scripts/benchmark/compare.sh benchmark/results/smoke.json
```

To refresh the committed baseline after a verified improvement on all six metrics:

```sh
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/baseline-otel.json
# review, then commit benchmark/results/baseline-otel.json
```

## Generate data only

```sh
python3 scripts/benchmark/gen_otlp.py all 1000000
```

## Files

| Script | Role |
|---|---|
| `lib.sh` | Shared helpers (paths, duckdb binary, timing) |
| `gen_otlp.py` | OTLP/JSON NDJSON generator (traces, logs, metrics) |
| `run_otel.sh` | Cold/warm ingest timing, best of N runs |
| `compare.sh` | Regression gate vs `baseline-otel.json` |
