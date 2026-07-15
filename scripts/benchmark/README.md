# RawDuck benchmark scripts

Reproducible OTEL ingest timings for the performance program. Data files land in
`benchmark/data/` (gitignored); JSON results in `benchmark/results/`.

**Policy:** we only merge changes that improve or match baseline on every metric
(cold and warm, all signals). Features that regress any default path are removed,
not shipped as opt-in toggles.

## Quick smoke (CI-friendly, ~100k records/signal)

```sh
GEN=ninja make release
./scripts/benchmark/run_otel.sh --quick --output benchmark/results/smoke.json
./scripts/benchmark/compare.sh benchmark/results/smoke.json
```

## Full baseline (1M records, best of 3 runs)

```sh
./scripts/benchmark/run_otel.sh --records 1000000 --runs 3 \
  --output benchmark/results/otel_1m.json
```

Reports **cold** (fresh DB, schema discovery) and **warm** (re-ingest into an
evolved empty table — schema cached, rows deleted between timed runs) for traces,
logs, and metrics.

## Compare against baseline

`benchmark/results/baseline-otel.json` holds committed thresholds (100k quick
run). Fail if any metric drops below 90% of baseline:

```sh
./scripts/benchmark/compare.sh benchmark/results/smoke.json
```

## Generate data only

```sh
python3 scripts/benchmark/gen_otlp.py all 1000000
```
