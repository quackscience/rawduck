# Distribution probes — one DuckLake, multiple writers/readers

Run the **probe matrix** to learn what works on your platform before scaling DuckLake metadata.

```sh
GEN=ninja make release
./scripts/distribution/probe_matrix.sh          # full matrix (~1 min)
./scripts/distribution/probe_matrix.sh --quick  # skip expected-fail dual-attach probe
```

Reports land in `benchmark/work/distribution_probe/`:

- `probe_report.md` — human table
- `probe_report.json` — machine-readable for CI baselines

## Scenario categories

| Category | What it models | Key question |
|---|---|---|
| **in_process** | sqllogictest, one ATTACH, multi-connection | Do interleaved writes/reads and schema evolution work? |
| **multi_process** | Separate DuckDB PIDs, short- or long-lived attach | Can N writer processes share sqlite DuckLake metadata? |
| **single_hub** | One `raw_serve` + DuckLake backend, parallel HTTP | Is fan-in to one hub the high-throughput path? |

## Individual scripts

| Script | Scenario |
|---|---|
| `probe_matrix.sh` | Runs all probes, writes report |
| `run_ducklake_otel_cluster.sh` | Sequential OTEL writer processes → READ_ONLY reader |
| `run_ducklake_writers.sh` | Overlapping raw_ingest processes |

## SQL tests (CI / unittest)

```sh
./build/release/test/unittest --test-dir . "test/sql/raw_distribution*"
```

| File | Covers |
|---|---|
| `raw_distribution_ducklake.test` | Two connections, interleaved ingest |
| `raw_distribution_ducklake_otel.test` | OTLP transform + prefix + schema evolution |
| `raw_distribution_ducklake_inprocess.test` | Read-while-write, type widening, column churn |

Design notes and evolution roadmap: [docs/DISTRIBUTION.md](../../docs/DISTRIBUTION.md).
