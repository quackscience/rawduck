# K8s Logs Cross-System Benchmark

Compare **RawDuck** (DuckDB extension) vs **OpenObserve** (DataFusion) vs **ClickHouse**
on Kubernetes-style log data. Inspired by the [OpenObserve vs ClickHouse benchmark](https://openobserve.ai/blog/openobserve-vs-clickhouse-one-billion-logs-benchmark/).

## Quick Start

```bash
# From the rawduck project root:
cd benchmark/k8s-comparison

# 1. Setup (one-time)
./setup/install_openobserve.sh
./setup/install_clickhouse.sh

# 2. Start services (in separate terminals or background)
./setup/start_openobserve.sh --background
./setup/start_clickhouse.sh --background

# 3. Run benchmark
./run_k8s_comparison.sh --quick               # 1M records smoke test
./run_k8s_comparison.sh --records 10000000    # 10M records full run

# 4. Stop services
./setup/stop_openobserve.sh
./setup/stop_clickhouse.sh
```

## Prerequisites

### RawDuck (required)
Build from project root:
```bash
cd /path/to/rawduck
GEN=ninja make release
```

### OpenObserve
Auto-installed via `setup/install_openobserve.sh`. Downloads the official binary for your platform.

- Web UI: http://localhost:5080
- User: `root@example.com`
- Password: `Complexpass#123`

### ClickHouse
Auto-installed via `setup/install_clickhouse.sh`. Downloads the official binary for your platform.

- HTTP API: http://localhost:8123

## Benchmark Details

### Dataset
Kubernetes-style log records with 25 columns:
- `timestamp` (nanoseconds)
- `level` (INFO/WARN/ERROR)
- `message`
- `kubernetes.*` (namespace, pod, container, labels, node)
- `trace_id`, `span_id`
- `http.*` (method, status_code, url, route)
- `request_duration_ms`, `response_size_bytes`
- `service_name`, `region`, `environment`

### Queries (9 total)

**Indexed Count (3)**
- Count by service_name
- Filter by trace_id prefix
- Count errors by service

**Full-Scan Aggregations (3)**
- Histogram by minute
- HTTP status code distribution
- Average duration by route

**Row Fetches (3)**
- Recent errors (ORDER BY timestamp DESC LIMIT 100)
- Service sample (filter + LIMIT)
- High latency requests

### Metrics
- **Ingest time**: Time to load NDJSON into each system
- **Storage size**: On-disk footprint after ingest
- **Query latency**: Cold and hot (cached) execution times

## Results

Best of 3 complete `./run_full_benchmark.sh` runs (full 10M-record ingest + 3 query runs each,
repeated 3 times end-to-end) on an NVIDIA GB10 Spark aarch64 host (20-core Cortex-X925/A725, 121 GiB
RAM). Every one of the 9 system-runs verified its own row count after ingest (all `10,000,000`, no
short-ingests or empty-result queries silently scored as fast -- see the "measurement defects" fix
below); the best of 3 full runs is reported, with the run-to-run spread shown so it doesn't read as
more precise than it is. Absolute numbers are hardware-dependent -- re-run `./run_full_benchmark.sh`
on your own machine for numbers that matter for your deployment; this is one data point, not a
universal claim, and CI now runs it on GitHub-hosted workers too (see below).

### Ingest (10,000,000 records, ~6.4 GB NDJSON)

| System | Best of 3 (s) | Records/s | Storage (MB) |
|---|---:|---:|---:|
| RawDuck | 21.3 | 469,080 | ~581 |
| ClickHouse | 22.2 | 450,957 | 2,975.8 |
| OpenObserve | 123.8 | 80,792 | 855.4 (5,594 uncompressed, 6.5x) |

### Query latency, hot (ms, best of 3 full runs; +spread shows the worst-vs-best run delta)

| Query | RawDuck | ClickHouse | OpenObserve |
|---|---:|---:|---:|
| count_by_service | 2.5 (+1.4) | 5.1 (+2.0) | 18.1 (+11.5) |
| filter_trace | 11.9 (+0.7) | 16.0 (+0.5) | 45.9 (+10.6) |
| filter_error | 3.5 (+0.7) | 6.8 (+0.6) | 28.4 (+8.3) |
| histogram_minute | 10.6 (+0.7) | 11.3 (+0.6) | 24.7 (+5.0) |
| status_distribution | 2.3 (+0.6) | 5.6 (+0.3) | 18.7 (+2.8) |
| avg_latency_by_path | 3.4 (+1.8) | 6.5 (+1.9) | 36.2 (+2.6) |
| recent_errors | 3.5 (+0.9) | 3.4 (+1.6) | 6.2 (+4.4) |
| service_sample | 0.9 (+0.4) | 4.0 (+0.7) | 7.4 (+4.5) |
| high_latency | 3.1 (+0.6) | 7.9 (+1.0) | 29.7 (+13.0) |

RawDuck ingests fastest, uses roughly a fifth of ClickHouse's storage footprint, and averages **2.1x**
faster than ClickHouse and **6.7x** faster than OpenObserve across these 9 queries. Run-to-run spread
is tight for RawDuck/ClickHouse (sub-millisecond to ~2ms) and wider for OpenObserve, consistent with
background compaction still settling during the sweep. Full per-query cold/hot/all-runs numbers are
in `results/k8s_comparison_10m.json` after any run (gitignored; not checked in, since it's
regenerated data, not source).

### Measurement defects fixed since the first pass

The harness could previously report fast query times against a store holding no matching rows or a
partially-discarded ingest, which would silently invert the comparison rather than just add noise.
Fixed (see PR #14): a future-dated query window that fell partly outside OpenObserve's default
ingest-acceptance range, empty query results being timed instead of flagged, discarded-record ingest
responses being read as successful, uncontrolled time-since-ingest during the query sweep, and
OpenObserve's storage size never being read from its streams API (always printed `0.0`). Separately,
`histogram_minute` was collapsing OpenObserve's result to a single bucket instead of ~120 -- its
`_timestamp` is normalized to microseconds on ingest while the query divided by a
nanoseconds-per-minute constant, so it was measuring a cheaper query than RawDuck/ClickHouse were
running. All three systems now group into the same 121 buckets.

## Usage

```bash
# Smoke test (1M records)
./run_k8s_comparison.sh --quick

# Full benchmark (10M records)
./run_k8s_comparison.sh --records 10000000

# Single system only
./run_k8s_comparison.sh --systems rawduck
./run_k8s_comparison.sh --systems openobserve
./run_k8s_comparison.sh --systems clickhouse

# Skip ingest (re-run queries only)
./run_k8s_comparison.sh --skip-ingest

# Custom output
./run_k8s_comparison.sh --records 10000000 --output results/my_run.json
```

## Directory Structure

```
benchmark/k8s-comparison/
├── README.md                    # This file
├── run_k8s_comparison.sh        # Main benchmark runner
├── run_k8s_comparison.py        # Benchmark logic
├── gen_k8s_logs.py              # Data generator
├── setup/
│   ├── install_openobserve.sh   # Download OpenObserve binary
│   ├── start_openobserve.sh     # Start OpenObserve server
│   ├── stop_openobserve.sh      # Stop OpenObserve
│   ├── install_clickhouse.sh    # Download ClickHouse binary
│   ├── start_clickhouse.sh      # Start ClickHouse server
│   └── stop_clickhouse.sh       # Stop ClickHouse
├── data/                        # Generated test data (gitignored)
└── results/                     # Benchmark results (gitignored)
```

## Replicating on Other Systems

1. Clone the rawduck repo and build:
   ```bash
   git clone --recurse-submodules https://github.com/your/rawduck
   cd rawduck
   GEN=ninja make release
   ```

2. Navigate to benchmark:
   ```bash
   cd benchmark/k8s-comparison
   ```

3. Run setup scripts (auto-detect platform):
   ```bash
   ./setup/install_openobserve.sh
   ./setup/install_clickhouse.sh
   ```

4. Start services and run benchmark:
   ```bash
   ./setup/start_openobserve.sh --background
   ./setup/start_clickhouse.sh --background
   ./run_k8s_comparison.sh --records 10000000
   ```

### Supported Platforms
- macOS (Intel and Apple Silicon)
- Linux (x86_64 and arm64)

### System Requirements
- 10M records: ~1 GB NDJSON, ~10 GB RAM recommended
- 100M records: ~10 GB NDJSON, ~32 GB RAM recommended
- 1B records: ~100 GB NDJSON, ~128 GB RAM recommended (matches OpenObserve benchmark)

## Notes

- **Cold queries**: First query execution after service restart / cache clear
- **Hot queries**: Second+ execution with warm caches
- Query timing includes network overhead for OpenObserve/ClickHouse (HTTP API)
- RawDuck timing includes process startup for each query (subprocess per query)
- For production-grade benchmarking, run multiple iterations and report median
