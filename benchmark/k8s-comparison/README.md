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

Measured with `./run_full_benchmark.sh` (full 10M-record run, `--systems` defaults, 3 query runs
each) on an NVIDIA GB10 Spark aarch64 host (20-core Cortex-X925/A725, 121 GiB RAM). Absolute numbers
are hardware-dependent -- re-run `./run_full_benchmark.sh` on your own machine for numbers that
matter for your deployment; this is one data point, not a universal claim.

### Ingest (10,000,000 records, ~6.4 GB NDJSON)

| System | Time (s) | Records/s | Storage (MB) |
|---|---:|---:|---:|
| RawDuck | 27.7 | 361,573 | 581.0 |
| ClickHouse | 29.5 | 339,156 | 2,951.4 |
| OpenObserve | 140.0 | 71,422 | n/a (not reported by the API) |

### Query latency, hot (ms, best of 3 runs)

| Query | RawDuck | ClickHouse | OpenObserve |
|---|---:|---:|---:|
| count_by_service | 2.7 | 6.8 | 13.6 |
| filter_trace | 14.5 | 16.7 | 44.7 |
| filter_error | 3.6 | 7.0 | 30.0 |
| histogram_minute | 11.2 | 11.3 | 19.6 |
| status_distribution | 2.9 | 6.0 | 17.1 |
| avg_latency_by_path | 4.5 | 8.9 | 28.6 |
| recent_errors | 4.3 | 4.1 | 13.4 |
| service_sample | 1.4 | 4.4 | 18.2 |
| high_latency | 3.7 | 8.9 | 35.1 |

RawDuck matches or beats ClickHouse on ingest throughput while using roughly a fifth of its storage
footprint, and wins or ties nearly every query -- often by 2-3x -- against both ClickHouse and
OpenObserve. Full per-query cold/hot/all-runs numbers are in `results/k8s_comparison_10m.json` after
any run (gitignored; not checked in, since it's regenerated data, not source).

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
