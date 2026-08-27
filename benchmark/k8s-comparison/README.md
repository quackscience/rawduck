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
