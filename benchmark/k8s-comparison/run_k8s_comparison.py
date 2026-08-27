#!/usr/bin/env python3
"""Cross-system benchmark: RawDuck vs OpenObserve vs ClickHouse on K8s-style logs.

Prerequisites:
  - RawDuck: build/release/duckdb with extension (GEN=ninja make release)
  - OpenObserve: running at localhost:5080 (docker or binary)
  - ClickHouse: running at localhost:8123 (docker or binary)

Usage:
  python3 run_k8s_comparison.py --records 10000000
  python3 run_k8s_comparison.py --quick  # 1M records for smoke test
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import urllib.request
import urllib.error

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parents[1]  # rawduck/
BENCHMARK_DIR = SCRIPT_DIR  # benchmark/k8s-comparison/
DATA_DIR = BENCHMARK_DIR / "data"
RESULTS_DIR = BENCHMARK_DIR / "results"
DUCKDB_BIN = PROJECT_ROOT / "build" / "release" / "duckdb"


@dataclass
class QueryResult:
    name: str
    system: str
    cold_ms: float = 0  # First run (cold cache)
    hot_ms: float = 0   # Best of subsequent runs (hot cache)
    runs: list[float] = field(default_factory=list)  # All run times
    error: str | None = None


@dataclass
class BenchmarkResult:
    system: str
    ingest_time_s: float = 0
    storage_mb: float = 0
    records: int = 0
    queries: list[QueryResult] = field(default_factory=list)
    error: str | None = None


# Queries designed to work across all three systems (using underscore column names)
QUERIES = {
    # Count/aggregation queries
    "count_by_service": {
        "desc": "Count records by service_name",
        "rawduck": "SELECT service_name, count(*) as cnt FROM k8s_logs GROUP BY service_name ORDER BY cnt DESC",
        "openobserve": "SELECT service_name, count(*) as cnt FROM k8s_logs GROUP BY service_name ORDER BY cnt DESC",
        "clickhouse": "SELECT service_name, count(*) as cnt FROM k8s_logs GROUP BY service_name ORDER BY cnt DESC",
    },
    "filter_trace": {
        "desc": "Filter by trace_id prefix",
        "rawduck": "SELECT count(*) FROM k8s_logs WHERE trace_id LIKE 'a%'",
        "openobserve": "SELECT count(*) FROM k8s_logs WHERE trace_id LIKE 'a%'",
        "clickhouse": "SELECT count(*) FROM k8s_logs WHERE trace_id LIKE 'a%'",
    },
    "filter_error": {
        "desc": "Count errors by service",
        "rawduck": "SELECT service_name, count(*) as errors FROM k8s_logs WHERE level = 'ERROR' GROUP BY service_name",
        "openobserve": "SELECT service_name, count(*) as errors FROM k8s_logs WHERE level = 'ERROR' GROUP BY service_name",
        "clickhouse": "SELECT service_name, count(*) as errors FROM k8s_logs WHERE level = 'ERROR' GROUP BY service_name",
    },
    "histogram_minute": {
        "desc": "Records per minute histogram",
        "rawduck": "SELECT (_timestamp / 60000000000)::BIGINT as minute, count(*) FROM k8s_logs GROUP BY minute ORDER BY minute",
        "openobserve": "SELECT (_timestamp / 60000000000) as minute, count(*) FROM k8s_logs GROUP BY minute ORDER BY minute",
        "clickhouse": "SELECT intDiv(_timestamp, 60000000000) as minute, count(*) FROM k8s_logs GROUP BY minute ORDER BY minute",
    },
    "status_distribution": {
        "desc": "HTTP status code distribution",
        "rawduck": "SELECT http_status, count(*) as cnt FROM k8s_logs GROUP BY http_status ORDER BY cnt DESC",
        "openobserve": "SELECT http_status, count(*) as cnt FROM k8s_logs GROUP BY http_status ORDER BY cnt DESC",
        "clickhouse": "SELECT http_status, count(*) as cnt FROM k8s_logs GROUP BY http_status ORDER BY cnt DESC",
    },
    "avg_latency_by_path": {
        "desc": "Average latency by HTTP path",
        "rawduck": "SELECT http_path, avg(http_latency_ms) as avg_ms FROM k8s_logs GROUP BY http_path",
        "openobserve": "SELECT http_path, avg(http_latency_ms) as avg_ms FROM k8s_logs GROUP BY http_path",
        "clickhouse": "SELECT http_path, avg(http_latency_ms) as avg_ms FROM k8s_logs GROUP BY http_path",
    },
    # Row fetch queries
    "recent_errors": {
        "desc": "Fetch recent errors",
        "rawduck": "SELECT * FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
        "openobserve": "SELECT * FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
        "clickhouse": "SELECT * FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
    },
    "service_sample": {
        "desc": "Sample rows for a specific service",
        "rawduck": "SELECT * FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
        "openobserve": "SELECT * FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
        "clickhouse": "SELECT * FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
    },
    "high_latency": {
        "desc": "Find high latency requests",
        "rawduck": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
        "openobserve": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
        "clickhouse": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
    },
}


# Global RawDuck server process (modeled after OTEL streaming benchmark's Server class)
_rawduck_proc = None
_rawduck_db_path = None
RAWDUCK_PORT = 9876
RAWDUCK_TOKEN = "benchmark_token"


def _rawduck_send(sql: str) -> str:
    """Send SQL to running RawDuck process and return output."""
    global _rawduck_proc
    if not _rawduck_proc or not _rawduck_proc.stdin or not _rawduck_proc.stdout:
        return ""

    marker = "__RAWDUCK_BENCH_DONE__"
    cmd = f"{sql.rstrip()}\nSELECT '{marker}';\n"
    _rawduck_proc.stdin.write(cmd)
    _rawduck_proc.stdin.flush()

    lines = []
    while True:
        line = _rawduck_proc.stdout.readline()
        if not line:
            break
        line = line.rstrip("\n")
        if marker in line:
            break
        lines.append(line)
    return "\n".join(lines)


def start_rawduck_server() -> bool:
    """Start RawDuck DuckDB process with raw_serve() for HTTP queries."""
    global _rawduck_proc, _rawduck_db_path

    if not DUCKDB_BIN.exists():
        return False

    _rawduck_db_path = DATA_DIR / "rawduck_k8s.db"
    _rawduck_db_path.unlink(missing_ok=True)
    wal_path = Path(str(_rawduck_db_path) + ".wal")
    wal_path.unlink(missing_ok=True)

    # Start DuckDB CLI process (like OTEL streaming benchmark)
    _rawduck_proc = subprocess.Popen(
        [str(DUCKDB_BIN), str(_rawduck_db_path), "-unsigned", "-batch", "-csv", "-noheader"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=0,
    )

    # Load extension and start HTTP server
    _rawduck_send("LOAD rawduck;")
    _rawduck_send(f"CALL raw_serve(host := '127.0.0.1', port := {RAWDUCK_PORT}, token := '{RAWDUCK_TOKEN}');")

    # Wait for server to be ready
    for _ in range(30):
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{RAWDUCK_PORT}/health")
            urllib.request.urlopen(req, timeout=1)
            return True
        except Exception:
            time.sleep(0.5)
    return False


def stop_rawduck_server():
    """Stop RawDuck HTTP server and process."""
    global _rawduck_proc
    if _rawduck_proc:
        try:
            _rawduck_send("CALL raw_serve_stop();")
            time.sleep(0.3)
            _rawduck_proc.stdin.close()
            _rawduck_proc.wait(timeout=10)
        except Exception:
            _rawduck_proc.kill()
            _rawduck_proc.wait()
        _rawduck_proc = None


def run_rawduck_ingest(data_path: Path) -> BenchmarkResult:
    """Ingest K8s logs into RawDuck using raw_ingest_file().

    This matches the OTEL benchmark approach - direct file ingestion is RawDuck's
    native high-performance path, comparable to ClickHouse's native file loading.
    """
    global _rawduck_db_path
    result = BenchmarkResult(system="rawduck")

    # Use raw_ingest_file() via the running process (like OTEL benchmark)
    sql = f"SELECT rows, columns_added, columns_widened, errors FROM raw_ingest_file('k8s_logs', '{data_path}');"

    start = time.perf_counter()
    try:
        output = _rawduck_send(sql)
        result.ingest_time_s = time.perf_counter() - start

        # Parse CSV output: rows,columns_added,columns_widened,errors
        for line in output.strip().split("\n"):
            parts = line.split(",")
            if len(parts) >= 4 and parts[0].lstrip("-").isdigit():
                result.records = int(parts[0])
                break

        # Get storage size
        if _rawduck_db_path and _rawduck_db_path.exists():
            result.storage_mb = _rawduck_db_path.stat().st_size / (1024 * 1024)

    except Exception as e:
        result.error = str(e)

    return result


def run_rawduck_queries(result: BenchmarkResult, runs: int = 3) -> None:
    """Run benchmark queries against RawDuck HTTP API with cold/hot measurement."""
    for name, q in QUERIES.items():
        sql = q["rawduck"]
        payload = json.dumps({"sql": sql}).encode()

        times = []
        error = None

        for run_idx in range(runs):
            start = time.perf_counter()
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:{RAWDUCK_PORT}/v1/query",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")
                req.add_header("Authorization", f"Bearer {RAWDUCK_TOKEN}")

                with urllib.request.urlopen(req, timeout=120) as resp:
                    resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
            except Exception as e:
                error = str(e)
                break

        if times:
            qr = QueryResult(
                name=name,
                system="rawduck",
                cold_ms=times[0],
                hot_ms=min(times[1:]) if len(times) > 1 else times[0],
                runs=times,
                error=error,
            )
        else:
            qr = QueryResult(name=name, system="rawduck", error=error)
        result.queries.append(qr)


def run_openobserve_ingest(data_path: Path) -> BenchmarkResult:
    """Ingest K8s logs into OpenObserve."""
    result = BenchmarkResult(system="openobserve")

    # Check if OpenObserve is running
    try:
        req = urllib.request.Request("http://localhost:5080/healthz")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        result.error = "OpenObserve not running at localhost:5080"
        return result

    # Delete existing stream
    try:
        delete_req = urllib.request.Request(
            "http://localhost:5080/api/default/streams/k8s_logs",
            method="DELETE",
        )
        delete_req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")  # default creds
        urllib.request.urlopen(delete_req, timeout=10)
    except Exception:
        pass  # Stream might not exist

    # Ingest data in batches
    batch_size = 10000
    start = time.perf_counter()
    records_sent = 0

    try:
        with data_path.open() as f:
            batch = []
            for line in f:
                record = json.loads(line)
                # Flatten dotted keys for OpenObserve
                flat = {}
                for k, v in record.items():
                    flat[k.replace(".", "_")] = v
                batch.append(flat)

                if len(batch) >= batch_size:
                    payload = json.dumps(batch).encode()
                    req = urllib.request.Request(
                        "http://localhost:5080/api/default/k8s_logs/_json",
                        data=payload,
                        method="POST",
                    )
                    req.add_header("Content-Type", "application/json")
                    req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")
                    urllib.request.urlopen(req, timeout=60)
                    records_sent += len(batch)
                    batch = []

                    if records_sent % 100000 == 0:
                        print(f"  OpenObserve: {records_sent:,} records ingested...")

            # Final batch
            if batch:
                payload = json.dumps(batch).encode()
                req = urllib.request.Request(
                    "http://localhost:5080/api/default/k8s_logs/_json",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")
                req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")
                urllib.request.urlopen(req, timeout=60)
                records_sent += len(batch)

        result.ingest_time_s = time.perf_counter() - start
        result.records = records_sent

    except Exception as e:
        result.error = str(e)

    return result


def run_openobserve_queries(result: BenchmarkResult, runs: int = 3) -> None:
    """Run benchmark queries against OpenObserve with cold/hot measurement."""
    # OpenObserve requires time range - use last 24 hours
    now_us = int(time.time() * 1_000_000)
    start_time = now_us - (24 * 60 * 60 * 1_000_000)  # 24 hours ago in microseconds

    for name, q in QUERIES.items():
        sql = q["openobserve"]
        payload = json.dumps({
            "query": {
                "sql": sql,
                "from": 0,
                "size": 1000,
                "start_time": start_time,
                "end_time": now_us,
            },
        }).encode()

        times = []
        error = None

        for run_idx in range(runs):
            start = time.perf_counter()
            try:
                req = urllib.request.Request(
                    "http://localhost:5080/api/default/_search?type=logs",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")
                req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")

                with urllib.request.urlopen(req, timeout=120) as resp:
                    resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
            except Exception as e:
                error = str(e)
                break

        if times:
            qr = QueryResult(
                name=name,
                system="openobserve",
                cold_ms=times[0],
                hot_ms=min(times[1:]) if len(times) > 1 else times[0],
                runs=times,
                error=error,
            )
        else:
            qr = QueryResult(name=name, system="openobserve", error=error)
        result.queries.append(qr)


def run_clickhouse_ingest(data_path: Path) -> BenchmarkResult:
    """Ingest K8s logs into ClickHouse."""
    result = BenchmarkResult(system="clickhouse")

    # Check if ClickHouse is running
    try:
        req = urllib.request.Request("http://127.0.0.1:8123/ping")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        result.error = "ClickHouse not running at localhost:8123"
        return result

    # Drop existing table (separate request - ClickHouse doesn't allow multi-statement)
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8123/",
            data=b"DROP TABLE IF EXISTS k8s_logs",
            method="POST",
        )
        urllib.request.urlopen(req, timeout=30)
    except Exception:
        pass  # Table might not exist

    # Create table with underscore column names (matching OpenObserve benchmark)
    create_sql = """CREATE TABLE k8s_logs (
    _timestamp UInt64,
    level LowCardinality(String),
    message String,
    kubernetes_namespace_name LowCardinality(String),
    kubernetes_pod_name String,
    kubernetes_container_name LowCardinality(String),
    kubernetes_labels_app LowCardinality(String),
    kubernetes_labels_version LowCardinality(String),
    kubernetes_node_name LowCardinality(String),
    trace_id String,
    span_id String,
    http_method LowCardinality(String),
    http_status UInt16,
    http_path LowCardinality(String),
    http_latency_ms UInt32,
    http_bytes_out UInt32,
    client_ip String,
    error_message Nullable(String),
    region LowCardinality(String),
    service_name LowCardinality(String),
    host_name String
) ENGINE = MergeTree()
ORDER BY (_timestamp)"""
    try:
        req = urllib.request.Request(
            "http://127.0.0.1:8123/",
            data=create_sql.encode(),
            method="POST",
        )
        urllib.request.urlopen(req, timeout=30)
    except Exception as e:
        result.error = f"Failed to create table: {e}"
        return result

    # Ingest data (with async_insert=0 to ensure fair sync comparison)
    start = time.perf_counter()
    try:
        with data_path.open("rb") as f:
            req = urllib.request.Request(
                "http://127.0.0.1:8123/?query=INSERT%20INTO%20k8s_logs%20FORMAT%20JSONEachRow&async_insert=0&wait_end_of_query=1",
                data=f.read(),
                method="POST",
            )
            req.add_header("Content-Type", "application/octet-stream")
            urllib.request.urlopen(req, timeout=600)

        result.ingest_time_s = time.perf_counter() - start

        # Get record count
        count_req = urllib.request.Request(
            "http://127.0.0.1:8123/?query=SELECT%20count()%20FROM%20k8s_logs",
        )
        with urllib.request.urlopen(count_req, timeout=30) as resp:
            result.records = int(resp.read().decode().strip())

        # Get storage size
        size_req = urllib.request.Request(
            "http://127.0.0.1:8123/?query=SELECT%20sum(bytes_on_disk)%20FROM%20system.parts%20WHERE%20table='k8s_logs'",
        )
        with urllib.request.urlopen(size_req, timeout=30) as resp:
            result.storage_mb = int(resp.read().decode().strip()) / (1024 * 1024)

    except Exception as e:
        result.error = str(e)

    return result


def run_clickhouse_queries(result: BenchmarkResult, runs: int = 3) -> None:
    """Run benchmark queries against ClickHouse with cold/hot measurement."""
    for name, q in QUERIES.items():
        sql = q["clickhouse"]
        times = []
        error = None

        for run_idx in range(runs):
            start = time.perf_counter()
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:8123/?query={urllib.parse.quote(sql)}",
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
            except Exception as e:
                error = str(e)
                break

        if times:
            qr = QueryResult(
                name=name,
                system="clickhouse",
                cold_ms=times[0],
                hot_ms=min(times[1:]) if len(times) > 1 else times[0],
                runs=times,
                error=error,
            )
        else:
            qr = QueryResult(name=name, system="clickhouse", error=error)
        result.queries.append(qr)


def print_results(results: list[BenchmarkResult], records: int) -> None:
    """Print comparison table with cold/hot query times."""
    print("\n" + "=" * 80)
    print(f"BENCHMARK RESULTS: {records:,} K8s-style log records")
    print("=" * 80)

    valid_results = [r for r in results if not r.error]

    # Ingest comparison
    print("\n## Ingest Performance")
    print(f"{'System':<15} {'Time (s)':<12} {'Records/s':<15} {'Storage (MB)':<15}")
    print("-" * 60)
    for r in results:
        if r.error:
            print(f"{r.system:<15} ERROR: {r.error[:40]}")
        else:
            rps = r.records / r.ingest_time_s if r.ingest_time_s > 0 else 0
            print(f"{r.system:<15} {r.ingest_time_s:<12.2f} {rps:<15,.0f} {r.storage_mb:<15.1f}")

    query_names = list(QUERIES.keys())

    # Cold query times
    print("\n## Cold Query Performance (ms, first run)")
    header = f"{'Query':<25}"
    for r in valid_results:
        header += f" {r.system:<15}"
    print(header)
    print("-" * (25 + 16 * len(valid_results)))

    for qname in query_names:
        row = f"{qname:<25}"
        for r in valid_results:
            qr = next((q for q in r.queries if q.name == qname), None)
            if qr and not qr.error:
                row += f" {qr.cold_ms:<15.1f}"
            elif qr:
                row += f" {'ERR':<15}"
            else:
                row += f" {'-':<15}"
        print(row)

    # Hot query times
    print("\n## Hot Query Performance (ms, best of subsequent runs)")
    header = f"{'Query':<25}"
    for r in valid_results:
        header += f" {r.system:<15}"
    print(header)
    print("-" * (25 + 16 * len(valid_results)))

    for qname in query_names:
        row = f"{qname:<25}"
        for r in valid_results:
            qr = next((q for q in r.queries if q.name == qname), None)
            if qr and not qr.error:
                row += f" {qr.hot_ms:<15.1f}"
            elif qr:
                row += f" {'ERR':<15}"
            else:
                row += f" {'-':<15}"
        print(row)

    # Speedup comparison (vs slowest, using hot times)
    if len(valid_results) > 1:
        print("\n## Relative Speedup (hot queries, higher is better)")
        header = f"{'Query':<25}"
        for r in valid_results:
            header += f" {r.system:<15}"
        print(header)
        print("-" * (25 + 16 * len(valid_results)))

        for qname in query_names:
            times = []
            for r in valid_results:
                qr = next((q for q in r.queries if q.name == qname and not q.error), None)
                if qr:
                    times.append((r.system, qr.hot_ms))

            if times:
                slowest = max(t[1] for t in times)
                row = f"{qname:<25}"
                for r in valid_results:
                    qr = next((q for q in r.queries if q.name == qname and not q.error), None)
                    if qr:
                        speedup = slowest / qr.hot_ms if qr.hot_ms > 0 else 0
                        row += f" {speedup:<15.1f}x"
                    else:
                        row += f" {'-':<15}"
                print(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="Cross-system K8s logs benchmark")
    parser.add_argument("--records", type=int, default=10_000_000, help="Number of records")
    parser.add_argument("--quick", action="store_true", help="Quick run with 1M records")
    parser.add_argument("--runs", type=int, default=3, help="Query runs (1=cold only, 3+=cold+hot)")
    parser.add_argument("--systems", default="rawduck,openobserve,clickhouse",
                        help="Comma-separated list of systems to test")
    parser.add_argument("--output", type=str, help="Output JSON path")
    parser.add_argument("--skip-ingest", action="store_true", help="Skip ingest, just run queries")
    args = parser.parse_args()

    records = 1_000_000 if args.quick else args.records
    runs = args.runs
    systems = [s.strip() for s in args.systems.split(",")]

    # Generate data
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data_path = DATA_DIR / f"k8s_logs_{records // 1_000_000}m.ndjson"
    if not data_path.exists():
        print(f"Generating {records:,} records...")
        # Import and run from same directory
        import importlib.util
        spec = importlib.util.spec_from_file_location("gen_k8s_logs", SCRIPT_DIR / "gen_k8s_logs.py")
        gen_k8s_logs = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(gen_k8s_logs)
        # Call main with proper args
        old_argv = sys.argv
        sys.argv = ["gen_k8s_logs.py", str(records), str(data_path)]
        gen_k8s_logs.main()
        sys.argv = old_argv

    print(f"\nData file: {data_path} ({data_path.stat().st_size / (1024*1024):.1f} MB)")

    results = []

    # RawDuck
    if "rawduck" in systems:
        print("\n### RawDuck")
        print("  Starting RawDuck server...")
        if not start_rawduck_server():
            r = BenchmarkResult(system="rawduck", error="Failed to start RawDuck server")
            results.append(r)
        else:
            try:
                if not args.skip_ingest:
                    print("  Ingesting...")
                    r = run_rawduck_ingest(data_path)
                    if r.error:
                        print(f"  ERROR: {r.error}")
                    else:
                        print(f"  Ingested {r.records:,} records in {r.ingest_time_s:.2f}s, storage: {r.storage_mb:.1f} MB")
                else:
                    r = BenchmarkResult(system="rawduck")
                    r.records = records

                if not r.error:
                    print(f"  Running queries ({runs} runs each)...")
                    run_rawduck_queries(r, runs=runs)
                results.append(r)
            finally:
                print("  Stopping RawDuck server...")
                stop_rawduck_server()

    # OpenObserve
    if "openobserve" in systems:
        print("\n### OpenObserve")
        if not args.skip_ingest:
            print("  Ingesting...")
            r = run_openobserve_ingest(data_path)
            if r.error:
                print(f"  ERROR: {r.error}")
            else:
                print(f"  Ingested {r.records:,} records in {r.ingest_time_s:.2f}s")
        else:
            r = BenchmarkResult(system="openobserve")
            r.records = records

        if not r.error:
            print(f"  Running queries ({runs} runs each)...")
            run_openobserve_queries(r, runs=runs)
        results.append(r)

    # ClickHouse
    if "clickhouse" in systems:
        print("\n### ClickHouse")
        if not args.skip_ingest:
            print("  Ingesting...")
            r = run_clickhouse_ingest(data_path)
            if r.error:
                print(f"  ERROR: {r.error}")
            else:
                print(f"  Ingested {r.records:,} records in {r.ingest_time_s:.2f}s, storage: {r.storage_mb:.1f} MB")
        else:
            r = BenchmarkResult(system="clickhouse")
            r.records = records

        if not r.error:
            print(f"  Running queries ({runs} runs each)...")
            run_clickhouse_queries(r, runs=runs)
        results.append(r)

    # Print results
    print_results(results, records)

    # Save results
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = RESULTS_DIR / f"k8s_comparison_{records // 1_000_000}m.json"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output = {
        "records": records,
        "query_runs": runs,
        "data_file": str(data_path),
        "data_size_mb": data_path.stat().st_size / (1024 * 1024),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": [
            {
                "system": r.system,
                "ingest_time_s": r.ingest_time_s,
                "storage_mb": r.storage_mb,
                "records": r.records,
                "error": r.error,
                "queries": [
                    {
                        "name": q.name,
                        "cold_ms": q.cold_ms,
                        "hot_ms": q.hot_ms,
                        "all_runs_ms": q.runs,
                        "error": q.error,
                    }
                    for q in r.queries
                ],
            }
            for r in results
        ],
    }
    with output_path.open("w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to: {output_path}")

    return 0


if __name__ == "__main__":
    import urllib.parse
    raise SystemExit(main())
