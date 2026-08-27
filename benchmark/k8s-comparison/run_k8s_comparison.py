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

# Warn if the dataset's newest record is older than this. Must stay below OpenObserve's
# ZO_INGEST_ALLOWED_UPTO (default 5h) with room for the run itself.
MAX_DATA_AGE_HOURS = 1.0

# Seconds to wait after ingest before the first query. The benchmark deliberately
# measures read performance on a freshly-ingested, unsettled store, so this is 0 - but it
# is applied identically to every system so the scenario means the same thing for each.
# Residual lag (verification queries, per-run drift) is recorded per measurement as
# since_ingest_s rather than eliminated.
POST_INGEST_DELAY_S = 0.0


@dataclass
class QueryResult:
    name: str
    system: str
    cold_ms: float = 0  # First run (cold cache)
    hot_ms: float = 0   # Best of subsequent runs (hot cache)
    runs: list[float] = field(default_factory=list)  # All run times
    # Seconds between end of ingest and the start of each run, parallel to `runs`.
    # Background compaction keeps progressing during the sweep, so a later run is served
    # from a more settled store - this makes that visible instead of silent.
    since_ingest_s: list[float] = field(default_factory=list)
    rows: int = 0  # Result rows returned (0 => treated as an error, see finish_query)
    error: str | None = None


@dataclass
class BenchmarkResult:
    system: str
    ingest_time_s: float = 0
    storage_mb: float = 0
    records: int = 0  # Records the harness believes it sent
    verified_records: int = 0  # Records the system actually reports holding
    short_by: int = 0  # Records sent but not visible at verification time (not an error)
    # Logical (pre-compression) size. Only OpenObserve reports this directly; left at 0
    # for the others rather than guessed at.
    storage_uncompressed_mb: float = 0
    ingest_end: float | None = None  # perf_counter() at the moment ingest finished
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
    # Row fetch queries (specific columns to avoid JSON serialization overhead skewing results)
    "recent_errors": {
        "desc": "Fetch recent errors",
        "rawduck": "SELECT _timestamp, level, service_name, message, error_message FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
        "openobserve": "SELECT _timestamp, level, service_name, message, error_message FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
        "clickhouse": "SELECT _timestamp, level, service_name, message, error_message FROM k8s_logs WHERE level = 'ERROR' ORDER BY _timestamp DESC LIMIT 100",
    },
    "service_sample": {
        "desc": "Sample rows for a specific service",
        "rawduck": "SELECT _timestamp, level, message, http_status, http_latency_ms FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
        "openobserve": "SELECT _timestamp, level, message, http_status, http_latency_ms FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
        "clickhouse": "SELECT _timestamp, level, message, http_status, http_latency_ms FROM k8s_logs WHERE service_name = 'api-gateway' LIMIT 100",
    },
    "high_latency": {
        "desc": "Find high latency requests",
        "rawduck": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
        "openobserve": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
        "clickhouse": "SELECT service_name, http_latency_ms, http_path FROM k8s_logs WHERE http_latency_ms > 1000 ORDER BY http_latency_ms DESC LIMIT 100",
    },
}


def data_time_range_ns(data_path: Path) -> tuple[int, int]:
    """Return (min_ts_ns, max_ts_ns) of the NDJSON data file.

    gen_k8s_logs.py emits strictly increasing timestamps, so the first and last lines
    bound the range. Reading both avoids assuming anything about when the file was made.
    """
    with data_path.open("rb") as f:
        first = f.readline()
        try:
            f.seek(-min(65536, data_path.stat().st_size), 2)
        except OSError:
            f.seek(0)
        tail = f.read().splitlines()
    last = next(line for line in reversed(tail) if line.strip())
    return json.loads(first)["_timestamp"], json.loads(last)["_timestamp"]


def count_rows(system: str, body: bytes) -> int:
    """Number of result rows in a query response, per system response format.

    A query that returns nothing is not a fast query - it means the store is empty or
    the time filter excluded everything. Callers treat 0 as an error, not a fast result.
    """
    if system == "rawduck":
        # /v1/query -> {"meta": ..., "data": [...], "rows": N, "statistics": ...}
        doc = json.loads(body)
        if isinstance(doc.get("rows"), int):
            return doc["rows"]
        return len(doc.get("data") or [])
    if system == "openobserve":
        # /_search -> {"hits": [...], "total": N, ...}
        return len(json.loads(body).get("hits") or [])
    if system == "clickhouse":
        # default HTTP format is TabSeparated: one line per row
        return sum(1 for line in body.splitlines() if line.strip())
    raise ValueError(f"unknown system: {system}")


def openobserve_storage_mb() -> tuple[float, float]:
    """Return (on_disk_mb, uncompressed_mb) for the k8s_logs stream.

    GET /api/{org}/streams returns {"list": [{"name", "stats": {...}}], "total": N}
    (ListStream, src/common/src/meta/stream.rs:82). StreamStats carries storage_size /
    compressed_size / index_size, accumulated from FileMeta byte counts - but the service
    layer divides all three by SIZE_IN_MB before returning (transform_stats,
    src/stream/src/lib.rs:1011-1019), so these are already MEGABYTES, not bytes.

    on_disk = compressed_size + index_size, the analogue of ClickHouse's bytes_on_disk
    (part files including indexes) and of RawDuck's .db file size. Data still in the WAL
    is not counted, matching the other two reporting only what is on disk right now.
    """
    req = urllib.request.Request("http://localhost:5080/api/default/streams?type=logs")
    req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")
    with urllib.request.urlopen(req, timeout=30) as resp:
        doc = json.loads(resp.read())

    entries = doc.get("list") if isinstance(doc, dict) else doc
    for entry in entries or []:
        if entry.get("name") != "k8s_logs":
            continue
        stats = entry.get("stats") or {}
        compressed = float(stats.get("compressed_size") or 0.0)
        index = float(stats.get("index_size") or 0.0)
        uncompressed = float(stats.get("storage_size") or 0.0)
        return compressed + index, uncompressed
    return 0.0, 0.0


def verify_stored_records(system: str, data_range_ns: tuple[int, int]) -> int:
    """Ask a system how many rows it actually holds.

    The ingest paths report what the harness *sent*. That is not the same as what the
    system *stored* - records can be silently discarded. Every empty-result and speedup
    number downstream is meaningless if the store is short, so this is checked directly.
    """
    sql = "SELECT count(*) as cnt FROM k8s_logs"
    if system == "rawduck":
        req = urllib.request.Request(
            f"http://127.0.0.1:{RAWDUCK_PORT}/v1/query",
            data=json.dumps({"sql": sql}).encode(),
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", f"Bearer {RAWDUCK_TOKEN}")
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read()).get("data") or []
        row = data[0] if data else None
        if isinstance(row, dict):
            return int(next(iter(row.values())))
        return int(row[0]) if row else 0

    if system == "clickhouse":
        req = urllib.request.Request(
            f"http://127.0.0.1:8123/?query={urllib.parse.quote(sql)}",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            return int(resp.read().decode().strip())

    if system == "openobserve":
        min_ns, max_ns = data_range_ns
        margin_us = 60 * 1_000_000
        payload = json.dumps({
            "query": {
                "sql": sql,
                "from": 0,
                "size": 1,
                "start_time": (min_ns // 1000) - margin_us,
                "end_time": (max_ns // 1000) + margin_us,
            },
        }).encode()
        req = urllib.request.Request(
            "http://localhost:5080/api/default/_search?type=logs",
            data=payload,
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")
        with urllib.request.urlopen(req, timeout=120) as resp:
            hits = json.loads(resp.read()).get("hits") or []
        return int(next(iter(hits[0].values()))) if hits else 0

    raise ValueError(f"unknown system: {system}")


def finish_query(system: str, name: str, times: list[float], rows: int,
                 error: str | None, since_ingest: list[float] | None = None) -> QueryResult:
    """Build a QueryResult, downgrading an empty result set to an error.

    An empty result is recorded as an error so it shows as ERR in the tables instead of
    contributing a misleadingly fast latency to the comparison.
    """
    since_ingest = since_ingest or []
    if error is None and times and rows == 0:
        error = "empty result (0 rows) - not counted as a valid timing"
    if not times or error is not None:
        return QueryResult(name=name, system=system, runs=times,
                           since_ingest_s=since_ingest, error=error)
    return QueryResult(
        name=name,
        system=system,
        cold_ms=times[0],
        hot_ms=min(times[1:]) if len(times) > 1 else times[0],
        runs=times,
        since_ingest_s=since_ingest,
        rows=rows,
        error=None,
    )


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
        result.ingest_end = time.perf_counter()

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
        since_ingest = []
        error = None
        rows = 0

        for run_idx in range(runs):
            start = time.perf_counter()
            if result.ingest_end is not None:
                since_ingest.append(start - result.ingest_end)
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:{RAWDUCK_PORT}/v1/query",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")
                req.add_header("Authorization", f"Bearer {RAWDUCK_TOKEN}")

                with urllib.request.urlopen(req, timeout=120) as resp:
                    body = resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
                    rows = count_rows("rawduck", body)
            except Exception as e:
                error = str(e)
                break

        result.queries.append(
            finish_query("rawduck", name, times, rows, error, since_ingest))


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

    # Ingest data in batches.
    #
    # OpenObserve discards any record whose timestamp falls outside
    # [now - ZO_INGEST_ALLOWED_UPTO, now + ZO_INGEST_ALLOWED_IN_FUTURE] (defaults 5h/24h,
    # see handle_timestamp in src/core/src/logs/ingest.rs). Discards are reported per
    # batch in the response body as status[].failed, NOT as an HTTP error - so the body
    # must be read, or a fully-discarded ingest looks like a complete one.
    batch_size = 10000
    start = time.perf_counter()
    records_sent = 0
    accepted = 0
    rejected = 0

    def post_batch(batch: list[dict]) -> tuple[int, int]:
        payload = json.dumps(batch).encode()
        req = urllib.request.Request(
            "http://localhost:5080/api/default/k8s_logs/_json",
            data=payload,
            method="POST",
        )
        req.add_header("Content-Type", "application/json")
        req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = resp.read()
        ok = bad = 0
        try:
            for st in json.loads(body).get("status") or []:
                ok += int(st.get("successful") or 0)
                bad += int(st.get("failed") or 0)
        except (ValueError, TypeError):
            pass
        return ok, bad

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
                    ok, bad = post_batch(batch)
                    accepted += ok
                    rejected += bad
                    records_sent += len(batch)
                    batch = []

                    if records_sent % 100000 == 0:
                        print(f"  OpenObserve: {records_sent:,} records ingested...")

            # Final batch
            if batch:
                ok, bad = post_batch(batch)
                accepted += ok
                rejected += bad
                records_sent += len(batch)

        result.ingest_time_s = time.perf_counter() - start
        result.ingest_end = time.perf_counter()
        result.records = records_sent
        result.verified_records = accepted

        try:
            on_disk_mb, uncompressed_mb = openobserve_storage_mb()
            result.storage_mb = on_disk_mb
            result.storage_uncompressed_mb = uncompressed_mb
        except Exception as e:
            print(f"  WARNING: could not read OpenObserve stream storage stats: {e}")
        if rejected:
            result.error = (
                f"OpenObserve discarded {rejected:,} of {records_sent:,} records "
                f"(timestamp outside ZO_INGEST_ALLOWED_UPTO/IN_FUTURE window)"
            )

    except Exception as e:
        result.error = str(e)

    return result


def run_openobserve_queries(result: BenchmarkResult, data_range_ns: tuple[int, int],
                            runs: int = 3) -> None:
    """Run benchmark queries against OpenObserve with cold/hot measurement.

    OpenObserve's _search API requires an explicit time range. The window is derived from
    the actual min/max timestamp of the dataset rather than from "now", so that it always
    covers exactly the ingested data. A window ending at "now" silently excluded every
    record newer than the moment the query ran.

    OpenObserve normalises the nanosecond _timestamp values to microseconds on ingest
    (parse_i64_to_timestamp_micros in src/config/src/utils/time.rs branches on magnitude
    and divides ns by 1000), so the window must be expressed in microseconds.
    """
    min_ns, max_ns = data_range_ns
    margin_us = 60 * 1_000_000  # 1 minute either side
    start_time = (min_ns // 1000) - margin_us
    end_time = (max_ns // 1000) + margin_us

    for name, q in QUERIES.items():
        sql = q["openobserve"]
        payload = json.dumps({
            "query": {
                "sql": sql,
                "from": 0,
                "size": 1000,
                "start_time": start_time,
                "end_time": end_time,
            },
        }).encode()

        times = []
        since_ingest = []
        error = None
        rows = 0

        for run_idx in range(runs):
            start = time.perf_counter()
            if result.ingest_end is not None:
                since_ingest.append(start - result.ingest_end)
            try:
                req = urllib.request.Request(
                    "http://localhost:5080/api/default/_search?type=logs",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/json")
                req.add_header("Authorization", "Basic cm9vdEBleGFtcGxlLmNvbTpDb21wbGV4cGFzcyMxMjM=")

                with urllib.request.urlopen(req, timeout=120) as resp:
                    body = resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
                    rows = count_rows("openobserve", body)
            except Exception as e:
                error = str(e)
                break

        result.queries.append(
            finish_query("openobserve", name, times, rows, error, since_ingest))


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

    # Ingest data in chunks (large files can't be sent in one request)
    # Use 100k lines per chunk like RawDuck's batch approach for fair comparison
    chunk_size = 100000
    start = time.perf_counter()
    try:
        with data_path.open("rb") as f:
            lines = []
            for line in f:
                lines.append(line)
                if len(lines) >= chunk_size:
                    payload = b"".join(lines)
                    req = urllib.request.Request(
                        "http://127.0.0.1:8123/?query=INSERT%20INTO%20k8s_logs%20FORMAT%20JSONEachRow&async_insert=0&wait_end_of_query=1",
                        data=payload,
                        method="POST",
                    )
                    req.add_header("Content-Type", "application/octet-stream")
                    urllib.request.urlopen(req, timeout=300)
                    lines = []

            # Final chunk
            if lines:
                payload = b"".join(lines)
                req = urllib.request.Request(
                    "http://127.0.0.1:8123/?query=INSERT%20INTO%20k8s_logs%20FORMAT%20JSONEachRow&async_insert=0&wait_end_of_query=1",
                    data=payload,
                    method="POST",
                )
                req.add_header("Content-Type", "application/octet-stream")
                urllib.request.urlopen(req, timeout=300)

        result.ingest_time_s = time.perf_counter() - start
        result.ingest_end = time.perf_counter()

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
        since_ingest = []
        error = None
        rows = 0

        for run_idx in range(runs):
            start = time.perf_counter()
            if result.ingest_end is not None:
                since_ingest.append(start - result.ingest_end)
            try:
                req = urllib.request.Request(
                    f"http://127.0.0.1:8123/?query={urllib.parse.quote(sql)}",
                )
                with urllib.request.urlopen(req, timeout=120) as resp:
                    body = resp.read()
                    latency = (time.perf_counter() - start) * 1000
                    times.append(latency)
                    rows = count_rows("clickhouse", body)
            except Exception as e:
                error = str(e)
                break

        result.queries.append(
            finish_query("clickhouse", name, times, rows, error, since_ingest))


def await_post_ingest_delay(system: str) -> None:
    """Apply the fixed post-ingest delay, identically for every system.

    POST_INGEST_DELAY_S is 0 by design: the benchmark measures reads against a store that
    is still settling. The point of routing it through one function is that the scenario
    is defined once and is the same for all three systems.
    """
    if POST_INGEST_DELAY_S > 0:
        print(f"  Waiting {POST_INGEST_DELAY_S:.1f}s post-ingest ({system})...")
        time.sleep(POST_INGEST_DELAY_S)


def check_stored(result: BenchmarkResult, system: str, expected: int,
                 data_range_ns: tuple[int, int]) -> None:
    """Verify the system stored what the harness sent; flag the result if it did not."""
    try:
        stored = verify_stored_records(system, data_range_ns)
    except Exception as e:
        result.error = f"could not verify stored record count: {e}"
        print(f"  ERROR: {result.error}")
        return

    result.verified_records = stored
    result.short_by = max(0, expected - stored)

    if stored == 0:
        # Nothing to read: every timing below would be measuring an empty store.
        result.error = "system stored 0 records - all query timings would be meaningless"
        print(f"  ERROR: {result.error}")
    elif stored != expected:
        # Not an error. The benchmark queries a freshly-ingested store on purpose, so a
        # system that has not yet made every record visible is a legitimate result - but
        # it means this system answered over fewer rows than the others, so it is
        # recorded and reported rather than swallowed.
        pct = 100.0 * stored / expected if expected else 0.0
        print(f"  NOTE: {stored:,} of {expected:,} records visible at query time "
              f"({pct:.2f}%, short by {expected - stored:,}) - "
              f"this system reads over fewer rows than the others")
    else:
        print(f"  Verified: {stored:,} records stored")


def print_results(results: list[BenchmarkResult], records: int) -> None:
    """Print comparison table with cold/hot query times."""
    print("\n" + "=" * 80)
    print(f"BENCHMARK RESULTS: {records:,} K8s-style log records")
    print("=" * 80)

    valid_results = [r for r in results if not r.error]

    # Ingest comparison
    print("\n## Ingest Performance")
    print(f"{'System':<15} {'Time (s)':<12} {'Records/s':<15} {'Storage (MB)':<15} "
          f"{'Visible':<15} {'Short by':<12}")
    print("-" * 87)
    for r in results:
        if r.error:
            print(f"{r.system:<15} ERROR: {r.error[:65]}")
        else:
            rps = r.records / r.ingest_time_s if r.ingest_time_s > 0 else 0
            short = f"{r.short_by:,}" if r.short_by else "-"
            print(f"{r.system:<15} {r.ingest_time_s:<12.2f} {rps:<15,.0f} "
                  f"{r.storage_mb:<15.1f} {r.verified_records:<15,} {short:<12}")
    if any(r.storage_uncompressed_mb for r in results if not r.error):
        print("\nStorage is the on-disk footprint at query time. OpenObserve also reports "
              "an\nuncompressed size; both are snapshots taken while compaction is still "
              "in progress.")
        for r in results:
            if not r.error and r.storage_uncompressed_mb:
                ratio = (r.storage_uncompressed_mb / r.storage_mb) if r.storage_mb else 0
                print(f"  {r.system}: {r.storage_mb:.1f} MB on disk, "
                      f"{r.storage_uncompressed_mb:.1f} MB uncompressed"
                      + (f" ({ratio:.1f}x)" if ratio else ""))

    if any(r.short_by for r in results if not r.error):
        print("\nNOTE: a system short of records answered over fewer rows than the "
              "others.\n      Ingest timings are diagnostic only - this benchmark "
              "compares read performance.")

    # Time-since-ingest. The store keeps compacting during the sweep, so a query measured
    # late is served from a more settled store than one measured early. Reported so the
    # comparison is not silently confounded by sweep position.
    print("\n## Time Since Ingest (s, per query sweep)")
    print(f"{'System':<15} {'First run':<14} {'Last run':<14} {'Sweep span':<14}")
    print("-" * 60)
    for r in valid_results:
        lags = [t for q in r.queries for t in q.since_ingest_s]
        if not lags:
            print(f"{r.system:<15} {'-':<14} {'-':<14} {'-':<14}")
            continue
        print(f"{r.system:<15} {min(lags):<14.2f} {max(lags):<14.2f} "
              f"{max(lags) - min(lags):<14.2f}")

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
                        row += f" {f'{speedup:.1f}x':<15}"
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

    # Generate data.
    #
    # A stale file must be regenerated, not reused: OpenObserve discards records older
    # than ZO_INGEST_ALLOWED_UPTO (default 5h), so yesterday's file would ingest as an
    # empty stream while still reporting a full, fast run.
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    data_path = DATA_DIR / f"k8s_logs_{records // 1_000_000}m.ndjson"

    # Warn but do NOT regenerate: each system is benchmarked in a separate invocation of
    # this script, so silently regenerating here would give different phases different
    # data. If a stale file really does get discarded on ingest, check_stored catches it.
    if data_path.exists():
        age_h = (time.time() - data_time_range_ns(data_path)[1] / 1e9) / 3600
        if age_h > MAX_DATA_AGE_HOURS:
            print(f"WARNING: data file's newest record is {age_h:.1f}h old. OpenObserve "
                  f"discards records older than ZO_INGEST_ALLOWED_UPTO (default 5h). "
                  f"Delete {data_path} to regenerate.")

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

    data_range_ns = data_time_range_ns(data_path)
    print(f"\nData file: {data_path} ({data_path.stat().st_size / (1024*1024):.1f} MB)")
    print(f"Data time range: {data_range_ns[0] / 1e9:.0f} .. {data_range_ns[1] / 1e9:.0f} "
          f"(epoch s, {(data_range_ns[1] - data_range_ns[0]) / 3.6e12:.2f}h span)")

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
                    await_post_ingest_delay("rawduck")
                    check_stored(r, "rawduck", records, data_range_ns)
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
            await_post_ingest_delay("openobserve")
            check_stored(r, "openobserve", records, data_range_ns)
        if not r.error:
            print(f"  Running queries ({runs} runs each)...")
            run_openobserve_queries(r, data_range_ns, runs=runs)
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
            await_post_ingest_delay("clickhouse")
            check_stored(r, "clickhouse", records, data_range_ns)
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
    new_result_blocks = [
        {
            "system": r.system,
            "ingest_time_s": r.ingest_time_s,
            "storage_mb": r.storage_mb,
            "storage_uncompressed_mb": r.storage_uncompressed_mb,
            "records": r.records,
            "verified_records": r.verified_records,
            "short_by": r.short_by,
            "error": r.error,
            "queries": [
                {
                    "name": q.name,
                    "cold_ms": q.cold_ms,
                    "hot_ms": q.hot_ms,
                    "all_runs_ms": q.runs,
                    "since_ingest_s": q.since_ingest_s,
                    "rows": q.rows,
                    "error": q.error,
                }
                for q in r.queries
            ],
        }
        for r in results
    ]
    # run_full_benchmark.sh invokes this script once per system (--systems
    # rawduck, then --systems openobserve, then --systems clickhouse) against
    # the same default output path -- a plain overwrite here would leave only
    # the last phase's system in the file. Merge with whatever's already on
    # disk instead, keeping results for systems this run didn't touch and
    # replacing results for systems it did (so a single-system re-run, e.g.
    # --systems rawduck to redo a bad measurement, still updates in place).
    new_systems = {r.system for r in results}
    existing_blocks = []
    if output_path.exists():
        try:
            existing = json.loads(output_path.read_text())
            existing_blocks = [b for b in existing.get("results", []) if b.get("system") not in new_systems]
        except (json.JSONDecodeError, OSError):
            existing_blocks = []
    output = {
        "records": records,
        "query_runs": runs,
        "post_ingest_delay_s": POST_INGEST_DELAY_S,
        "data_file": str(data_path),
        "data_size_mb": data_path.stat().st_size / (1024 * 1024),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "results": existing_blocks + new_result_blocks,
    }
    with output_path.open("w") as f:
        json.dump(output, f, indent=2)
    print(f"\nResults saved to: {output_path}")

    return 0


if __name__ == "__main__":
    import urllib.parse
    raise SystemExit(main())
