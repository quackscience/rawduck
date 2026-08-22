#!/usr/bin/env python3
"""Realistic OTEL streaming ingestion benchmark.

Unlike run_otel.sh (bulk NDJSON file load), this drives real OTLP/HTTP
traffic — via the actual OpenTelemetry Python SDK, protobuf wire format,
the SDK's default BatchSpanProcessor batching — into RawDuck's in-process
HTTP API (raw_serve). Concurrent worker *processes* (not threads: sidesteps
Python's GIL) each run an independent TracerProvider/exporter, modeling
multiple services/collectors exporting to the same RawDuck endpoint.

Requires the packages in otel_streaming_requirements.txt; run_otel_streaming.sh
manages a venv for this automatically.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
GEN_SCRIPT = Path(__file__).resolve().parent / "otel_gen_load.py"

_ENV = {**os.environ, "DUCKDB_NO_HIGHLIGHT": "1"}


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default)))


def duckdb_bin() -> Path:
    return _env_path("DUCKDB", ROOT / "build/release/duckdb")


def extension_path() -> Path:
    return _env_path("EXT", ROOT / "build/release/extension/rawduck/rawduck.duckdb_extension")


def work_dir() -> Path:
    return _env_path("BENCH_WORK", ROOT / "benchmark/work")


def results_dir() -> Path:
    return _env_path("BENCH_RESULTS", ROOT / "benchmark/results")


def escape_sql(s: str) -> str:
    return s.replace("'", "''")


class Server:
    """One interactive `duckdb` CLI process running raw_serve() for the
    benchmark's duration. Commands are piped over stdin (the CLI never sees
    EOF, so the listener thread stays alive) and responses are read back off
    stdout up to a marker line — the same pattern run_variant.py's DuckSession
    uses for cold->warm sessions, adapted for a long-lived server process.
    """

    def __init__(self, db_path: Path, host: str, port: int, token: str):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        for p in (db_path, Path(str(db_path) + ".wal")):
            if p.exists():
                p.unlink()
        argv = [str(duckdb_bin()), str(db_path), "-unsigned", "-batch", "-dark-mode"]
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=_ENV,
            bufsize=0,
            text=True,
        )
        self.host, self.port, self.token = host, port, token
        self._send(f"LOAD '{escape_sql(str(extension_path()))}';")
        self._send(f"CALL raw_serve(host := '{host}', port := {port}, token := '{token}');")
        self._wait_healthy()

    def _send(self, sql: str) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(sql.rstrip() + "\n")
        self.proc.stdin.flush()

    def _wait_healthy(self, timeout_s: float = 10.0) -> None:
        deadline = time.monotonic() + timeout_s
        url = f"http://{self.host}:{self.port}/health"
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=1) as resp:
                    if resp.status == 200:
                        return
            except Exception:
                time.sleep(0.2)
        raise RuntimeError("raw_serve did not become healthy in time")

    def query(self, sql: str) -> str:
        assert self.proc.stdin is not None and self.proc.stdout is not None
        marker = "__RAWDUCK_BENCH_DONE__"
        self._send(f"{sql}\nSELECT '{marker}';")
        lines = []
        while True:
            line = self.proc.stdout.readline()
            if not line:
                break
            line = line.rstrip("\n")
            if marker in line:
                break
            lines.append(line)
        return "\n".join(lines)

    def scalar_count(self, table: str) -> int:
        # -csv-ish minimal parse: last non-empty, non-box-drawing line with digits
        out = self.query(f"SELECT count(*) FROM {table};")
        for line in reversed(out.splitlines()):
            stripped = "".join(ch for ch in line if ch.isdigit())
            if stripped and stripped.isdigit():
                return int(stripped)
        raise RuntimeError(f"could not parse row count from: {out!r}")

    def close(self) -> None:
        try:
            self._send("CALL raw_serve_stop();")
            time.sleep(0.3)
            assert self.proc.stdin is not None
            self.proc.stdin.close()
            self.proc.wait(timeout=10)
        except Exception:
            self.proc.kill()


def host_info() -> dict:
    try:
        cores = os.cpu_count() or 0
    except Exception:
        cores = 0
    return {"host": platform.node(), "cpu": platform.machine(), "cores": cores}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--workers", type=int, default=16, help="concurrent exporter processes")
    ap.add_argument("--spans-per-worker", type=int, default=60000)
    ap.add_argument("--quick", action="store_true", help="small smoke run: 4 workers x 5000 spans")
    ap.add_argument("--port", type=int, default=19999)
    ap.add_argument("--token", default="rt_bench_secret")
    ap.add_argument("--python", default=os.environ.get("OTEL_VENV_PYTHON", sys.executable))
    ap.add_argument("--output", default=None, help="write JSON results here")
    args = ap.parse_args()

    if args.quick:
        args.workers, args.spans_per_worker = 4, 5000

    if not duckdb_bin().exists() or not extension_path().exists():
        print("Build release first: GEN=ninja make release", file=sys.stderr)
        return 1
    if not Path(args.python).exists():
        print(f"Missing python interpreter: {args.python}", file=sys.stderr)
        return 1

    db_path = work_dir() / f"otel_streaming_{os.getpid()}.db"
    server = Server(db_path, "127.0.0.1", args.port, args.token)
    try:
        endpoint = f"http://127.0.0.1:{args.port}/otlp/v1/traces"
        argvs = [
            [
                args.python, str(GEN_SCRIPT),
                "--endpoint", endpoint,
                "--token", args.token,
                "--count", str(args.spans_per_worker),
                "--seed", str(i),
            ]
            for i in range(args.workers)
        ]

        start = time.perf_counter()
        procs = [subprocess.Popen(argv, stderr=subprocess.PIPE, text=True) for argv in argvs]
        failures = []
        for i, p in enumerate(procs):
            _, err = p.communicate(timeout=300)
            if p.returncode != 0:
                failures.append((i, err))
        elapsed = time.perf_counter() - start

        if failures:
            for i, err in failures:
                print(f"worker {i} failed:\n{err}", file=sys.stderr)
            return 1

        total_spans = args.workers * args.spans_per_worker
        rows = server.scalar_count("otel_traces")
        spans_per_sec = total_spans / elapsed if elapsed > 0 else 0.0

        result = {
            "benchmark": "otel_streaming",
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "transport": "otlp/http/protobuf",
            "workers": args.workers,
            "spans_per_worker": args.spans_per_worker,
            "total_spans_sent": total_spans,
            "rows_ingested": rows,
            "wall_seconds": round(elapsed, 3),
            "spans_per_sec": round(spans_per_sec),
            **host_info(),
        }
        print(json.dumps(result, indent=2))

        if rows != total_spans:
            print(f"WARNING: sent {total_spans} spans but only {rows} rows landed", file=sys.stderr)

        output = args.output
        if output is None:
            ts = result["timestamp"].replace(":", "")
            output = str(results_dir() / f"otel_streaming_{total_spans}_{host_info()['host']}_{ts}.json")
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        Path(output).write_text(json.dumps(result, indent=2) + "\n")
        print(f"Wrote {output}", file=sys.stderr)
        return 0 if rows == total_spans else 1
    finally:
        server.close()
        for p in (db_path, Path(str(db_path) + ".wal")):
            if p.exists():
                p.unlink()


if __name__ == "__main__":
    sys.exit(main())
