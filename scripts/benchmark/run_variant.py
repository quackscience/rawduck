#!/usr/bin/env python3
"""Compare DuckDB VARIANT (v1.5) against RawDuck shredded tables.

Same OTLP/JSON NDJSON envelopes as run_otel.sh. VARIANT is measured as it
ships in DuckDB v1.5.5 (no v2.0 shredded execution / extraction pushdown).

Ingest grain is labeled explicitly: envelope rows are not span records.

Query encodings:
  rawduck            typed columns after otlp-traces shred
  variant_otlp       one VARIANT {resource, span} per span (KeyValue arrays kept)
  variant_otlp_pos   same table, positional attribute extract (generator-stable)
  variant_otlp_kv    same table, key lookup via list comprehension (honest OTLP)
  json_otlp / _pos / _kv   same shape stored as JSON
  variant_flat       VARIANT of the shredded RawDuck row (query/storage only)
  json_flat          JSON of the shredded RawDuck row (existing ->> baseline)

VARIANT columns require storage v1.5.0; every path uses that so sizes are comparable.
"""
from __future__ import annotations

import argparse
import json
import os
import platform
import select
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, str(default)))


def duckdb_bin() -> Path:
    return _env_path("DUCKDB", ROOT / "build/release/duckdb")


def extension_path() -> Path:
    return _env_path("EXT", ROOT / "build/release/extension/rawduck/rawduck.duckdb_extension")


def data_dir() -> Path:
    return _env_path("BENCH_DATA", ROOT / "benchmark/data")


def work_dir() -> Path:
    return _env_path("BENCH_WORK", ROOT / "benchmark/work")


def results_dir() -> Path:
    return _env_path("BENCH_RESULTS", ROOT / "benchmark/results")


def escape_sql(path: str) -> str:
    return path.replace("'", "''")


def explode_cte(file_sql: str) -> str:
    return f"""
WITH envelopes AS (
  SELECT unnest(resourceSpans) AS rs
  FROM read_json('{file_sql}', format='newline_delimited')
), scopes AS (
  SELECT rs.resource AS resource, unnest(rs.scopeSpans) AS ss
  FROM envelopes
), spans AS (
  SELECT resource, unnest(ss.spans) AS span
  FROM scopes
)
"""


# Query SQL per (query, encoding). Each must return columns (k, n).
QUERY_SQL = {
    "errors_by_service": {
        "rawduck": """
SELECT "resource.service.name" AS k, count(*) AS n
FROM traces
WHERE "http.status_code" >= 500
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_pos": """
SELECT CAST(payload.resource.attributes[1].value.stringValue AS VARCHAR) AS k, count(*) AS n
FROM t
WHERE CAST(payload.span.attributes[3].value.intValue AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_kv": """
SELECT CAST((
  [x->'value'->>'stringValue' FOR x IN CAST(payload.resource.attributes AS JSON[])
   IF x->>'key' = 'service.name']
)[1] AS VARCHAR) AS k, count(*) AS n
FROM t
WHERE CAST((
  [x->'value'->>'intValue' FOR x IN CAST(payload.span.attributes AS JSON[])
   IF x->>'key' = 'http.status_code']
)[1] AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_pos": """
SELECT CAST(payload->'resource'->'attributes'->0->'value'->>'stringValue' AS VARCHAR) AS k,
       count(*) AS n
FROM t
WHERE CAST(payload->'span'->'attributes'->2->'value'->>'intValue' AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_kv": """
SELECT CAST((
  [x->'value'->>'stringValue' FOR x IN CAST(payload->'resource'->'attributes' AS JSON[])
   IF x->>'key' = 'service.name']
)[1] AS VARCHAR) AS k, count(*) AS n
FROM t
WHERE CAST((
  [x->'value'->>'intValue' FOR x IN CAST(payload->'span'->'attributes' AS JSON[])
   IF x->>'key' = 'http.status_code']
)[1] AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
        "variant_flat": """
SELECT CAST(payload['resource.service.name'] AS VARCHAR) AS k, count(*) AS n
FROM t
WHERE CAST(payload['http.status_code'] AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
        "json_flat": """
SELECT j->>'resource.service.name' AS k, count(*) AS n
FROM t
WHERE CAST(j->>'http.status_code' AS BIGINT) >= 500
GROUP BY 1 ORDER BY 1
""",
    },
    "p99_by_route": {
        "rawduck": """
SELECT "http.route" AS k,
       quantile_cont(("endTimeUnixNano" - "startTimeUnixNano"), 0.99)::BIGINT AS n
FROM traces
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_pos": """
SELECT CAST(payload.span.attributes[2].value.stringValue AS VARCHAR) AS k,
       quantile_cont(
         CAST(payload.span.endTimeUnixNano AS BIGINT)
         - CAST(payload.span.startTimeUnixNano AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_kv": """
SELECT CAST((
  [x->'value'->>'stringValue' FOR x IN CAST(payload.span.attributes AS JSON[])
   IF x->>'key' = 'http.route']
)[1] AS VARCHAR) AS k,
       quantile_cont(
         CAST(payload.span.endTimeUnixNano AS BIGINT)
         - CAST(payload.span.startTimeUnixNano AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_pos": """
SELECT CAST(payload->'span'->'attributes'->1->'value'->>'stringValue' AS VARCHAR) AS k,
       quantile_cont(
         CAST(payload->'span'->>'endTimeUnixNano' AS BIGINT)
         - CAST(payload->'span'->>'startTimeUnixNano' AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_kv": """
SELECT CAST((
  [x->'value'->>'stringValue' FOR x IN CAST(payload->'span'->'attributes' AS JSON[])
   IF x->>'key' = 'http.route']
)[1] AS VARCHAR) AS k,
       quantile_cont(
         CAST(payload->'span'->>'endTimeUnixNano' AS BIGINT)
         - CAST(payload->'span'->>'startTimeUnixNano' AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "variant_flat": """
SELECT CAST(payload['http.route'] AS VARCHAR) AS k,
       quantile_cont(
         CAST(payload.endTimeUnixNano AS BIGINT)
         - CAST(payload.startTimeUnixNano AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_flat": """
SELECT j->>'http.route' AS k,
       quantile_cont(
         CAST(j->>'endTimeUnixNano' AS BIGINT)
         - CAST(j->>'startTimeUnixNano' AS BIGINT), 0.99)::BIGINT AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
    },
    "status_dist": {
        "rawduck": """
SELECT "http.status_code"::VARCHAR AS k, count(*) AS n
FROM traces
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_pos": """
SELECT CAST(payload.span.attributes[3].value.intValue AS VARCHAR) AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "variant_otlp_kv": """
SELECT CAST((
  [x->'value'->>'intValue' FOR x IN CAST(payload.span.attributes AS JSON[])
   IF x->>'key' = 'http.status_code']
)[1] AS VARCHAR) AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_pos": """
SELECT CAST(payload->'span'->'attributes'->2->'value'->>'intValue' AS VARCHAR) AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_otlp_kv": """
SELECT CAST((
  [x->'value'->>'intValue' FOR x IN CAST(payload->'span'->'attributes' AS JSON[])
   IF x->>'key' = 'http.status_code']
)[1] AS VARCHAR) AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "variant_flat": """
SELECT CAST(payload['http.status_code'] AS VARCHAR) AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
        "json_flat": """
SELECT j->>'http.status_code' AS k, count(*) AS n
FROM t
GROUP BY 1 ORDER BY 1
""",
    },
}

# Expected (groups, sum_n) for verification. p99 sum_n is not a row count.
QUERY_EXPECT = {
    "errors_by_service": {"groups": 8, "sum_n": None, "sum_is_error_rows": True},
    "p99_by_route": {"groups": 6, "sum_n": None, "sum_is_error_rows": False},
    "status_dist": {"groups": 5, "sum_n": None, "sum_is_error_rows": False},
}


class DuckSession:
    """Interactive DuckDB CLI session.

    New files must be ATTACH'd with STORAGE_VERSION v1.5.0 (VARIANT cannot
    persist on the default v1.0.0 format). Existing files are opened as the
    CLI's main database — re-ATTACH with STORAGE_VERSION deadlocks.
    """

    def __init__(self, binary: Path, ext: Path, db_path: Path, *, create: bool):
        self.db_path = db_path
        self._err_chunks: list[bytes] = []
        if create:
            argv = [str(binary), "-unsigned", "-batch", "-csv", "-noheader"]
        else:
            argv = [str(binary), str(db_path), "-unsigned", "-batch", "-csv", "-noheader"]
        self.proc = subprocess.Popen(
            argv,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if self.proc.stdin is None or self.proc.stdout is None:
            raise RuntimeError("failed to open duckdb pipes")
        threading.Thread(target=self._drain_stderr, daemon=True).start()
        self.exec("SET enable_progress_bar = false;")
        threads = os.environ.get("DUCKDB_THREADS", "").strip()
        if threads:
            self.exec(f"SET threads = {int(threads)};")
        self.exec(f"LOAD '{escape_sql(str(ext))}';")
        if create:
            self.exec(
                f"ATTACH '{escape_sql(str(db_path))}' AS bench (STORAGE_VERSION 'v1.5.0'); USE bench;"
            )

    def _drain_stderr(self) -> None:
        assert self.proc.stderr is not None
        while True:
            chunk = self.proc.stderr.read(4096)
            if not chunk:
                break
            self._err_chunks.append(chunk)

    def _read_until_done(self, timeout_sec: float = 1800.0) -> list[str]:
        lines: list[str] = []
        assert self.proc.stdout is not None
        deadline = time.monotonic() + timeout_sec
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError(
                    "timed out waiting for __bench_done__"
                    + (f"\n{self.stderr_text()}" if self.stderr_text() else "")
                )
            ready, _, _ = select.select([self.proc.stdout], [], [], min(remaining, 1.0))
            if not ready:
                if self.proc.poll() is not None:
                    raise RuntimeError(
                        "duckdb process ended before __bench_done__"
                        + (f"\n{self.stderr_text()}" if self.stderr_text() else "")
                    )
                continue
            raw = self.proc.stdout.readline()
            if not raw:
                err = self.stderr_text()
                raise RuntimeError(
                    "duckdb process ended before __bench_done__"
                    + (f"\n{err}" if err else "")
                )
            text = raw.decode(errors="replace").strip()
            if not text:
                continue
            if text == "__bench_done__" or text.startswith("__bench_done__,"):
                break
            lines.append(text)
        return lines

    def exec(self, sql: str) -> list[str]:
        assert self.proc.stdin is not None
        body = sql.rstrip()
        if not body.endswith(";"):
            body += ";"
        self.proc.stdin.write((body + "\nSELECT '__bench_done__';\n").encode())
        self.proc.stdin.flush()
        return self._read_until_done()

    def stderr_text(self) -> str:
        return b"".join(self._err_chunks).decode(errors="replace")

    def close(self) -> str:
        if self.proc.poll() is None:
            try:
                assert self.proc.stdin is not None
                self.proc.stdin.close()
            except Exception:
                pass
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()
        return self.stderr_text()

    def fail(self, message: str) -> None:
        err = self.close()
        if err:
            sys.stderr.write(err)
            if not err.endswith("\n"):
                sys.stderr.write("\n")
        raise RuntimeError(message)


def parse_count(lines: list[str]) -> int:
    for line in reversed(lines):
        token = line.split(",")[0].strip()
        if token.lstrip("-").isdigit():
            return int(token)
    raise RuntimeError(f"no count in output: {lines!r}")


def parse_ingest(lines: list[str]) -> tuple[int, int, int, int]:
    for line in reversed(lines):
        parts = line.split(",")
        if len(parts) >= 4 and parts[0].lstrip("-").isdigit():
            return int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
    raise RuntimeError(f"no ingest result row in output: {lines!r}")


def parse_db_size(lines: list[str]) -> dict:
    """Parse total_blocks, used_blocks, free_blocks, block_size from pragma_database_size."""
    for line in reversed(lines):
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 4 and all(p.lstrip("-").isdigit() for p in parts[:4]):
            total, used, free, block = (int(p) for p in parts[:4])
            return {
                "total_blocks": total,
                "used_blocks": used,
                "free_blocks": free,
                "block_size": block,
                "used_bytes": used * block,
            }
    raise RuntimeError(f"no database size row in output: {lines!r}")


DB_SIZE_SQL = (
    "SELECT total_blocks, used_blocks, free_blocks, block_size "
    "FROM pragma_database_size() WHERE total_blocks > 0 ORDER BY used_blocks DESC LIMIT 1;"
)


def parse_fingerprint(lines: list[str]) -> tuple[int, int]:
    """Parse `groups,sum_n` from the fingerprint SELECT."""
    for line in reversed(lines):
        parts = [p.strip() for p in line.split(",")]
        if len(parts) >= 2 and parts[0].lstrip("-").isdigit() and parts[1].lstrip("-").isdigit():
            return int(parts[0]), int(parts[1])
    raise RuntimeError(f"no fingerprint in output: {lines!r}")


def file_bytes(path: Path) -> int:
    return path.stat().st_size if path.exists() else 0


def ingest_sql(path_name: str, file_sql: str) -> str:
    if path_name == "rawduck":
        return f"""
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('traces', '{file_sql}', transform := 'otlp-traces');
CHECKPOINT;
"""
    if path_name == "variant_envelope":
        return f"""
CREATE TABLE IF NOT EXISTS t (payload VARIANT);
INSERT INTO t
SELECT json::VARIANT
FROM read_json('{file_sql}', format='newline_delimited', records='false', columns={{json:'JSON'}});
CHECKPOINT;
"""
    cte = explode_cte(file_sql)
    if path_name == "variant_otlp":
        return f"""
CREATE TABLE IF NOT EXISTS t (payload VARIANT);
INSERT INTO t
{cte}
SELECT {{'resource': resource, 'span': span}}::VARIANT AS payload FROM spans;
CHECKPOINT;
"""
    if path_name == "json_otlp":
        return f"""
CREATE TABLE IF NOT EXISTS t (payload JSON);
INSERT INTO t
{cte}
SELECT {{'resource': resource, 'span': span}}::JSON AS payload FROM spans;
CHECKPOINT;
"""
    raise ValueError(path_name)


def create_empty_sql(path_name: str) -> str:
    if path_name == "rawduck":
        return "-- table created by raw_ingest_file"
    if path_name == "variant_envelope":
        return "CREATE TABLE t (payload VARIANT);"
    if path_name == "variant_otlp":
        return "CREATE TABLE t (payload VARIANT);"
    if path_name == "json_otlp":
        return "CREATE TABLE t (payload JSON);"
    raise ValueError(path_name)


def count_sql(path_name: str) -> str:
    table = "traces" if path_name == "rawduck" else "t"
    return f"SELECT count(*) FROM {table};"


def delete_sql(path_name: str) -> str:
    table = "traces" if path_name == "rawduck" else "t"
    return f"DELETE FROM {table};"


def query_table_for(encoding: str) -> str | None:
    if encoding == "rawduck":
        return "rawduck"
    if encoding.startswith("variant_otlp"):
        return "variant_otlp"
    if encoding.startswith("json_otlp"):
        return "json_otlp"
    if encoding == "variant_flat":
        return "variant_flat"
    if encoding == "json_flat":
        return "json_flat"
    return None


def run_ingest_session(
    binary: Path,
    ext: Path,
    db_path: Path,
    path_name: str,
    cold_file: Path,
    warm_file: Path,
) -> dict:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    wal = Path(str(db_path) + ".wal")
    if wal.exists():
        wal.unlink()

    sess = DuckSession(binary, ext, db_path, create=True)
    cold_sql_file = escape_sql(str(cold_file))
    warm_sql_file = escape_sql(str(warm_file))
    try:
        if path_name != "rawduck":
            sess.exec(create_empty_sql(path_name))

        t0 = time.perf_counter()
        cold_lines = sess.exec(ingest_sql(path_name, cold_sql_file))
        cold_sec = time.perf_counter() - t0
        if path_name == "rawduck":
            cold_rows, added, widened, errors = parse_ingest(cold_lines)
        else:
            cold_rows = parse_count(sess.exec(count_sql(path_name)))
            added = widened = errors = 0
        cold_size = parse_db_size(sess.exec(DB_SIZE_SQL))

        sess.exec(delete_sql(path_name))

        t1 = time.perf_counter()
        warm_lines = sess.exec(ingest_sql(path_name, warm_sql_file))
        warm_sec = time.perf_counter() - t1
        if path_name == "rawduck":
            warm_rows, warm_added, warm_widened, _warm_errors = parse_ingest(warm_lines)
            if warm_added != 0 or warm_widened != 0:
                raise RuntimeError(
                    f"warm ingest mutated schema (columns_added={warm_added}, "
                    f"columns_widened={warm_widened}); shape must be stable"
                )
        else:
            warm_rows = parse_count(sess.exec(count_sql(path_name)))
        warm_size = parse_db_size(sess.exec(DB_SIZE_SQL))
    except Exception as exc:
        sess.fail(f"{path_name} ingest failed: {exc}")
        raise
    err = sess.close()
    if err and "Error" in err:
        sys.stderr.write(err)

    return {
        "path": path_name,
        "grain": "envelope" if path_name == "variant_envelope" else "span",
        "cold_seconds": round(cold_sec, 6),
        "warm_seconds": round(warm_sec, 6),
        "cold_rows": cold_rows,
        "warm_rows": warm_rows,
        "columns_added": added,
        "columns_widened": widened,
        "errors": errors,
        "file_bytes": file_bytes(db_path),
        "bytes": cold_size["used_bytes"],
        "used_bytes": cold_size["used_bytes"],
        "free_blocks": cold_size["free_blocks"],
        "used_blocks": cold_size["used_blocks"],
        "block_size": cold_size["block_size"],
        "note_storage": (
            "bytes/used_bytes is live data after cold CHECKPOINT (before DELETE). "
            "file_bytes is the file after warm re-ingest and may include free-list holes."
        ),
        "db": str(db_path),
        "warm_size": warm_size,
    }


def encode_flat(
    binary: Path,
    ext: Path,
    src_db: Path,
    dst_db: Path,
    encoding: str,
) -> dict:
    if dst_db.exists():
        dst_db.unlink()
    sess = DuckSession(binary, ext, dst_db, create=True)
    src = escape_sql(str(src_db))
    try:
        sess.exec(f"ATTACH '{src}' AS src (READ_ONLY);")
        if encoding == "variant_flat":
            sql = "CREATE TABLE t AS SELECT to_json(src.traces)::VARIANT AS payload FROM src.traces; CHECKPOINT;"
        elif encoding == "json_flat":
            sql = "CREATE TABLE t AS SELECT to_json(src.traces)::JSON AS j FROM src.traces; CHECKPOINT;"
        else:
            raise ValueError(encoding)
        t0 = time.perf_counter()
        sess.exec(sql)
        encode_sec = time.perf_counter() - t0
        rows = parse_count(sess.exec("SELECT count(*) FROM t;"))
        size = parse_db_size(sess.exec(DB_SIZE_SQL))
    except Exception as exc:
        sess.fail(f"{encoding} encode failed: {exc}")
        raise
    sess.close()
    return {
        "path": encoding,
        "grain": "span",
        "encode_seconds": round(encode_sec, 6),
        "rows": rows,
        "bytes": size["used_bytes"],
        "used_bytes": size["used_bytes"],
        "file_bytes": file_bytes(dst_db),
        "used_blocks": size["used_blocks"],
        "free_blocks": size["free_blocks"],
        "block_size": size["block_size"],
        "db": str(dst_db),
        "note": "query/storage encoding of already-shredded RawDuck rows; not an OTLP ingest path",
    }


def run_queries(
    binary: Path,
    ext: Path,
    db_path: Path,
    encoding: str,
    query_runs: int,
    expected_rows: int,
    error_rows: int | None,
) -> dict:
    sess = DuckSession(binary, ext, db_path, create=False)
    out: dict = {}
    try:
        for qname, variants in QUERY_SQL.items():
            sql = variants.get(encoding)
            if not sql:
                continue
            print(f"  {encoding} / {qname}...", file=sys.stderr, flush=True)
            wrapped = f"SELECT count(*) AS groups, coalesce(sum(n), 0)::BIGINT AS sum_n FROM ({sql}) q"
            # warmup
            sess.exec(wrapped)
            best = None
            groups = sum_n = 0
            for _ in range(query_runs):
                t0 = time.perf_counter()
                lines = sess.exec(wrapped)
                sec = time.perf_counter() - t0
                groups, sum_n = parse_fingerprint(lines)
                if best is None or sec < best:
                    best = sec
            expect = QUERY_EXPECT[qname]
            ok = groups == expect["groups"]
            if qname == "status_dist":
                ok = ok and sum_n == expected_rows
            elif qname == "errors_by_service" and error_rows is not None:
                ok = ok and sum_n == error_rows
            out[qname] = {
                "seconds": round(best or 0.0, 6),
                "ms": round((best or 0.0) * 1000.0, 3),
                "groups": groups,
                "sum_n": sum_n,
                "ok": ok,
            }
    except Exception as exc:
        sess.fail(f"{encoding} query failed: {exc}")
        raise
    sess.close()
    return out


def git_info() -> tuple[str, str]:
    def _run(args: list[str]) -> str:
        try:
            return subprocess.check_output(args, cwd=ROOT, text=True).strip()
        except Exception:
            return "unknown"

    return _run(["git", "rev-parse", "HEAD"]), _run(["git", "rev-parse", "--abbrev-ref", "HEAD"])


def duckdb_version(binary: Path) -> str:
    try:
        out = subprocess.check_output(
            [str(binary), "-unsigned", "-csv", "-noheader", "-c", "SELECT library_version FROM pragma_version();"],
            text=True,
        )
        return out.strip().splitlines()[-1]
    except Exception:
        return "unknown"


def host_info() -> dict:
    info: dict = {
        "hostname": platform.node(),
        "os": platform.system(),
        "os_release": platform.release(),
        "machine": platform.machine(),
        "processor": platform.processor(),
        "python": platform.python_version(),
        "cpu_count": os.cpu_count(),
    }
    try:
        if sys.platform == "darwin":
            info["ram_bytes"] = int(
                subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()
            )
            info["cpu_brand"] = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
            ).strip()
        elif sys.platform.startswith("linux"):
            mem_kb = None
            with open("/proc/meminfo", encoding="utf-8") as fh:
                for line in fh:
                    if line.startswith("MemTotal:"):
                        mem_kb = int(line.split()[1])
                        break
            if mem_kb is not None:
                info["ram_bytes"] = mem_kb * 1024
            cpuinfo = Path("/proc/cpuinfo")
            if cpuinfo.is_file():
                for line in cpuinfo.read_text(encoding="utf-8", errors="replace").splitlines():
                    if line.lower().startswith("model name"):
                        info["cpu_brand"] = line.split(":", 1)[1].strip()
                        break
            lscpu = subprocess.run(["lscpu"], capture_output=True, text=True)
            if lscpu.returncode == 0:
                info["lscpu"] = lscpu.stdout.strip()
    except Exception:
        pass
    if info.get("ram_bytes"):
        info["ram_gib"] = round(info["ram_bytes"] / (1024**3), 1)
    return info


def ensure_traces(records: int) -> tuple[Path, Path]:
    data = data_dir()
    data.mkdir(parents=True, exist_ok=True)
    cold = data / f"traces_{records // 1000}k.ndjson"
    warm = data / f"traces_{records // 1000}k_warm.ndjson"
    gen = ROOT / "scripts/benchmark/gen_otlp.py"
    python = os.environ.get("PYTHON") or sys.executable
    if not cold.exists():
        subprocess.check_call([python, str(gen), "traces", str(records), str(data)])
    if not warm.exists():
        subprocess.check_call(
            [python, str(gen), "traces", str(records), str(data), "1700086400000000000", "_warm"]
        )
    return cold, warm


def rec_s(rows: int, seconds: float) -> int:
    return int(rows / seconds) if seconds > 0 else 0


def mb_s(nbytes: int, seconds: float) -> float:
    return round(nbytes / seconds / 1e6, 1) if seconds > 0 else 0.0


def best_of(runs: list[dict], key: str) -> dict:
    return min(runs, key=lambda r: r[key])


def print_summary(doc: dict) -> None:
    ingest = doc["ingest"]
    print("\n== ingest (best of sessions; CHECKPOINT included) ==", file=sys.stderr)
    print(
        f"{'path':<20} {'grain':<10} {'rows':>10} {'cold s':>10} {'cold rec/s':>12} {'warm s':>10} {'disk':>10}",
        file=sys.stderr,
    )
    for name, row in ingest.items():
        rows = row.get("cold_rows") or row.get("rows") or 0
        cold = row.get("cold_seconds")
        warm = row.get("warm_seconds")
        disk = row["bytes"] / (1 << 20)
        file_disk = row.get("file_bytes")
        extra = ""
        if file_disk and file_disk > row["bytes"] * 1.05:
            extra = f" ({file_disk / (1 << 20):.1f} file)"
        cold_s = f"{cold:.3f}" if cold is not None else "—"
        warm_s = f"{warm:.3f}" if warm is not None else "—"
        rate = f"{row.get('cold_records_per_sec') or rec_s(rows, cold or 0):,}" if cold else "—"
        print(
            f"{name:<20} {row.get('grain','?'):<10} {rows:>10,} {cold_s:>10} {rate:>12} {warm_s:>10} {disk:>8.1f} MB{extra}",
            file=sys.stderr,
        )

    print("\n== queries (best of N; ms) ==", file=sys.stderr)
    queries = doc["queries"]
    qnames = ["errors_by_service", "p99_by_route", "status_dist"]
    encodings = list(queries.keys())
    header = f"{'encoding':<22}" + "".join(f"{q[:16]:>16}" for q in qnames)
    print(header, file=sys.stderr)
    for enc in encodings:
        cells = []
        for q in qnames:
            cell = queries[enc].get(q)
            if not cell:
                cells.append(f"{'—':>16}")
            else:
                flag = "" if cell["ok"] else "!"
                cells.append(f"{cell['ms']:.1f}{flag}" .rjust(16))
        print(f"{enc:<22}" + "".join(cells), file=sys.stderr)
    print(
        "\nVARIANT here is DuckDB v1.5.5 (persisted shredded VARIANT). "
        "v2.0 extraction pushdown / shredded execution is not in this pin.",
        file=sys.stderr,
    )


def merge_best_ingest(acc: dict | None, row: dict) -> dict:
    if acc is None:
        return dict(row)
    if row["cold_seconds"] < acc["cold_seconds"]:
        for k in (
            "cold_seconds",
            "cold_rows",
            "columns_added",
            "columns_widened",
            "errors",
            "bytes",
            "used_bytes",
            "file_bytes",
            "free_blocks",
            "used_blocks",
            "block_size",
            "warm_size",
            "note_storage",
            "db",
        ):
            acc[k] = row[k]
    if row["warm_seconds"] < acc["warm_seconds"]:
        acc["warm_seconds"] = row["warm_seconds"]
        acc["warm_rows"] = row["warm_rows"]
    return acc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--records", type=int, default=1_000_000)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--query-runs", type=int, default=3)
    parser.add_argument("--quick", action="store_true", help="100k records, 1 ingest run")
    parser.add_argument("--output", type=str, default="", help="JSON output path (default: benchmark/results/variant_<n>_<host>_<ts>.json)")
    parser.add_argument("--skip-kv", action="store_true", help="skip honest KeyValue-lookup queries")
    parser.add_argument(
        "--threads",
        type=int,
        default=0,
        help="DuckDB worker threads (0 = DuckDB default / all cores). Pin this on many-core ARM.",
    )
    parser.add_argument(
        "--paths",
        type=str,
        default="rawduck,variant_otlp,json_otlp,variant_flat,json_flat",
    )
    args = parser.parse_args()
    if args.quick:
        args.records = 100_000
        args.runs = 1
    if args.threads > 0:
        os.environ["DUCKDB_THREADS"] = str(args.threads)

    binary = duckdb_bin()
    ext = extension_path()
    if not binary.is_file() or not os.access(binary, os.X_OK):
        print("Build release first: GEN=ninja make release", file=sys.stderr)
        return 1
    if not ext.is_file():
        print(f"Missing extension: {ext}", file=sys.stderr)
        return 1

    cold_file, warm_file = ensure_traces(args.records)
    src_bytes = file_bytes(cold_file)
    work = work_dir() / f"variant_{os.getpid()}"
    work.mkdir(parents=True, exist_ok=True)
    results_dir().mkdir(parents=True, exist_ok=True)

    ingest_paths = [p.strip() for p in args.paths.split(",") if p.strip()]
    encode_paths = [p for p in ingest_paths if p in ("variant_flat", "json_flat")]
    timed_ingest = [p for p in ingest_paths if p not in ("variant_flat", "json_flat")]

    commit, branch = git_info()
    version = duckdb_version(binary)
    host = host_info()
    ingest_best: dict[str, dict] = {}
    last_dbs: dict[str, Path] = {}

    for r in range(1, args.runs + 1):
        for path_name in timed_ingest:
            db = work / f"{path_name}_run{r}.db"
            print(f"ingest {path_name} run {r}/{args.runs}...", file=sys.stderr)
            row = run_ingest_session(binary, ext, db, path_name, cold_file, warm_file)
            ingest_best[path_name] = merge_best_ingest(ingest_best.get(path_name), row)
            last_dbs[path_name] = db

    encode_info: dict[str, dict] = {}
    raw_db = last_dbs.get("rawduck")
    if encode_paths and raw_db is None:
        print("variant_flat/json_flat require the rawduck path", file=sys.stderr)
        return 1
    for encoding in encode_paths:
        assert raw_db is not None
        db = work / f"{encoding}.db"
        print(f"encode {encoding}...", file=sys.stderr)
        encode_info[encoding] = encode_flat(binary, ext, raw_db, db, encoding)
        last_dbs[encoding] = db

    # Error-row count from rawduck (or first successful errors query).
    error_rows = None
    query_encodings = []
    for enc_group, src in (
        ("rawduck", "rawduck"),
        ("variant_otlp_pos", "variant_otlp"),
        ("variant_otlp_kv", "variant_otlp"),
        ("json_otlp_pos", "json_otlp"),
        ("json_otlp_kv", "json_otlp"),
        ("variant_flat", "variant_flat"),
        ("json_flat", "json_flat"),
    ):
        if src in last_dbs and any(enc_group == p or p == src for p in ingest_paths):
            if args.skip_kv and enc_group.endswith("_kv"):
                continue
            query_encodings.append((enc_group, last_dbs[src]))

    queries: dict[str, dict] = {}
    for encoding, db in query_encodings:
        print(f"query {encoding}...", file=sys.stderr)
        q = run_queries(binary, ext, db, encoding, args.query_runs, args.records, error_rows)
        if error_rows is None and "errors_by_service" in q and q["errors_by_service"]["ok"]:
            error_rows = q["errors_by_service"]["sum_n"]
        elif error_rows is not None and "errors_by_service" in q:
            q["errors_by_service"]["ok"] = (
                q["errors_by_service"]["ok"] and q["errors_by_service"]["sum_n"] == error_rows
            )
        queries[encoding] = q

    ingest_out = {}
    for name, row in ingest_best.items():
        recs = args.records if row["grain"] == "span" else row["cold_rows"]
        ingest_out[name] = {
            **row,
            "source_bytes": src_bytes,
            "cold_records_per_sec": rec_s(recs, row["cold_seconds"]),
            "warm_records_per_sec": rec_s(
                args.records if row["grain"] == "span" else row["warm_rows"],
                row["warm_seconds"],
            ),
            "cold_mb_per_sec": mb_s(src_bytes, row["cold_seconds"]),
            "warm_mb_per_sec": mb_s(src_bytes, row["warm_seconds"]),
        }
        if row["grain"] == "envelope":
            ingest_out[name]["note"] = (
                "row grain is OTLP export envelopes, not exploded spans; "
                "do not compare rec/s to RawDuck without dividing by spans-per-line"
            )
    for name, row in encode_info.items():
        ingest_out[name] = row

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    host_slug = host.get("hostname", "host").split(".")[0]
    ts_file = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    doc = {
        "benchmark": "variant_vs_rawduck",
        "timestamp": ts,
        "git_commit": commit,
        "git_branch": branch,
        "duckdb_version": version,
        "host": host,
        "variant_note": (
            "DuckDB VARIANT as of v1.5.5. The v2.0 preview (shredded execution from "
            "storage, extraction pushdown, Parquet shred, extra variant_* functions) "
            "is not in this pin."
        ),
        "records": args.records,
        "ingest_runs": args.runs,
        "query_runs": args.query_runs,
        "threads": int(os.environ["DUCKDB_THREADS"]) if os.environ.get("DUCKDB_THREADS") else None,
        "source": str(cold_file),
        "source_bytes": src_bytes,
        "storage_version": "v1.5.0",
        "ingest": ingest_out,
        "queries": queries,
    }

    out_path = (
        Path(args.output)
        if args.output
        else results_dir() / f"variant_{args.records}_{host_slug}_{ts_file}.json"
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(doc, indent=2) + "\n")
    print(f"Wrote {out_path}", file=sys.stderr)
    print_summary(doc)
    print(
        f"\nSend back: {out_path}\n"
        f"  git={commit}  branch={branch}  duckdb={version}\n"
        f"  host={host.get('hostname')}  cpu={host.get('cpu_brand') or host.get('machine')}  "
        f"cores={host.get('cpu_count')}  ram={host.get('ram_gib', '?')} GiB",
        file=sys.stderr,
    )

    shutil.rmtree(work, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
