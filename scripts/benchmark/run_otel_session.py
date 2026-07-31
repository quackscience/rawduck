#!/usr/bin/env python3
"""Run one cold + warm OTEL ingest in a single DuckDB process.

Cold: first export (schema discovery). Warm: second export with shifted timestamps
and the absorbed schema (same process — ObjectCache stays hot).

Host-side time.perf_counter() brackets each timed window (ingest + CHECKPOINT).
DELETE between cold and warm is untimed so warm measures empty-table re-ingest
into an evolved schema, not append onto 2× data.

Warm must report columns_added=0 and columns_widened=0 or the session fails.
"""
from __future__ import annotations

import subprocess
import sys
import time


def escape_sql_path(path: str) -> str:
    return path.replace("'", "''")


def _read_until_done(stdout) -> list[str]:
    lines: list[str] = []
    while True:
        raw = stdout.readline()
        if not raw:
            raise RuntimeError("duckdb process ended before __bench_done__")
        text = raw.decode(errors="replace").strip()
        if not text:
            continue
        if text == "__bench_done__" or text.startswith("__bench_done__,"):
            break
        lines.append(text)
    return lines


def _exec(proc: subprocess.Popen, sql: str) -> list[str]:
    assert proc.stdin is not None and proc.stdout is not None
    proc.stdin.write((sql.rstrip() + "\nSELECT '__bench_done__';\n").encode())
    proc.stdin.flush()
    return _read_until_done(proc.stdout)


def _parse_ingest(lines: list[str]) -> tuple[int, int, int, int]:
    for line in reversed(lines):
        parts = line.split(",")
        if len(parts) >= 4 and parts[0].lstrip("-").isdigit():
            return int(parts[0]), int(parts[1]), int(parts[2]), int(parts[3])
    raise RuntimeError(f"no ingest result row in output: {lines!r}")


def run_session(
    duckdb: str,
    ext: str,
    db_path: str,
    table: str,
    cold_path: str,
    warm_path: str,
    transform: str,
) -> tuple[dict, dict]:
    cold_file = escape_sql_path(cold_path)
    warm_file = escape_sql_path(warm_path)
    ext_sql = escape_sql_path(ext)

    proc = subprocess.Popen(
        [duckdb, db_path, "-unsigned", "-batch", "-csv", "-noheader"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None

    cold_sec = warm_sec = 0.0
    cold_rows = cold_added = cold_widened = cold_errors = 0
    warm_rows = warm_added = warm_widened = warm_errors = 0

    try:
        _exec(proc, f"LOAD '{ext_sql}';")

        cold_sql = f"""
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('{table}', '{cold_file}', transform := '{transform}');
CHECKPOINT;
"""
        t0 = time.perf_counter()
        cold_lines = _exec(proc, cold_sql)
        cold_sec = time.perf_counter() - t0
        cold_rows, cold_added, cold_widened, cold_errors = _parse_ingest(cold_lines)

        _exec(proc, f"DELETE FROM {table};")

        warm_sql = f"""
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('{table}', '{warm_file}', transform := '{transform}');
CHECKPOINT;
"""
        t1 = time.perf_counter()
        warm_lines = _exec(proc, warm_sql)
        warm_sec = time.perf_counter() - t1
        warm_rows, warm_added, warm_widened, warm_errors = _parse_ingest(warm_lines)

        if warm_added != 0 or warm_widened != 0:
            raise SystemExit(
                f"warm ingest mutated schema (columns_added={warm_added}, "
                f"columns_widened={warm_widened}); shape must be stable"
            )
    except BaseException:
        err = ""
        if proc.stderr is not None:
            try:
                err = proc.stderr.read().decode(errors="replace")
            except Exception:
                pass
        if err:
            sys.stderr.write(err)
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        raise
    finally:
        if proc.poll() is None:
            try:
                proc.stdin.close()
            except Exception:
                pass
            try:
                proc.wait(timeout=30)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()

    cold = {
        "seconds": cold_sec,
        "rows": cold_rows,
        "columns_added": cold_added,
        "columns_widened": cold_widened,
        "errors": cold_errors,
    }
    warm = {
        "seconds": warm_sec,
        "rows": warm_rows,
        "columns_added": warm_added,
        "columns_widened": warm_widened,
        "errors": warm_errors,
    }
    return cold, warm


def main() -> int:
    if len(sys.argv) != 8:
        print(
            f"usage: {sys.argv[0]} DUCKDB EXT DB TABLE COLD_PATH WARM_PATH TRANSFORM",
            file=sys.stderr,
        )
        return 2
    duckdb, ext, db_path, table, cold_path, warm_path, transform = sys.argv[1:8]
    cold, warm = run_session(duckdb, ext, db_path, table, cold_path, warm_path, transform)
    print(
        f"{cold['seconds']:.6f},{cold['rows']},{cold['columns_added']},{cold['columns_widened']},{cold['errors']}"
    )
    print(
        f"{warm['seconds']:.6f},{warm['rows']},{warm['columns_added']},{warm['columns_widened']},{warm['errors']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
