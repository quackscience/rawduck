#!/usr/bin/env python3
"""Run one cold + warm OTEL ingest in a single DuckDB session.

Cold: first export batch (schema discovery). Warm: second batch in the same
process with fresh timestamps and the same column shape (append, no DDL).
"""
from __future__ import annotations

import subprocess
import sys


def escape_sql_path(path: str) -> str:
    return path.replace("'", "''")


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
    sql = f"""
LOAD '{ext_sql}';
SELECT 'mark0', epoch_ms(current_timestamp);
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('{table}', '{cold_file}', transform := '{transform}');
CHECKPOINT;
SELECT 'mark1', epoch_ms(current_timestamp);
DELETE FROM {table};
SELECT 'mark2', epoch_ms(current_timestamp);
SELECT rows, columns_added, columns_widened, errors
FROM raw_ingest_file('{table}', '{warm_file}', transform := '{transform}');
CHECKPOINT;
SELECT 'mark3', epoch_ms(current_timestamp);
"""
    proc = subprocess.run(
        [duckdb, db_path, "-unsigned", "-batch", "-csv", "-noheader"],
        input=sql.encode(),
        capture_output=True,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr.decode())
        sys.stderr.write(proc.stdout.decode())
        raise SystemExit(proc.returncode)

    marks: list[int] = []
    ingest_rows: list[tuple[int, int, int, int]] = []
    for line in proc.stdout.decode().splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(",")
        if len(parts) == 2 and parts[0].startswith("mark"):
            marks.append(int(float(parts[1])))
            continue
        if len(parts) >= 4 and parts[0].isdigit():
            ingest_rows.append(
                (
                    int(parts[0]),
                    int(parts[1]),
                    int(parts[2]),
                    int(parts[3]),
                )
            )

    if len(marks) != 4 or len(ingest_rows) != 2:
        sys.stderr.write(proc.stdout.decode())
        sys.stderr.write(proc.stderr.decode())
        raise SystemExit(f"unexpected benchmark output: marks={len(marks)} ingests={len(ingest_rows)}")

    cold_sec = max((marks[1] - marks[0]) / 1000.0, 0.0)
    warm_sec = max((marks[3] - marks[2]) / 1000.0, 0.0)
    cold_rows, cold_added, cold_widened, cold_errors = ingest_rows[0]
    warm_rows, warm_added, warm_widened, warm_errors = ingest_rows[1]

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
