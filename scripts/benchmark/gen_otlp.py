#!/usr/bin/env python3
"""Generate OTLP/JSON export envelopes for RawDuck OTEL benchmarks.

Usage:
  python3 gen_otlp.py traces 1000000   -> benchmark/data/traces_1m.ndjson
  python3 gen_otlp.py all 1000000      -> traces, logs, metrics
"""
from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path

random.seed(11)

SVC = ["checkout", "cart", "payments", "search", "auth", "inventory", "shipping", "frontend"]
RT = ["/api/v1/orders", "/api/v1/cart", "/api/v1/pay", "/api/v1/search", "/login", "/health"]
METRICS = ["http.server.duration", "process.cpu.time", "db.client.connections", "queue.depth"]


def kv(key, value):
    if isinstance(value, bool):
        return {"key": key, "value": {"boolValue": value}}
    if isinstance(value, int):
        return {"key": key, "value": {"intValue": str(value)}}
    if isinstance(value, float):
        return {"key": key, "value": {"doubleValue": value}}
    return {"key": key, "value": {"stringValue": str(value)}}


def res(svc: str) -> dict:
    return {
        "attributes": [
            kv("service.name", svc),
            kv("deployment.environment", "production"),
            kv("cloud.region", "us-east-1"),
            kv("host.name", f"pod-{random.randint(1, 400)}"),
        ]
    }


def span(ts: int) -> dict:
    st = random.choice([200, 200, 200, 201, 400, 404, 500])
    dur = random.randint(2 * 10**5, 8 * 10**8)
    return {
        "traceId": os.urandom(16).hex(),
        "spanId": os.urandom(8).hex(),
        "name": random.choice(RT),
        "kind": random.randint(1, 5),
        "startTimeUnixNano": str(ts),
        "endTimeUnixNano": str(ts + dur),
        "attributes": [
            kv("http.method", random.choice(["GET", "POST", "PUT", "DELETE"])),
            kv("http.route", random.choice(RT)),
            kv("http.status_code", st),
            kv("retry", random.choice([True, False])),
        ],
        "status": {"code": 2 if st >= 500 else 1},
    }


def log_record(ts: int) -> dict:
    st = random.choice([200, 201, 400, 404, 500])
    return {
        "timeUnixNano": str(ts),
        "severityNumber": random.randint(9, 17),
        "severityText": random.choice(["INFO", "WARN", "ERROR"]),
        "body": {"stringValue": random.choice(["request ok", "cache miss", "timeout", "retry"])},
        "attributes": [
            kv("http.status_code", st),
            kv("http.route", random.choice(RT)),
            kv("pod", f"pod-{random.randint(1, 400)}"),
        ],
    }


def metric_point(ts: int) -> dict:
    return {
        "name": random.choice(METRICS),
        "unit": random.choice(["ms", "s", "1"]),
        "sum": {
            "aggregationTemporality": 2,
            "isMonotonic": True,
            "dataPoints": [
                {
                    "startTimeUnixNano": str(ts - 10**9),
                    "timeUnixNano": str(ts),
                    "asDouble": random.random() * 1000,
                    "attributes": [
                        kv("service.name", random.choice(SVC)),
                        kv("http.route", random.choice(RT)),
                    ],
                }
            ],
        },
    }


def write_envelopes(path: Path, total: int, per_line: int, record_fn, wrap_fn) -> None:
    ts = 1_700_000_000_000_000_000
    written = 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        while written < total:
            n = min(per_line, total - written)
            svc = random.choice(SVC)
            records = [record_fn(ts + (written + j) * 1000) for j in range(n)]
            f.write(json.dumps(wrap_fn(svc, records), separators=(",", ":")) + "\n")
            written += n


def traces_path(out_dir: Path, total: int) -> Path:
    path = out_dir / f"traces_{total // 1000}k.ndjson"
    write_envelopes(
        path,
        total,
        80,
        span,
        lambda svc, spans: {"resourceSpans": [{"resource": res(svc), "scopeSpans": [{"spans": spans}]}]},
    )
    return path


def logs_path(out_dir: Path, total: int) -> Path:
    path = out_dir / f"logs_{total // 1000}k.ndjson"
    write_envelopes(
        path,
        total,
        100,
        log_record,
        lambda svc, records: {"resourceLogs": [{"resource": res(svc), "scopeLogs": [{"logRecords": records}]}]},
    )
    return path


def metrics_path(out_dir: Path, total: int) -> Path:
    path = out_dir / f"metrics_{total // 1000}k.ndjson"
    write_envelopes(
        path,
        total,
        120,
        metric_point,
        lambda svc, metrics: {"resourceMetrics": [{"resource": res(svc), "scopeMetrics": [{"metrics": metrics}]}]},
    )
    return path


def main() -> int:
    if len(sys.argv) < 3:
        print(f"usage: {sys.argv[0]} <traces|logs|metrics|all> <record_count> [output_dir]", file=sys.stderr)
        return 2
    signal = sys.argv[1]
    total = int(sys.argv[2])
    out_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else Path(__file__).resolve().parents[2] / "benchmark" / "data"

    makers = {
        "traces": traces_path,
        "logs": logs_path,
        "metrics": metrics_path,
    }
    if signal == "all":
        for name, maker in makers.items():
            p = maker(out_dir, total)
            print(p)
        return 0
    if signal not in makers:
        print(f"unknown signal: {signal}", file=sys.stderr)
        return 2
    print(makers[signal](out_dir, total))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
