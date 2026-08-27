#!/usr/bin/env python3
"""Generate Kubernetes-style logs for cross-system benchmark (RawDuck vs OpenObserve vs ClickHouse).

Schema modeled after OpenObserve's benchmark: 25 columns of typical K8s observability data.

Usage:
  python3 gen_k8s_logs.py 10000000   -> benchmark/data/k8s_logs_10m.ndjson
  python3 gen_k8s_logs.py 1000000 benchmark/data/test.ndjson
"""
from __future__ import annotations

import json
import os
import random
import sys
import time
from pathlib import Path

random.seed(42)

# Realistic distributions
SERVICES = ["api-gateway", "user-service", "order-service", "payment-service",
            "inventory-service", "notification-service", "search-service", "auth-service",
            "cart-service", "shipping-service", "analytics-service", "recommendation-service"]
NAMESPACES = ["production", "staging", "default", "monitoring", "logging"]
LEVELS = ["INFO", "INFO", "INFO", "INFO", "WARN", "ERROR"]  # 67% INFO, 17% WARN, 17% ERROR
METHODS = ["GET", "GET", "GET", "POST", "PUT", "DELETE"]
ROUTES = ["/api/v1/users", "/api/v1/orders", "/api/v1/products", "/api/v1/cart",
          "/api/v1/payments", "/api/v1/search", "/api/v1/health", "/api/v1/metrics",
          "/api/v1/auth/login", "/api/v1/auth/logout", "/api/v1/notifications", "/api/v1/recommendations"]
STATUS_CODES = [200, 200, 200, 200, 201, 204, 400, 401, 403, 404, 500, 502, 503]
REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]
ENVIRONMENTS = ["production", "staging", "development"]
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) Safari/605.1.15",
    "python-requests/2.31.0",
    "curl/8.1.2",
    "grpc-go/1.59.0",
]
MESSAGES = [
    "Request completed successfully",
    "Processing request",
    "Cache hit for key",
    "Cache miss, fetching from database",
    "Database query executed",
    "Connection established",
    "Retrying request after timeout",
    "Rate limit exceeded",
    "Authentication successful",
    "Authorization failed",
    "Invalid request payload",
    "Internal server error occurred",
    "Service unavailable, circuit breaker open",
    "Request timed out",
    "Batch processing completed",
]
ERROR_MESSAGES = [
    None, None, None, None,  # 80% no error
    "Connection refused",
    "Timeout waiting for response",
    "Invalid JSON payload",
    "Database connection pool exhausted",
    "Service unavailable",
]


def generate_record(ts_ns: int, seq: int) -> dict:
    """Generate a single Kubernetes-style log record."""
    service = random.choice(SERVICES)
    level = random.choice(LEVELS)
    status = random.choice(STATUS_CODES)

    # Generate error message only for ERROR level or 5xx status
    error_msg = None
    if level == "ERROR" or status >= 500:
        error_msg = random.choice(ERROR_MESSAGES[4:])  # only error messages

    # Use underscores (not dots) for compatibility with all systems
    return {
        "_timestamp": ts_ns,
        "level": level,
        "message": random.choice(MESSAGES),
        "kubernetes_namespace_name": random.choice(NAMESPACES),
        "kubernetes_pod_name": f"{service}-{random.randint(1, 20):02d}-{os.urandom(3).hex()}",
        "kubernetes_container_name": service,
        "kubernetes_labels_app": service,
        "kubernetes_labels_version": f"v{random.randint(1, 5)}.{random.randint(0, 9)}.{random.randint(0, 99)}",
        "kubernetes_node_name": f"node-{random.randint(1, 50):03d}",
        "trace_id": os.urandom(16).hex(),
        "span_id": os.urandom(8).hex(),
        "http_method": random.choice(METHODS),
        "http_status": status,
        "http_path": random.choice(ROUTES),
        "http_latency_ms": random.randint(1, 5000),
        "http_bytes_out": random.randint(100, 50000),
        "client_ip": f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
        "error_message": error_msg,
        "region": random.choice(REGIONS),
        "service_name": service,
        "host_name": f"ip-10-{random.randint(0,255)}-{random.randint(0,255)}-{random.randint(1,254)}",
    }


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} <record_count> [output_path]", file=sys.stderr)
        return 2

    total = int(sys.argv[1])
    default_path = Path(__file__).resolve().parents[2] / "benchmark" / "data" / f"k8s_logs_{total // 1_000_000}m.ndjson"
    out_path = Path(sys.argv[2]) if len(sys.argv) > 2 else default_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    # Base timestamp: recent data (OpenObserve only accepts last few hours by default)
    import time as time_module
    ts_base = int(time_module.time() * 1_000_000_000) - (3 * 60 * 60 * 1_000_000_000)  # 3 hours ago
    ts_increment = (4 * 60 * 60 * 1_000_000_000) // total  # Spread over 4 hours

    print(f"Generating {total:,} records to {out_path}...")
    start = time.perf_counter()

    with out_path.open("w", encoding="utf-8") as f:
        for i in range(total):
            ts = ts_base + (i * ts_increment)
            record = generate_record(ts, i)
            f.write(json.dumps(record, separators=(",", ":")) + "\n")

            if (i + 1) % 1_000_000 == 0:
                elapsed = time.perf_counter() - start
                rate = (i + 1) / elapsed
                print(f"  {(i + 1) // 1_000_000}M records, {rate:,.0f} rec/s")

    elapsed = time.perf_counter() - start
    size_mb = out_path.stat().st_size / (1024 * 1024)
    print(f"Done: {total:,} records, {size_mb:.1f} MB, {elapsed:.1f}s ({total / elapsed:,.0f} rec/s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
