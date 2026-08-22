#!/usr/bin/env python3
"""Generate real OTLP traffic via the actual OpenTelemetry Python SDK and
export it to a RawDuck raw_serve() endpoint.

This is deliberately the real SDK (TracerProvider, BatchSpanProcessor,
OTLPSpanExporter) rather than RawDuck's own NDJSON generator: it exercises
the exact wire format (OTLP/HTTP protobuf, the SDK default) and envelope
shape a production OpenTelemetry Collector/SDK actually sends.

Each invocation is one process = one TracerProvider = one exporter. Run
multiple processes concurrently (see run_otel_streaming.py) to generate
load beyond what a single process's GIL allows.
"""
from __future__ import annotations

import argparse
import random
import sys
import time

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.trace import SpanKind, StatusCode

SERVICES = ["checkout", "cart", "payments", "search", "auth", "inventory", "shipping", "frontend"]
ROUTES = ["/api/v1/orders", "/api/v1/cart", "/api/v1/pay", "/api/v1/search", "/login", "/health"]
METHODS = ["GET", "POST", "PUT", "DELETE"]
STATUSES = [200, 200, 200, 201, 400, 404, 500]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--endpoint", required=True, help="http://host:port/otlp/v1/traces")
    ap.add_argument("--token", required=True)
    ap.add_argument("--count", type=int, required=True, help="spans to emit from this process")
    ap.add_argument("--service", default=None, help="fix the service name (else random per process)")
    ap.add_argument("--seed", type=int, default=0)
    ap.add_argument("--batch-size", type=int, default=2048, help="OTLP export batch size")
    ap.add_argument("--queue-size", type=int, default=200_000, help="BatchSpanProcessor max_queue_size")
    ap.add_argument("--schedule-delay-ms", type=int, default=200)
    args = ap.parse_args()

    random.seed(args.seed)
    service = args.service or random.choice(SERVICES)
    resource = Resource.create(
        {
            "service.name": service,
            "deployment.environment": "production",
            "cloud.region": "us-east-1",
            "host.name": f"pod-{random.randint(1, 400)}",
        }
    )
    exporter = OTLPSpanExporter(endpoint=args.endpoint, headers={"Authorization": f"Bearer {args.token}"})
    processor = BatchSpanProcessor(
        exporter,
        max_queue_size=args.queue_size,
        max_export_batch_size=args.batch_size,
        schedule_delay_millis=args.schedule_delay_ms,
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(processor)
    tracer = provider.get_tracer("rawduck-bench")

    start = time.perf_counter()
    for _ in range(args.count):
        route = random.choice(ROUTES)
        status = random.choice(STATUSES)
        with tracer.start_as_current_span(route, kind=SpanKind.SERVER) as span:
            span.set_attribute("http.method", random.choice(METHODS))
            span.set_attribute("http.route", route)
            span.set_attribute("http.status_code", status)
            span.set_attribute("retry", random.choice([True, False]))
            if status >= 500:
                span.set_status(StatusCode.ERROR)
    gen_elapsed = time.perf_counter() - start

    flush_start = time.perf_counter()
    provider.force_flush()
    provider.shutdown()
    flush_elapsed = time.perf_counter() - flush_start

    total = time.perf_counter() - start
    print(
        f"generated={args.count} gen_s={gen_elapsed:.3f} flush_s={flush_elapsed:.3f} total_s={total:.3f}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
