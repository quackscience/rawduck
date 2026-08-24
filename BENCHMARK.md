# RawDuck Benchmark

Primary workload: **OTEL telemetry** (OTLP/JSON logs, metrics, traces). GH Archive is a
wide-schema stress test in the appendix.

All published numbers: DuckDB **v1.5.5**, default RawDuck settings, Apple **M3 Ultra**
(32 cores, 512 GiB) unless noted. Cold ingest = first `raw_ingest_file` in a fresh database;
warm = second ingest in the same process after `DELETE` (`columns_added = 0`). Report the
best of N sessions unless noted. See `scripts/benchmark/README.md` for metric definitions.

### Harness

```sh
GEN=ninja make release

# OTEL bulk ingest (NDJSON file)
./scripts/benchmark/run_otel.sh --quick
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5

# OTEL streaming ingest (OpenTelemetry SDK → raw_serve HTTP)
./scripts/benchmark/run_otel_streaming.sh --quick
./scripts/benchmark/run_otel_streaming.sh --workers 16 --spans-per-worker 60000

# VARIANT vs RawDuck (same trace dataset)
./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
```

## OTEL bulk ingest

1,000,000 records per signal, OTLP/JSON export envelopes (collector POST bodies), best of 5
sessions:

| signal | records | source NDJSON | cold ingest | records/s | throughput |
|---|---:|---:|---:|---:|---:|
| traces | 1,000,000 | 435 MB | 1.19 s | 841k | 366 MB/s |
| logs | 1,000,000 | 294 MB | 0.87 s | 1.15M | 338 MB/s |
| metrics | 1,000,000 | 353 MB | 1.13 s | 889k | 314 MB/s |

3M telemetry records in **3.2 s** (~940k records/s average). Warm ingest matches cold within
~2% on each signal.

### Query speed (1,000,000 spans)

Same spans — shredded typed columns vs one JSON object per span (`->>`), best of 3 runs:

| query | JSON `->>` | RawDuck | speedup |
|---|---:|---:|---:|
| error count by service (`status>=500`) | 39 ms | 1.5 ms | 26× |
| p99 latency by route | 99 ms | 3.2 ms | 31× |
| status-code distribution | 35 ms | 2.5 ms | 14× |
| storage | 143 MB | 39.5 MB | 3.6× smaller |

### Reproduce

```sh
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3   # queries + storage above
```

## OTEL streaming ingest

Real OTLP/HTTP **protobuf** traffic via the OpenTelemetry Python SDK into `raw_serve()`
(concurrent exporter processes, not bulk NDJSON):

| workers | spans | wall | spans/s |
|---:|---:|---:|---:|
| 4 | 20,000 | 0.75 s | 27k |
| 16 | 960,000 | 3.85 s | 250k |

`rows_ingested` must equal `total_spans_sent` (checked by the harness).

### Reproduce

```sh
./scripts/benchmark/run_otel_streaming.sh --workers 16 --spans-per-worker 60000
```

First run creates `benchmark/work/otel-streaming-venv` (OpenTelemetry SDK dependency).

## VARIANT vs RawDuck (DuckDB v1.5.5)

Same 1,000,000 OTLP/JSON trace spans. Paths:

| path | definition |
|---|---|
| RawDuck | `raw_ingest_file(..., transform := 'otlp-traces')` → typed columns |
| VARIANT OTLP | SQL unnest → one `VARIANT` `{resource, span}` per span (KeyValue arrays kept) |
| JSON OTLP | same exploded shape as `JSON` |
| VARIANT-flat | `to_json(traces)::VARIANT` of shredded RawDuck rows (encode/query only) |
| JSON-flat | same shredded rows as `JSON`, queried with `->>` |

Disk = `used_blocks × block_size` after cold `CHECKPOINT`. Parenthetical file size is after
warm re-ingest (includes free-list holes; not comparable across paths). VARIANT requires
`STORAGE_VERSION 'v1.5.0'`.

### Ingest + storage (1,000,000 spans)

| path | M3 Ultra | Spark GB10 aarch64 (`--threads 8`) |
|---|---|---|
| RawDuck | 0.99 s · 1.01M rec/s · 39.5 MB (108 MB file) | 1.18 s · 850k rec/s · 35.5 MB (91 MB file) |
| VARIANT-flat | encode · 35.8 MB | encode · 39.5 MB |
| VARIANT OTLP | 11.96 s · 84k · 53.5 MB (106 MB file) | 7.96 s · 126k · 54.5 MB (114 MB file) |
| JSON-flat | encode · 143 MB | encode · 142 MB |
| JSON OTLP | 4.72 s · 212k · 241 MB (484 MB file) | 5.71 s · 175k · 242 MB (484 MB file) |

Spark GB10: 20 cores, 122 GiB, Linux aarch64.

### Queries (best of 3, ms)

| encoding | M3 Ultra | Spark GB10 |
|---|---|---|
| | errors / p99 / status | errors / p99 / status |
| RawDuck | 1.5 / 3.2 / 2.5 | 1.3 / 4.9 / 5.1 |
| JSON-flat | 39 / 99 / 35 | 65 / 136 / 63 |
| JSON OTLP positional | 213 / 303 / 193 | 253 / 415 / 215 |
| JSON OTLP key lookup | 344 / 418 / 304 | 397 / 580 / 366 |
| VARIANT-flat | 436 / 1225 / 416 | 700 / 1988 / 696 |
| VARIANT OTLP positional | 1227 / 3479 / 1162 | 2013 / 5947 / 1852 |
| VARIANT OTLP key lookup | 1493 / 3658 / 1404 | 2107 / 5996 / 2049 |

### Reproduce

```sh
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 --threads 8   # many-core ARM
```

### Running the same comparison on DuckDB v2.0-dev

The numbers above are the v1.5.5 pin, where VARIANT has no shredded execution and
no extraction pushdown. Branch `v2.0.0` builds RawDuck against the DuckDB **main
tip (v2.0-dev)** so the same harness measures v2's VARIANT instead. Nothing about
the RawDuck path changes — only the DuckDB it links against — so the two result
files are directly comparable.

`run_variant.py` detects the pin from `pragma_version()` and labels the run
accordingly (`variant_note` in the result JSON, plus the printed summary line),
so a v2 run is never mistaken for a v1.5.5 one.

```sh
git checkout v2.0.0 && git submodule update --init --recursive
GEN=ninja make release
./build/release/test/unittest --test-dir . "test/sql/*"

./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
```

VARIANT columns still require `STORAGE_VERSION 'v1.5.0'`, so database files written
by a v2.0-dev run are not interchangeable with the v1.5.5 ones — measure storage per
branch, never by copying a database across pins.

## Appendix: GH Archive (wide-schema stress test)

One hour of [GH Archive](https://www.gharchive.org/) data — 247,199 events / 956 MB NDJSON /
914 columns. Apple Silicon, 10 cores, DuckDB v1.5.5:

| | JSON column | RawDuck | |
|---|---:|---:|---|
| count by event type | 231 ms | 1 ms | 231× |
| top repos by pushes | 268 ms | 3 ms | 89× |
| distinct repos per actor | 457 ms | 10 ms | 46× |
| sum of push payload sizes | 265 ms | 1 ms | 265× |
| events per minute | 236 ms | 3 ms | 79× |
| cold ingest | 1.4 s | ~13 s | one-time cost |
| warm re-ingest | — | ~4.9 s | steady state |
| storage | 1.05 GB | 636 MB | 40% smaller |

### Reproduce

```sh
curl -sL https://data.gharchive.org/2024-01-15-10.json.gz -o gh.json.gz
```

```sql
CALL raw_ingest_file('gh_events', 'gh.json.gz');
CHECKPOINT;
```

## Pitfalls

- Split NDJSON on `\n` only (not `splitlines()` — `\u2028`/`\u2029` appear in strings).
- Use `.timer on` via `duckdb -f script.sql` (not `duckdb -c`).
- Shallow duckdb clones without tags report `v0.0.1`; fetch tag `v1.5.5`.
