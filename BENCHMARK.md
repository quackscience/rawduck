# RawDuck Benchmark

Primary workload: **OTEL telemetry** (OTLP/JSON logs, metrics, traces). GH Archive is a
wide-schema stress test in the appendix.

Published numbers are Apple **M3 Ultra** (32 cores, 512 GiB), default RawDuck
settings. Product lock is DuckDB **v2.0-cyanoptera** `60c58d1` (v2.0.0-dev88475)
with duration shred, INTEGER lattice, and status-code clustering. Cold ingest =
first `raw_ingest_file` in a fresh database; warm = second ingest in the same
process after `DELETE` (`columns_added = 0`). Report the best of N sessions
unless noted. See
`scripts/benchmark/README.md` for metric definitions.

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

1,000,000 records per signal, OTLP/JSON export envelopes (collector POST bodies).
Default v2 storage, `run_otel.sh --records 1000000`:

| signal | cold ingest | records/s | warm records/s |
|---|---:|---:|---:|
| traces | 0.84–0.85 s | 1.17–1.19M | 1.19–1.20M |
| logs | 0.49 s | 2.04M | 2.10M |
| metrics | 0.54 s | 1.85M | 1.75M |

Keep ingest ≥ 98% of this table. Queries: see Query speed. The traces table is
16 typed columns (including shredded `duration`), **zero overflow**. Compact
ints (`kind`, `http.status_code`, `status.code`, `duration` on this harness)
are INTEGER; UnixNano timestamps stay BIGINT.

## Query speed (1,000,000 spans)

Same `run_variant.py` SQL on this machine (M3 Ultra, 32 cores). New process after
`CHECKPOINT`. Traces table is 16 typed columns, **zero overflow**. p99 scans the
shredded `duration` INTEGER column.

| tip | harness | errors / p99 / status (ms) | traces ingest |
|---|---|---|---|
| cyanoptera `60c58d1`, dict GROUP BY | best of 7, `--paths rawduck` | 1.2 / 3.2 / 2.4 | 1.17–1.20M rec/s |
| duration shred + INTEGER lattice | best of 7, `--paths rawduck` | 1.2 / 3.0 / 2.1 | 1.19M rec/s |
| chunk cluster + INTEGER status GROUP BY | best of 7, `--paths rawduck` | **1.1 / 3.0 / 0.9** | **1.18M rec/s** |

The latest row is the product lock. `status_dist` groups the INTEGER
`"http.status_code"` column so v2 Perfect Hash Group By fires; ingest sorts
each chunk by that column so the `>= 500` filter zone-maps on the first scan.
JSON-flat `->>` on the same shredded rows is
~36 / 88 / 32 ms — typed columns stay ~30× faster. Nested VARIANT OTLP stays
90–230× slower (KeyValue lists were never shredded). VARIANT-flat (already-shredded
rows stored as VARIANT) can tie RawDuck; that is DuckDB scanning a flat blob, not
a reason to stop shredding at ingest.

v2 query leverage: `rawduck_dictionary_groupby` (default on) keeps DICT_FSST
VARCHAR group keys as dictionaries instead of `__internal_compress_string_uhugeint`.
Compact INTEGER columns densify `kind` / `http.status_code` / `duration` bitpacking.
After `raw_optimize()` clusters by observed filters, integers RLE and zone maps
prune — errors **0.87 ms**, status **0.90 ms** on the freeze line. Remaining p99
cost is `quantile_cont` over 1M durations.

### Reproduce

```sh
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5
# product query path (default storage; no VARIANT tables)
./scripts/benchmark/run_variant.sh --records 1000000 --runs 1 --query-runs 7 --paths rawduck
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

## VARIANT / JSON controls vs typed shred

Controls only — not the product path. Product lock is **Query speed** above.
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

Product RawDuck queries use DuckDB default storage (`--paths rawduck` implies
`--storage-version default`). VARIANT comparison tables still need
`STORAGE_VERSION 'v1.5.0'`. That choice does not change RawDuck query ms. Do not
copy databases across DuckDB pins.

```sh
git checkout v2.0.0 && git submodule update --init --recursive
GEN=ninja make release
./build/release/test/unittest --test-dir . "test/sql/*"

./scripts/benchmark/run_variant.sh --records 1000000 --runs 1 --query-runs 7 --paths rawduck
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
```

Overflow is VARIANT (sink only). OTLP metrics fan out to data-point grain so
`dataPoints` are typed columns. VARIANT-flat can tie typed shred because both
are already flat; nested VARIANT OTLP does not.

## Appendix: GH Archive (wide-schema stress test)

One hour of [GH Archive](https://www.gharchive.org/) data — 247,199 events / 956 MB NDJSON /
914 columns. Apple Silicon, 10 cores:

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
- Shallow duckdb clones without tags report `v0.0.1`; fetch the pin’s tags.
