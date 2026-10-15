# RawDuck Benchmark

Primary workload: **OTEL telemetry** (OTLP/JSON logs, metrics, traces). GH Archive is a
wide-schema stress test in the appendix.

Published numbers are Apple **M3 Ultra** (32 cores, 512 GiB), default RawDuck
settings. **Do not mix pins:** `main` is DuckDB **v1.5.5**; this branch’s product
lock is DuckDB **v2.0-cyanoptera** `60c58d1` (v2.0.0-dev88475). Cold ingest =
first `raw_ingest_file` in a fresh database; warm = second ingest in the same
process after `DELETE` (`columns_added = 0`). Report the best of N sessions
unless noted. See `scripts/benchmark/README.md` for metric definitions.

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
sessions. **`main` pin (DuckDB v1.5.5):**

| signal | records | source NDJSON | cold ingest | records/s | throughput |
|---|---:|---:|---:|---:|---:|
| traces | 1,000,000 | 435 MB | 1.19 s | 841k | 366 MB/s |
| logs | 1,000,000 | 294 MB | 0.87 s | 1.15M | 338 MB/s |
| metrics | 1,000,000 | 353 MB | 1.13 s | 889k | 314 MB/s |

3M telemetry records in **3.2 s** (~940k records/s average). Warm ingest matches cold within
~2% on each signal.

### Branch `v2.0.0` ingest (August pin `21447bd`, historical peak)

Same machine and harness: VARIANT overflow sink + OTLP metrics shredded to
data-point grain (typed columns, not a `dataPoints` blob). Historical ingest
peak; the freeze-line lock is the `60c58d1` table below.

| signal | cold ingest | records/s | throughput | vs v1.5.5 |
|---|---:|---:|---:|---:|
| traces | 0.80 s | 1.25M | 543 MB/s | 1.5× |
| logs | 0.45 s | 2.20M | 646 MB/s | 1.9× |
| metrics | 0.51 s | 1.96M | 690 MB/s | 2.2× |

3M telemetry records in **~1.77 s** (~1.7M records/s average). Warm within a few percent of
cold on every signal.

### Branch `v2.0.0` freeze line (`60c58d1` / v2.0.0-dev88475)

Default v2 storage, `run_otel.sh --records 1000000 --runs 1` (this tip’s lock):

| signal | cold ingest | records/s | warm records/s |
|---|---:|---:|---:|
| traces | 0.85 s | 1.17M | 1.20M |
| logs | 0.49 s | 2.04M | 2.10M |
| metrics | 0.54 s | 1.85M | 1.75M |

Keep ingest ≥ 98% of this table. Queries: see the `60c58d1` row under Query speed.

## Query speed (1,000,000 spans)

Same `run_variant.py` SQL on this machine (M3 Ultra, 32 cores). New process after
`CHECKPOINT`. Traces table is 15 typed columns, **zero overflow**. Storage version
(`v1.5.0` vs DuckDB default) does not change the query numbers on either pin.

**Do not mix these rows.** The old “1.5 ms RawDuck” figure is the v1.5.5 pin, not v2.

| pin | harness | errors / p99 / status (ms) | traces ingest |
|---|---|---|---|
| `main` DuckDB v1.5.5 | best of 7, `--paths rawduck` | 1.1 / 2.6 / 2.4 | 0.84–1.01M rec/s |
| `v2.0.0` @ `21447bd` (v1.5.6-dev11341) | best of 3 | 3.8 / 5.1 / 3.5 | 1.25M rec/s |
| `v2.0.0` cyanoptera (v2.0.0-dev84550) | best of 7 | 4.5 / 5.8 / 4.4 | 1.20–1.23M rec/s |
| `v2.0.0` cyanoptera `60c58d1` (v2.0.0-dev88475) | best of 7, dict GROUP BY | **1.2 / 3.2 / 2.4** | 1.17–1.20M rec/s |

JSON-flat `->>` on the same shredded rows is ~36 / 88 / 32 ms on cyanoptera (~39 / 99 / 35
on v1.5.5) — the JSON control did not regress; only the shredded scan did. Nested VARIANT
OTLP stays 90–230× slower than typed columns (KeyValue lists were never shredded).
VARIANT-flat (already-shredded rows stored as VARIANT) ties RawDuck on the v2 line; that
is DuckDB scanning a flat shredded blob, not a reason to stop shredding at ingest.

v2 query leverage on this tip: `rawduck_dictionary_groupby` (default on) keeps DICT_FSST
VARCHAR group keys as dictionaries instead of `__internal_compress_string_uhugeint`
(status **ties** the v1.5.5 lock; errors 1.2 vs 1.1). After `raw_optimize()` clusters by
observed filters, integers RLE and zone maps prune — errors **0.87 ms**, status **0.90 ms**.
The remaining p99 gap (3.2 vs 2.6) is `quantile_cont` over 1M BIGINT durations, not extract.

v1.5.5 vs JSON `->>` (same queries; v1.5.5 pin):

| query | JSON `->>` | RawDuck v1.5.5 | speedup |
|---|---:|---:|---:|
| error count by service (`status>=500`) | 39 ms | 1.1 ms | 35× |
| p99 latency by route | 99 ms | 2.6 ms | 38× |
| status-code distribution | 35 ms | 2.4 ms | 15× |
| storage | 143 MB | 35–40 MB | ~3.6× smaller |

Cyanoptera query knobs (threads 1/4/8/32, `ANALYZE`, in-memory `CTAS`) do not recover
the v1.5.5 latency. Do not merge `v2.0.0` into `main` until that table’s cyanoptera
row matches `main`.

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

## VARIANT vs RawDuck (DuckDB v1.5.5 pin)

Same 1,000,000 OTLP/JSON trace spans. These VARIANT/JSON comparison rows are the
v1.5.5 pin. RawDuck query lock for that pin is the table in **Query speed** above
(1.1 / 2.6 / 2.4 ms, best of 7); the 1.5 / 3.2 / 2.5 ms row below is the earlier
best-of-3 publish of the same SQL.

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

### Same comparison on DuckDB v2.0-cyanoptera (`60c58d1` / v2.0.0-dev88475)

This freeze-line tip is the `v2.0.0` product lock. Ingest: traces 1.17M cold /
1.20M warm, logs 2.04M, metrics 1.85M (`run_otel.sh --records 1000000 --runs 1`).
Queries on default v2 storage: **1.4 / 3.4 / 2.4 ms** (best of 3) — much closer
to the v1.5.5 lock than the earlier cyanoptera 4.5 / 5.8 / 4.4 row, still not
a merge into `main`. VARIANT-flat ties RawDuck because both are already shredded;
nested VARIANT OTLP does not. The harness labels `pragma_version()` so result
JSON is not mixed with v1.5.5.

Product RawDuck queries use DuckDB default storage (`--paths rawduck` implies
`--storage-version default`). VARIANT comparison tables still need
`STORAGE_VERSION 'v1.5.0'`. That choice does not change RawDuck query ms. Do not
copy databases across pins.

```sh
git checkout v2.0.0 && git submodule update --init --recursive
GEN=ninja make release
./build/release/test/unittest --test-dir . "test/sql/*"

./scripts/benchmark/run_variant.sh --records 1000000 --runs 1 --query-runs 7 --paths rawduck
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
```

Overflow columns on this branch are VARIANT (sink only). Keep `main` on the JSON
sink. OTLP metrics fan out to data-point grain so `dataPoints` are typed columns.

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
