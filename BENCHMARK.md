# RawDuck Benchmark

RawDuck's bet: shred schema-less event JSON into real typed columns at ingest so every later query
runs at native columnar speed, instead of keeping opaque JSON and paying `->>` extraction on every
scan. The primary benchmark is the realistic workload — **OTEL telemetry** (OTLP/JSON logs, metrics,
traces) — with the GH Archive run kept below as a historical wide-schema stress test.

### Harness

```sh
GEN=ninja make release
./scripts/benchmark/run_otel.sh --quick                      # CI / remote smoke (100k)
./scripts/benchmark/compare.sh benchmark/results/smoke.json  # vs committed baseline
./scripts/benchmark/run_otel.sh --records 1000000 --runs 5   # full publishable numbers
```

Each session is **one DuckDB process**: cold ingest (schema discovery) then warm ingest (same
process, fresh timestamps, `columns_added = 0`). See `scripts/benchmark/README.md`.

## OTEL ingestion (primary)

Published results — multi-core commodity hardware, DuckDB v1.5.5, 1,000,000 records per signal,
OTLP/JSON export envelopes (the exact bytes an OpenTelemetry Collector posts to an OTLP/HTTP json
endpoint), default settings (no manual tuning):

| signal | records | columns | source NDJSON | ingest | records/s | throughput | on disk |
|---|---:|---:|---:|---:|---:|---:|---:|
| traces  | 1,000,000 | 15 | 435 MB | 0.74 s | 1.35M | 586 MB/s | 50 MB |
| logs    | 1,000,000 | 11 | 294 MB | 0.80 s | 1.26M | 369 MB/s | 9 MB |
| metrics | 1,000,000 | 9  | 353 MB | 1.03 s | 970k | 342 MB/s | 88 MB |

3M telemetry records shredded into typed columns in **2.6 s (~1.2M records/s)**. The OTLP transform
explodes the nested `resource → scope → record` envelopes, flattens KeyValue attributes
(`http.status_code`, `service.name`, …) into typed columns, and normalizes byte ids to hex — all in
the parallel parse stage. Because each NDJSON line is a fat export envelope that explodes into many
records, the loader batches by bytes (not just line count) so the parse threads stay fed
automatically; no `batch_size` tuning is needed.

### Query speed (1,000,000 spans)

Same spans, identical results — shredded typed columns vs the "just keep the JSON" baseline (one
JSON object per span, queried with `->>`):

| query | JSON `->>` | RawDuck | speedup |
|---|---:|---:|---:|
| error count by service (`status>=500`) | 71 ms | 2 ms | 36× |
| p99 latency by route | 136 ms | 5 ms | 27× |
| status-code distribution | 63 ms | 1 ms | 63× |
| storage | 232 MB | 38 MB | 6× smaller |

This baseline is the *favorable* one. Real OTLP keeps attributes as KeyValue **arrays**, so querying
them without shredding means `UNNEST`-ing the `resource → scope → span` nesting and scanning each
attribute array by key — hundreds of times slower than reading a typed column.

### Reproduce

Generate OTLP/JSON envelopes (one `Export*ServiceRequest` per line):

```python
# gen_otlp.py  ->  python3 gen_otlp.py 1000000
import json, os, random, sys
random.seed(11)
SVC = ["checkout","cart","payments","search","auth","inventory","shipping","frontend"]
RT  = ["/api/v1/orders","/api/v1/cart","/api/v1/pay","/api/v1/search","/login","/health"]
def kv(k,v):
    if isinstance(v,bool): return {"key":k,"value":{"boolValue":v}}
    if isinstance(v,int):  return {"key":k,"value":{"intValue":str(v)}}
    if isinstance(v,float):return {"key":k,"value":{"doubleValue":v}}
    return {"key":k,"value":{"stringValue":str(v)}}
def res(s): return {"attributes":[kv("service.name",s),kv("deployment.environment","production"),
                                  kv("cloud.region","us-east-1"),kv("host.name","pod-%d"%random.randint(1,400))]}
def span(ts):
    st=random.choice([200,200,200,201,400,404,500]); d=random.randint(2*10**5,8*10**8)
    return {"traceId":os.urandom(16).hex(),"spanId":os.urandom(8).hex(),"name":random.choice(RT),
            "kind":random.randint(1,5),"startTimeUnixNano":str(ts),"endTimeUnixNano":str(ts+d),
            "attributes":[kv("http.method",random.choice(["GET","POST","PUT","DELETE"])),
                          kv("http.route",random.choice(RT)),kv("http.status_code",st),
                          kv("retry",random.choice([True,False]))],"status":{"code":2 if st>=500 else 1}}
def gen(name,total,per,rec,wrap):
    ts=1_700_000_000_000_000_000; w=0
    with open(name+".ndjson","w") as f:
        while w<total:
            n=min(per,total-w)
            f.write(json.dumps(wrap(random.choice(SVC),[rec(ts+(w+j)*1000) for j in range(n)]))+"\n"); w+=n
n=int(sys.argv[1]) if len(sys.argv)>1 else 1_000_000
gen("traces",n,80,span,lambda s,r:{"resourceSpans":[{"resource":res(s),"scopeSpans":[{"spans":r}]}]})
# logs/metrics envelopes follow the same shape with resourceLogs.scopeLogs.logRecords /
# resourceMetrics.scopeMetrics.metrics (see the repo's bench scripts for the full generator)
```

Ingest and query:

```sql
-- bduck otel.db
.timer on
CALL raw_ingest_file('traces', 'traces.ndjson', transform := 'otlp-traces');   -- and otlp-logs / otlp-metrics
CHECKPOINT;

-- baseline: identical spans as a JSON blob per row
CREATE TABLE traces_json AS SELECT to_json(traces)::JSON AS j FROM traces;

-- typed columns vs ->> extraction
SELECT "resource.service.name", count(*) FROM traces WHERE "http.status_code" >= 500 GROUP BY 1;
SELECT j->>'resource.service.name', count(*) FROM traces_json
  WHERE CAST(j->>'http.status_code' AS BIGINT) >= 500 GROUP BY 1;
```

Run each query three times (against a `-readonly` database) and report the best.

## VARIANT vs RawDuck (DuckDB v1.5.5)

DuckDB v1.5 shipped `VARIANT` (“JSON on steroids”: schema-less, binary, shredded
in storage). RawDuck still does the OTLP explode + KeyValue flatten into **named
typed columns**. This comparison uses VARIANT **as it exists in the v1.5.5 pin**,
not the v2.0 preview (extraction pushdown / shredded execution from storage).

Published results — Apple M3 Ultra (32 cores, 512 GiB), DuckDB v1.5.5,
`8cf4bf3` (packed drain), 1,000,000 OTLP/JSON trace records. Disk is
**used_blocks × block_size** after cold CHECKPOINT (before DELETE). File size
after warm is in parentheses and is *not* comparable (DELETE does not reclaim).

### Ingest + storage

| path | grain | ingest | records/s | live disk | file after warm |
|---|---|---:|---:|---:|---:|
| RawDuck typed columns | span | 1.01 s | 988k | **38.8 MB** | 110 MB |
| VARIANT of shredded rows | span | encode only | — | **38.8 MB** | — |
| VARIANT exploded OTLP (`{resource,span}`) | span | 11.8 s | 85k | 53.5 MB | 106 MB |
| JSON of shredded rows (`->>`) | span | encode only | — | 142 MB | — |
| JSON exploded OTLP | span | 4.75 s | 210k | 241 MB | 484 MB |

Packed drain matches VARIANT-flat on live size. Ingest is ~2× slower than the
pre-pack M3 run (0.53 s / 1.87M rec/s) because compression happens once at
CHECKPOINT instead of a parallel optimistic flush that left holes.

### Queries (best of 3, ms)

| encoding | errors by service | p99 by route | status dist | vs RawDuck |
|---|---:|---:|---:|---|
| RawDuck typed columns | **1.3** | **3.0** | **2.5** | — |
| JSON flat (`->>`) | 39 | 97 | 35 | 15–32× |
| JSON OTLP positional | 222 | 313 | 201 | 80–170× |
| JSON OTLP key lookup | 361 | 436 | 322 | 130–280× |
| VARIANT flat (same keys) | 430 | 1264 | 425 | 170–420× |
| VARIANT OTLP positional | 1224 | 3483 | 1161 | ~900× |
| VARIANT OTLP key lookup | 1506 | 3725 | 1405 | ~1100× |

### Who is good at what (v1.5.5)

- **RawDuck** wins ingest (~12× vs exploded VARIANT, ~5× vs exploded JSON) and
  every query. Typed columns stay in the 1–3 ms club.
- **Storage is a tie with VARIANT-flat** (38.8 MB). VARIANT was never a better
  codec — same `DICT_FSST` / `BitPacking` after shredding. JSON-flat is 3.7×
  larger; keeping OTLP KeyValue arrays is 1.4× (VARIANT) to 6× (JSON) larger.
- **VARIANT extract is slower than JSON `->>`** on the same shredded object
  (430 ms vs 39 ms). v2.0 shredded execution is not in this pin.
- **OTLP KeyValue arrays at query time lose.** Flatten once at ingest.
- **Envelope VARIANT** stays off the default path (fat export shred can be
  extremely slow on Linux aarch64; not needed for the fair compare).

### Cross-check: NVIDIA Spark GB10 (Linux aarch64, 100k)

Same harness after the CLI stdin fix (`e88c6a7`, `--threads 8`). Confirms the
ranking holds off Apple Silicon — idle hang is gone; DuckDB is CPU-only (CUDA
unused).

| path | ingest | live disk | errors / p99 / status (ms) |
|---|---:|---:|---|
| RawDuck | 0.31 s (325k rec/s) | **4.0 MB** | **1.0 / 1.6 / 3.6** |
| VARIANT-flat | encode | **4.0 MB** | 389 / 1112 / 372 |
| VARIANT OTLP | 1.72 s (58k) | 6.0 MB | 1271–3894 |
| JSON OTLP | 1.17 s (85k) | 24.5 MB | 121–290 |
| JSON-flat | encode | 14.5 MB | 33 / 72 / 30 |

Same story: storage ties VARIANT-flat; typed columns win every query (~30–1000×);
VARIANT extract still slower than JSON `->>` on identical keys.

```sh
git checkout feat/variant-benchmark
GEN=ninja make release
./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
# many-core ARM / oversubscribed hosts (DuckDB is CPU-only; CUDA does not apply):
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 --threads 8
```

Methodology: `scripts/benchmark/README.md`.

## Appendix: GH Archive (historical, wide-schema stress test)

One hour of real [GH Archive](https://www.gharchive.org/) data — 247,199 events / 956 MB NDJSON
exploding to a **914-column** schema. This is the worst case for shredding (extreme, sparse schema
churn), kept as a stress test rather than the representative workload.

Published results (Apple Silicon, 10 cores, DuckDB v1.5.5):

| | JSON column | RawDuck | |
|---|---:|---:|---|
| count by event type | 231 ms | 1 ms | 231× |
| top repos by pushes | 268 ms | 3 ms | 89× |
| distinct repos per actor | 457 ms | 10 ms | 46× |
| sum of push payload sizes | 265 ms | 1 ms | 265× |
| events per minute | 236 ms | 3 ms | 79× |
| ingest | 1.4 s | ~13 s | one-time cost |
| storage | 1.05 GB | 636 MB | 40% smaller |

```sh
curl -sL https://data.gharchive.org/2024-01-15-10.json.gz -o gh.json.gz   # raw_ingest_file reads .gz directly
```

```sql
-- RawDuck: one call shreds the whole hour (914 typed columns, evolution included)
CALL raw_ingest_file('gh_events', 'gh.json.gz');
CHECKPOINT;

-- baseline keeps raw JSON (records='false' alone still infers a STRUCT; the columns clause keeps it raw)
CREATE TABLE gh_raw AS SELECT json
  FROM read_json('gh.json.gz', format='newline_delimited', records='false', columns={json: 'JSON'});
CHECKPOINT;
```

RawDuck queries / baseline (`->>`) queries:

```sql
SELECT type, count(*) AS n FROM gh_events GROUP BY type ORDER BY n DESC;
SELECT "repo.name", count(*) AS n FROM gh_events WHERE type='PushEvent' GROUP BY 1 ORDER BY n DESC LIMIT 10;
SELECT "actor.login", count(DISTINCT "repo.name") AS r FROM gh_events GROUP BY 1 ORDER BY r DESC LIMIT 10;
SELECT sum("payload.size") FROM gh_events WHERE type='PushEvent';
SELECT date_trunc('minute', created_at) AS m, count(*) FROM gh_events GROUP BY m ORDER BY m;

SELECT json->>'$.type' AS type, count(*) AS n FROM gh_raw GROUP BY type ORDER BY n DESC;
SELECT json->>'$.repo.name' AS repo, count(*) AS n FROM gh_raw WHERE json->>'$.type'='PushEvent' GROUP BY 1 ORDER BY n DESC LIMIT 10;
SELECT json->>'$.actor.login' AS a, count(DISTINCT json->>'$.repo.name') AS r FROM gh_raw GROUP BY 1 ORDER BY r DESC LIMIT 10;
SELECT sum(CAST(json->>'$.payload.size' AS BIGINT)) FROM gh_raw WHERE json->>'$.type'='PushEvent';
SELECT date_trunc('minute', CAST(json->>'$.created_at' AS TIMESTAMP)) AS m, count(*) FROM gh_raw GROUP BY m ORDER BY m;
```

### Warm-table ingest

The cold GH numbers include schema discovery (CREATE + evolution sync points). Re-ingesting the same
hour into the already-evolved table runs fully parallel: **~4.9 s** — the steady-state rate once a
table's shape has stabilized (the realistic OTEL case, where the schema is stable after warmup).

### INSERT-syntax streaming (fastest path)

`INSERT INTO raw.ingest.t SELECT ...` streams any SQL source through a parallel zero-copy sink:

```sql
ATTACH 'rawduck:store.db' AS raw;
INSERT INTO raw.ingest.narrow SELECT '{"a":' || range || '}' FROM range(5000000);
-- 5M narrow rows in ~0.8 s  (~6.1M rows/s)
```

### Adaptive layout and projections

```sql
CALL raw_stats();
CALL raw_optimize('gh_events');     -- physically reorders by hottest columns
CALL raw_project('gh_events');      -- materializes the hottest aggregation
SET rawduck_use_projections = true; -- transparent rewrite of eligible count(*) queries
```

## Pitfalls

- Don't split NDJSON with Python's `splitlines()` — it splits on `\u2028`/`\u2029` which appear raw
  inside real-world strings and corrupts records. Split on `\n` only.
- The shell reports query times with `.timer on`; dot-commands don't work via `duckdb -c`, use
  `-f script.sql`.
- A shallow duckdb submodule clone without tags makes the shell report `v0.0.1`; fetch the release
  tag (`git -C duckdb fetch --depth 1 origin tag v1.5.5`) or extension installs 404.
- For large imports where each line is a fat container (OTLP envelopes, CloudWatch log groups), the
  loader auto-parallelizes via byte-aware batching; you only need `batch_size` to *raise* the line
  cap for very small flat records.
