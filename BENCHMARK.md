# RawDuck Benchmark

Primary workload: **OTEL telemetry** (OTLP/JSON logs, metrics, traces). GH Archive is a
wide-schema stress test in the appendix.

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

3M telemetry records shredded into typed columns in **2.6 s (~1.2M records/s)**.

### Query speed (1,000,000 spans)

Shredded typed columns vs one JSON object per span (`->>`):

| query | JSON `->>` | RawDuck | speedup |
|---|---:|---:|---:|
| error count by service (`status>=500`) | 71 ms | 2 ms | 36× |
| p99 latency by route | 136 ms | 5 ms | 27× |
| status-code distribution | 63 ms | 1 ms | 63× |
| storage | 232 MB | 38 MB | 6× smaller |

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

Same OTLP/JSON traces as the OTEL ingest suite. Paths:

| path | definition |
|---|---|
| RawDuck | `raw_ingest_file(..., transform := 'otlp-traces')` → typed columns |
| VARIANT OTLP | SQL unnest → one `VARIANT` `{resource, span}` per span (KeyValue arrays kept) |
| JSON OTLP | same exploded shape as `JSON` |
| VARIANT-flat | `to_json(traces)::VARIANT` of already-shredded RawDuck rows (encode/query only) |
| JSON-flat | same shredded rows as `JSON`, queried with `->>` |

Disk is `used_blocks × block_size` after cold `CHECKPOINT` (before `DELETE`). Parenthetical
file size is after warm re-ingest and includes free-list holes — not comparable across paths.
VARIANT requires `STORAGE_VERSION 'v1.5.0'`. Measured with DuckDB VARIANT as of **v1.5.5**
(not v2.0 shredded execution / extraction pushdown).

### Ingest + storage (1,000,000 spans)

| path | M3 Ultra (32 cores, 512 GiB) | Spark GB10 aarch64 (20 cores, 122 GiB, `--threads 8`) |
|---|---|---|
| RawDuck | 1.01 s · 988k rec/s · 38.8 MB (110 MB file) | 1.18 s · 850k rec/s · 35.5 MB (91 MB file) |
| VARIANT-flat | encode · 38.8 MB | encode · 39.5 MB |
| VARIANT OTLP | 11.8 s · 85k · 53.5 MB (106 MB file) | 7.96 s · 126k · 54.5 MB (114 MB file) |
| JSON-flat | encode · 142 MB | encode · 142 MB |
| JSON OTLP | 4.75 s · 210k · 241 MB (484 MB file) | 5.71 s · 175k · 242 MB (484 MB file) |

### Queries (best of 3, ms)

| encoding | M3 Ultra | Spark GB10 |
|---|---|---|
| | errors / p99 / status | errors / p99 / status |
| RawDuck | 1.3 / 3.0 / 2.5 | 1.3 / 4.9 / 5.1 |
| JSON-flat | 39 / 97 / 35 | 65 / 136 / 63 |
| JSON OTLP positional | 222 / 313 / 201 | 253 / 415 / 215 |
| JSON OTLP key lookup | 361 / 436 / 322 | 397 / 580 / 366 |
| VARIANT-flat | 430 / 1264 / 425 | 700 / 1988 / 696 |
| VARIANT OTLP positional | 1224 / 3483 / 1161 | 2013 / 5947 / 1852 |
| VARIANT OTLP key lookup | 1506 / 3725 / 1405 | 2107 / 5996 / 2049 |

### Reproduce

```sh
GEN=ninja make release
./scripts/benchmark/run_variant.sh --quick
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3
./scripts/benchmark/run_variant.sh --records 1000000 --runs 3 --threads 8
```

Harness details: `scripts/benchmark/README.md`.

## Appendix: GH Archive (historical, wide-schema stress test)

One hour of [GH Archive](https://www.gharchive.org/) data — 247,199 events / 956 MB NDJSON /
**914 columns**. Apple Silicon, 10 cores, DuckDB v1.5.5:

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

Re-ingest into the evolved table: **~4.9 s**.

### INSERT-syntax streaming

```sql
ATTACH 'rawduck:store.db' AS raw;
INSERT INTO raw.ingest.narrow SELECT '{"a":' || range || '}' FROM range(5000000);
-- 5M narrow rows in ~0.8 s (~6.1M rows/s)
```

### Adaptive layout and projections

```sql
CALL raw_stats();
CALL raw_optimize('gh_events');
CALL raw_project('gh_events');
SET rawduck_use_projections = true;
```

## Pitfalls

- Split NDJSON on `\n` only (not `splitlines()` — `\u2028`/`\u2029` appear in strings).
- Use `.timer on` via `duckdb -f script.sql` (not `duckdb -c`).
- Shallow duckdb clones without tags report `v0.0.1`; fetch tag `v1.5.5`.
