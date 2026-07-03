# Distribution test scripts

Multi-process simulations for RawDuck distributed setups. See [docs/DISTRIBUTION.md](../../docs/DISTRIBUTION.md).

## Prerequisites

```sh
GEN=ninja make release
export DUCKDB=./build/release/duckdb
export EXT=./build/release/extension/rawduck/rawduck.duckdb_extension
```

## Local Quack cluster (hub + readers)

Terminal 1 — ingest hub:

```sh
$DUCKDB -unsigned -c "
  LOAD '$EXT'; LOAD quack;
  SELECT * FROM quack_serve('quack:127.0.0.1:19920', token := 'rt_secret');
  CALL raw_serve('127.0.0.1:9999', token := 'rt_secret');
"
```

Terminal 2 — parallel HTTP writers:

```sh
./test/http/distribution_concurrent_writes.sh
./test/http/distribution_otlp_fanin.sh
```

Terminal 3 — Quack reader loop:

```sh
while true; do
  $DUCKDB -unsigned -c "
    LOAD '$EXT'; LOAD quack;
    ATTACH 'rawduck:quack:127.0.0.1:19920' AS raw (TOKEN 'rt_secret');
    SELECT count(*) FROM raw.dist_events;
    DETACH raw;
  "
  sleep 0.5
done
```

## DuckLake multi-process writers

```sh
./scripts/distribution/run_ducklake_writers.sh
```

Two DuckDB processes ingest into the same on-disk DuckLake attach. Inspect exit codes and final
row counts printed by the script.

## CI note

Layer 1 sqllogictests (`raw_distribution_*.test`) run in the normal unittest suite. HTTP and
multi-process scripts are manual or future `DistributionIntegration` workflow targets.
