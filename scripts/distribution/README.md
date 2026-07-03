# Distribution test scripts

Primary path: **multi-process OTEL writers → shared DuckLake → read-only reader**.

See [docs/DISTRIBUTION.md](../../docs/DISTRIBUTION.md).

## Quick start

```sh
GEN=ninja make release
./scripts/distribution/run_ducklake_otel_cluster.sh
./scripts/distribution/run_ducklake_writers.sh
```

## Writer hub recipe (one instance)

```sql
LOAD rawduck;
INSTALL ducklake; LOAD ducklake;
ATTACH 'ducklake:/path/shared.ducklake' AS lake (DATA_PATH '/path/parquet');
SELECT * FROM raw_serve(
    host := '0.0.0.0', port := 9999, token := 'secret',
    ingest_prefix := 'lake.main'
);
-- OTEL → POST /otlp/v1/traces  (table defaults to otel_traces → lake.main.otel_traces)
```

## Reader recipe

```sql
LOAD ducklake;
ATTACH 'ducklake:/path/shared.ducklake' AS lake (DATA_PATH '/path/parquet', READ_ONLY);
SELECT * FROM lake.main.otel_traces;
```

## Sqlite metadata note

Only one process can hold `ATTACH` on a sqlite-backed DuckLake file at a time. For N always-on
writer **processes**, either fan collectors into one hub or move metadata to postgres DuckLake.
The cluster script models N **sequential** writer processes (attach → serve → exit).

## Tests

| Layer | File |
|---|---|
| sqllogictest | `test/sql/raw_distribution_ducklake_otel.test` |
| multi-process | `scripts/distribution/run_ducklake_otel_cluster.sh` |
| HTTP fan-in (single hub) | `test/http/distribution_otlp_fanin.sh` |
