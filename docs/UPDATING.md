# Extension updating

Keep submodule SHAs, workflow pins, and Pages `DUCKDB_VERSION` in lockstep on
the branch you are releasing.

## `v2.0.0` — DuckDB Cyanoptera / v2.0.0 product line

- `./duckdb` → `origin/v2.0-cyanoptera` until tag `v2.0.0` exists, then that tag
- `./extension-ci-tools` → `main` until a cyanoptera / v2.0.0 branch exists
- `MainDistributionPipeline.yml`, `Release.yml`, `QuackIntegration.yml`:
  `duckdb_version: v2.0-cyanoptera`, `ci_tools_version: main`, reusable workflows `@main`
- Quack: `duckdb-httpfs` / `duckdb-quack` `GIT_TAG` must be cyanoptera-compatible
- `Release.yml` `DUCKDB_VERSION` stays the freeze-line ref (artifact names / CLI URL).
  `PAGES_VERSION` is `v2.0.0` so unsigned INSTALL also lives under the product folder.
  The day GitHub publishes `v2.0.0`, point both at that tag.
- Community-extensions: PR `extensions/rawduck/description.yml` from
  `community-extensions/rawduck/description.yml` in this repo. `repo.ref` is this
  branch (v2-capable). Do not set `ref_next`.

Build with `GEN=ninja make release`. Test with
`./build/release/test/unittest --test-dir . "test/sql/*"`. Format with `make format-fix`.

# API changes

DuckDB extensions built with this template use DuckDB's internal C++ API, which
is not stable. A pin bump may fail to compile.

Useful references:

- DuckDB [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB [core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- Git history of the C++ header whose API changed
