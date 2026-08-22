# Extension updating 
When cloning this template, the target version of DuckDB should be the latest stable release of DuckDB. However, there 
will inevitably come a time when a new DuckDB is released and the extension repository needs updating. This process goes
as follows:

- Bump submodules
  - `./duckdb` should be set to latest tagged release (currently `v1.5.5`)
  - `./extension-ci-tools` should be set to the matching branch tip. Prefer the
    exact patch branch (`v1.5.5`) when it exists; otherwise the line branch
    (`v1.5-variegata`). Keep the submodule SHA in sync with that tip.
- Bump versions in `./github/workflows`
  - `duckdb_version` / `ci_tools_version` in `MainDistributionPipeline.yml`,
    `Release.yml`, and `QuackIntegration.yml` should be the latest tagged release
  - the reusable workflow refs
    (`duckdb/extension-ci-tools/.github/workflows/_extension_*.yml@…`) should use
    the same tag/branch (e.g. `@v1.5.5`), not a moving alias unless intentional
  - `DUCKDB_VERSION` in `Release.yml` must match (GitHub Pages path + CLI download)

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed