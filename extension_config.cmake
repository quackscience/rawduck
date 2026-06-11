# This file is included by DuckDB's build system. It specifies which extension to load

# Build the whole tree with one C++ standard. gRPC/abseil require C++17 and
# CMake propagates that requirement through the static extension into DuckDB's
# own tools; mixing standards turns 'static constexpr' members into duplicate
# strong/inline definitions that GNU ld rejects (multiple definition of
# BufferedFileWriter::DEFAULT_OPEN_FLAGS, CatalogEntry::Name, ...).
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Extension from this repo
duckdb_extension_load(rawduck
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
)

# JSON extension: provides the JSON logical type and to_json()/json_* functions
# that RawDuck relies on for its structural-conflict columns
duckdb_extension_load(json)
