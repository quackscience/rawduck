PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rawduck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB main (v2.0-dev) requires C++17+. A leftover build/ from the v1.5.5 pin
# caches CMAKE_CXX_STANDARD=11 and cmake will keep that unless -D overrides it.
EXT_FLAGS:=$(EXT_FLAGS) -DCMAKE_CXX_STANDARD=17

# The OTLP/gRPC server is opt-in (it pulls the gRPC/protobuf stack and
# significantly lengthens builds): make release RAWDUCK_ENABLE_GRPC=1
ifeq ($(RAWDUCK_ENABLE_GRPC),1)
	EXT_FLAGS:=$(EXT_FLAGS) -DRAWDUCK_ENABLE_GRPC=1 -DVCPKG_MANIFEST_FEATURES=grpc
endif

# OTLP/protobuf HTTP bodies are decoded by default (vcpkg manifest default
# feature "protobuf"); disable to skip the protobuf dependency entirely:
# make release RAWDUCK_DISABLE_OTLP_PROTOBUF=1
ifeq ($(RAWDUCK_DISABLE_OTLP_PROTOBUF),1)
	EXT_FLAGS:=$(EXT_FLAGS) -DRAWDUCK_ENABLE_OTLP_PROTOBUF=0 -DVCPKG_MANIFEST_NO_DEFAULT_FEATURES=ON
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile