PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=rawduck
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Build without the OTLP/gRPC server even when gRPC is available
# (OTLP/HTTP keeps working): make release RAWDUCK_DISABLE_GRPC=1
ifeq ($(RAWDUCK_DISABLE_GRPC),1)
	EXT_FLAGS:=$(EXT_FLAGS) -DRAWDUCK_DISABLE_GRPC=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile