cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME rawduck)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include_directories(src/include)
# DuckDB's vendored yyjson (namespaced as duckdb_yyjson)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/yyjson/include)
# DuckDB's vendored cpp-httplib (namespace duckdb_httplib) for the API server
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/httplib)

# zlib enables cpp-httplib's native gzip handling for the HTTP API
# (request decompression and response compression)
if(NOT EMSCRIPTEN)
  find_package(ZLIB QUIET)
  if(ZLIB_FOUND)
    set_source_files_properties(src/raw_api.cpp PROPERTIES COMPILE_DEFINITIONS CPPHTTPLIB_ZLIB_SUPPORT=1)
  endif()
endif()

# OTLP/protobuf request bodies on the HTTP API: enabled by default whenever
# protobuf is available (the vcpkg manifest's default "protobuf" feature, or a
# system package). Never built for wasm; skipped gracefully when missing.
option(RAWDUCK_ENABLE_OTLP_PROTOBUF "Decode OTLP/protobuf HTTP request bodies (requires protobuf)" ON)
# Optional OTLP/gRPC ingestion server: opt-in, requires the full gRPC stack.
option(RAWDUCK_ENABLE_GRPC "Build the OTLP/gRPC ingestion server (requires gRPC + protobuf)" OFF)

set(RAWDUCK_PROTOBUF_ENABLED FALSE)
set(RAWDUCK_GRPC_ENABLED FALSE)
if((RAWDUCK_ENABLE_OTLP_PROTOBUF OR RAWDUCK_ENABLE_GRPC) AND NOT EMSCRIPTEN AND NOT MINGW)
  find_package(Protobuf CONFIG QUIET)
  if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf QUIET)
  endif()
  if(TARGET protobuf::libprotobuf AND TARGET protobuf::protoc)
    set(RAWDUCK_PROTOBUF_ENABLED TRUE)
  endif()
endif()
if(RAWDUCK_ENABLE_GRPC AND RAWDUCK_PROTOBUF_ENABLED)
  find_package(gRPC CONFIG QUIET)
  if(gRPC_FOUND AND TARGET gRPC::grpc_cpp_plugin)
    set(RAWDUCK_GRPC_ENABLED TRUE)
  endif()
endif()

set(RAWDUCK_PROTO_SOURCES)
if(RAWDUCK_PROTOBUF_ENABLED)
  message(STATUS "RawDuck: building with OTLP/protobuf support")
  set(RAWDUCK_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
  set(RAWDUCK_PROTO_GEN ${CMAKE_CURRENT_BINARY_DIR}/rawduck_proto_gen)
  file(MAKE_DIRECTORY ${RAWDUCK_PROTO_GEN})
  set(RAWDUCK_SERVICE_PROTOS
      opentelemetry/proto/collector/trace/v1/trace_service
      opentelemetry/proto/collector/logs/v1/logs_service
      opentelemetry/proto/collector/metrics/v1/metrics_service)
  set(RAWDUCK_ALL_PROTOS
      opentelemetry/proto/common/v1/common
      opentelemetry/proto/resource/v1/resource
      opentelemetry/proto/trace/v1/trace
      opentelemetry/proto/logs/v1/logs
      opentelemetry/proto/metrics/v1/metrics
      ${RAWDUCK_SERVICE_PROTOS})
  foreach(proto ${RAWDUCK_ALL_PROTOS})
    add_custom_command(
      OUTPUT ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc ${RAWDUCK_PROTO_GEN}/${proto}.pb.h
      COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${RAWDUCK_PROTO_DIR}
              --cpp_out=${RAWDUCK_PROTO_GEN} ${RAWDUCK_PROTO_DIR}/${proto}.proto
      DEPENDS ${RAWDUCK_PROTO_DIR}/${proto}.proto)
    list(APPEND RAWDUCK_PROTO_SOURCES ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc)
  endforeach()
  include_directories(${RAWDUCK_PROTO_GEN})
  set_property(SOURCE src/raw_otlp_pb.cpp APPEND PROPERTY COMPILE_DEFINITIONS RAWDUCK_WITH_OTLP_PROTOBUF=1)
else()
  message(STATUS "RawDuck: building without OTLP/protobuf support")
endif()

if(RAWDUCK_GRPC_ENABLED)
  message(STATUS "RawDuck: building with OTLP/gRPC support")
  foreach(proto ${RAWDUCK_SERVICE_PROTOS})
    add_custom_command(
      OUTPUT ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.cc ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.h
      COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${RAWDUCK_PROTO_DIR}
              --grpc_out=${RAWDUCK_PROTO_GEN}
              --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
              ${RAWDUCK_PROTO_DIR}/${proto}.proto
      DEPENDS ${RAWDUCK_PROTO_DIR}/${proto}.proto)
    list(APPEND RAWDUCK_PROTO_SOURCES ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.cc)
  endforeach()
  set_source_files_properties(src/raw_grpc.cpp PROPERTIES COMPILE_DEFINITIONS RAWDUCK_WITH_GRPC=1)
else()
  message(STATUS "RawDuck: building without OTLP/gRPC support")
endif()

set(EXTENSION_SOURCES
    src/rawduck_extension.cpp
    src/raw_json.cpp
    src/raw_records.cpp
    src/raw_ingest.cpp
    src/raw_attach.cpp
    src/raw_quack_attach.cpp
    src/raw_async.cpp
    src/raw_insert.cpp
    src/raw_api.cpp
    src/raw_otlp_pb.cpp
    src/raw_grpc.cpp
    ${RAWDUCK_PROTO_SOURCES}
    src/raw_transforms.cpp
    src/raw_optimize.cpp
    src/raw_scalars.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

if(ZLIB_FOUND)
  target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
endif()

if(RAWDUCK_PROTOBUF_ENABLED)
  target_link_libraries(${EXTENSION_NAME} protobuf::libprotobuf)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} protobuf::libprotobuf)
endif()

if(RAWDUCK_GRPC_ENABLED)
  target_link_libraries(${EXTENSION_NAME} gRPC::grpc++)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} gRPC::grpc++)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
