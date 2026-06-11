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

# Optional OTLP/gRPC ingestion server: enabled when gRPC and protobuf are
# available (vcpkg in CI, system packages locally). Never built for wasm.
option(RAWDUCK_DISABLE_GRPC "Build without the OTLP/gRPC server (OTLP/HTTP only)" OFF)
set(RAWDUCK_GRPC_ENABLED FALSE)
if(NOT EMSCRIPTEN AND NOT MINGW AND NOT RAWDUCK_DISABLE_GRPC)
  find_package(gRPC CONFIG QUIET)
  find_package(Protobuf CONFIG QUIET)
  if(NOT TARGET protobuf::libprotobuf)
    find_package(Protobuf QUIET)
  endif()
  if(gRPC_FOUND AND TARGET protobuf::libprotobuf AND TARGET gRPC::grpc_cpp_plugin)
    set(RAWDUCK_GRPC_ENABLED TRUE)
  endif()
endif()

set(RAWDUCK_GRPC_SOURCES)
if(RAWDUCK_GRPC_ENABLED)
  message(STATUS "RawDuck: building with OTLP/gRPC support")
  set(RAWDUCK_PROTO_DIR ${CMAKE_CURRENT_SOURCE_DIR}/proto)
  set(RAWDUCK_PROTO_GEN ${CMAKE_CURRENT_BINARY_DIR}/rawduck_proto_gen)
  file(MAKE_DIRECTORY ${RAWDUCK_PROTO_GEN})
  set(RAWDUCK_MESSAGE_PROTOS
      opentelemetry/proto/common/v1/common
      opentelemetry/proto/resource/v1/resource
      opentelemetry/proto/trace/v1/trace
      opentelemetry/proto/logs/v1/logs
      opentelemetry/proto/metrics/v1/metrics)
  set(RAWDUCK_SERVICE_PROTOS
      opentelemetry/proto/collector/trace/v1/trace_service
      opentelemetry/proto/collector/logs/v1/logs_service
      opentelemetry/proto/collector/metrics/v1/metrics_service)
  foreach(proto ${RAWDUCK_MESSAGE_PROTOS})
    add_custom_command(
      OUTPUT ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc ${RAWDUCK_PROTO_GEN}/${proto}.pb.h
      COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${RAWDUCK_PROTO_DIR}
              --cpp_out=${RAWDUCK_PROTO_GEN} ${RAWDUCK_PROTO_DIR}/${proto}.proto
      DEPENDS ${RAWDUCK_PROTO_DIR}/${proto}.proto)
    list(APPEND RAWDUCK_GRPC_SOURCES ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc)
  endforeach()
  foreach(proto ${RAWDUCK_SERVICE_PROTOS})
    add_custom_command(
      OUTPUT ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc ${RAWDUCK_PROTO_GEN}/${proto}.pb.h
             ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.cc ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.h
      COMMAND $<TARGET_FILE:protobuf::protoc> --proto_path=${RAWDUCK_PROTO_DIR}
              --cpp_out=${RAWDUCK_PROTO_GEN} --grpc_out=${RAWDUCK_PROTO_GEN}
              --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
              ${RAWDUCK_PROTO_DIR}/${proto}.proto
      DEPENDS ${RAWDUCK_PROTO_DIR}/${proto}.proto)
    list(APPEND RAWDUCK_GRPC_SOURCES ${RAWDUCK_PROTO_GEN}/${proto}.pb.cc
         ${RAWDUCK_PROTO_GEN}/${proto}.grpc.pb.cc)
  endforeach()
  include_directories(${RAWDUCK_PROTO_GEN})
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
    src/raw_api.cpp
    src/raw_grpc.cpp
    ${RAWDUCK_GRPC_SOURCES}
    src/raw_transforms.cpp
    src/raw_optimize.cpp
    src/raw_scalars.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} duckdb_yyjson)
target_link_libraries(${LOADABLE_EXTENSION_NAME} duckdb_yyjson)

if(RAWDUCK_GRPC_ENABLED)
  target_link_libraries(${EXTENSION_NAME} gRPC::grpc++ protobuf::libprotobuf)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} gRPC::grpc++ protobuf::libprotobuf)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
