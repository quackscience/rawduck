set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
# release-only: host-tool builds (protoc, grpc plugins) do not need debug
set(VCPKG_BUILD_TYPE release)
