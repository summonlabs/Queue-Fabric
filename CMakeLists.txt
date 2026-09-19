cmake_minimum_required(VERSION 3.25)

project(queue_fabric
  VERSION 1.0.0
  DESCRIPTION "Vendor-neutral C++20 runtime for queue lifecycle, scheduling-class authority, occupancy evidence and mutation governance"
  HOMEPAGE_URL "https://github.com/summonlabs/Queue-Fabric"
  LANGUAGES CXX)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CTest)

option(QF_BUILD_TESTS "Build the Queue Fabric test suite" ON)
option(QF_BUILD_BENCHMARKS "Build the Queue Fabric benchmark harness" ON)
option(QF_BUILD_TOOLS "Build the Queue Fabric coordinator and worker tools" ON)
option(QF_WERROR "Treat first-party warnings as errors" ON)
option(QF_SANITIZE_ADDRESS "Build with AddressSanitizer where supported" OFF)
option(QF_ANALYZE "Run the platform static analyzer over first-party sources" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

find_package(Threads REQUIRED)

# Strict warnings for every first-party target. Applied directly rather than
# through an interface library so the exported package carries no build-only
# usage requirements.
function(qf_apply_strict target)
  if(MSVC)
    target_compile_options(${target} PRIVATE /W4 /permissive- /Zc:__cplusplus /utf-8 /EHsc)
    if(QF_WERROR)
      target_compile_options(${target} PRIVATE /WX)
    endif()
    if(QF_ANALYZE)
      # Static analysis pass. Findings raised inside SDK and CRT headers are
      # reported by the tool but are not actionable from this repository, so the
      # analysis configuration is normally run with QF_WERROR=OFF and the output
      # triaged against first-party files.
      target_compile_options(${target} PRIVATE /analyze)
    endif()
  else()
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Wshadow)
    if(QF_WERROR)
      target_compile_options(${target} PRIVATE -Werror)
    endif()
  endif()
  if(QF_SANITIZE_ADDRESS)
    if(MSVC)
      # The MSVC ASan runtime is linked through the compiler's default library
      # directives; link.exe does not accept the switch itself.
      target_compile_options(${target} PRIVATE /fsanitize=address)
    else()
      target_compile_options(${target} PRIVATE -fsanitize=address -fno-omit-frame-pointer)
      target_link_options(${target} PRIVATE -fsanitize=address)
    endif()
  endif()
endfunction()

set(QF_SOURCES
  src/status.cpp
  src/time.cpp
  src/provenance.cpp
  src/lifecycle.cpp
  src/threshold.cpp
  src/occupancy.cpp
  src/backend.cpp
  src/drain.cpp
  src/authority.cpp
  src/mutation.cpp
  src/queue.cpp
  src/policy.cpp
  src/explain.cpp
  src/journal.cpp
  src/file_journal.cpp
  src/serialize.cpp
  src/fabric.cpp
  src/fabric_apply.cpp
  src/fabric_kinds.cpp
  src/fabric_plans.cpp
  src/fabric_recovery.cpp
  src/fabric_explain.cpp
  src/fabric_invariants.cpp
  src/fabric_governance.cpp
  src/frame.cpp
  src/transport.cpp
  src/protocol.cpp
  src/coordinator.cpp
  src/worker.cpp
  src/process.cpp
)

add_library(queue_fabric STATIC ${QF_SOURCES})
add_library(qf::queue_fabric ALIAS queue_fabric)

target_include_directories(queue_fabric
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
  PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src)

target_compile_features(queue_fabric PUBLIC cxx_std_20)
target_link_libraries(queue_fabric PUBLIC Threads::Threads)
qf_apply_strict(queue_fabric)

if(WIN32)
  # Winsock is required by the framed TCP transport and is part of the exported
  # link interface of the static library.
  target_link_libraries(queue_fabric PUBLIC ws2_32)
endif()

set_target_properties(queue_fabric PROPERTIES
  OUTPUT_NAME queue_fabric
  VERSION ${PROJECT_VERSION}
  SOVERSION ${PROJECT_VERSION_MAJOR})

if(QF_BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if(QF_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(QF_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()

install(TARGETS queue_fabric
  EXPORT qfTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})

install(EXPORT qfTargets
  FILE qfTargets.cmake
  NAMESPACE qf::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qf)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/qfConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/qfConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qf)

write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/qfConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)

install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/qfConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/qfConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/qf)
