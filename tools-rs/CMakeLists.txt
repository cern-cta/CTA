# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# ---------------------------------------------------------------------------
# Rust command-line tools (cargo workspace in this directory)
# ---------------------------------------------------------------------------
# The Rust tools are built by cargo, which owns its own dependency resolution
# and incremental build cache. CMake only drives cargo and installs the
# resulting binaries, so that `cmake --build . --target install` produces the
# same file layout as for the C++ tools and the RPM %files sections can pick
# them up.

option(CTA_BUILD_RUST_TOOLS "Build the Rust command-line tools in tools-rs" ON)

if(NOT CTA_BUILD_RUST_TOOLS)
  message(STATUS "Rust tools: disabled (CTA_BUILD_RUST_TOOLS=OFF)")
  return()
endif()

find_program(CARGO_EXECUTABLE cargo)

if(NOT CARGO_EXECUTABLE)
  message(FATAL_ERROR
    "cargo was not found but CTA_BUILD_RUST_TOOLS is ON.\n"
    "Install a Rust toolchain (see ci/docker/cta/el9/build.Dockerfile) or "
    "re-run cmake with -DCTA_BUILD_RUST_TOOLS:BOOL=OFF.")
endif()

# cargo profile: only Debug maps onto the unoptimised profile, every other
# CMake build type is built with optimisations (matching RelWithDebInfo, which
# keeps debug info via the workspace profile settings).
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  set(CARGO_PROFILE_DIR "debug")
  set(CARGO_PROFILE_FLAGS "")
else()
  set(CARGO_PROFILE_DIR "release")
  set(CARGO_PROFILE_FLAGS "--release")
endif()

# Keep cargo's output inside the CMake build tree: the source directory must
# stay clean, and a build tree per platform/build type must not share
# artefacts.
set(CARGO_TARGET_DIR "${CMAKE_CURRENT_BINARY_DIR}/cargo")

# Binaries produced by the workspace; add new tools here.
set(CTA_RUST_TOOLS cta-restore-files)

set(CTA_RUST_TOOL_BINARIES "")

foreach(tool IN LISTS CTA_RUST_TOOLS)
  list(APPEND CTA_RUST_TOOL_BINARIES
    "${CARGO_TARGET_DIR}/${CARGO_PROFILE_DIR}/${tool}")
endforeach()

# Rebuild whenever a source file or manifest changes. Globbing at configure
# time is good enough here because cargo itself decides what to recompile; the
# dependency list only has to make the custom command run again.
file(GLOB_RECURSE CTA_RUST_SOURCES
  CONFIGURE_DEPENDS
  "${CMAKE_CURRENT_SOURCE_DIR}/*/src/*.rs"
  "${CMAKE_CURRENT_SOURCE_DIR}/*/build.rs"
  "${CMAKE_CURRENT_SOURCE_DIR}/*/Cargo.toml"
  "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
  "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.lock")

add_custom_command(
  OUTPUT ${CTA_RUST_TOOL_BINARIES}
  COMMAND ${CMAKE_COMMAND} -E env
    "CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
    ${CARGO_EXECUTABLE} build ${CARGO_PROFILE_FLAGS}
      --manifest-path "${CMAKE_CURRENT_SOURCE_DIR}/Cargo.toml"
      --locked
      --workspace
      --bins
  DEPENDS ${CTA_RUST_SOURCES}
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Building Rust tools with cargo (${CARGO_PROFILE_DIR})"
  VERBATIM)

add_custom_target(cta-rust-tools ALL DEPENDS ${CTA_RUST_TOOL_BINARIES})

install(PROGRAMS ${CTA_RUST_TOOL_BINARIES}
  DESTINATION ${CMAKE_INSTALL_BINDIR})

message(STATUS "Rust tools: ${CTA_RUST_TOOLS} (cargo ${CARGO_PROFILE_DIR})")
