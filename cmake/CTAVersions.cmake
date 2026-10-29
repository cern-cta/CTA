# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(CTA_VERSION "" CACHE STRING "CTA version-release")

# For e.g. RPMs we need to distinguish between the version and the release.
# For that reason, we require there to be at least one hyphen in the version string.
if(NOT CTA_VERSION MATCHES "^[A-Za-z0-9._+~^]+-[A-Za-z0-9._+~^]+$")
  message(FATAL_ERROR
    "CTA_VERSION must contain two nonempty parts separated by exactly one hyphen, "
    "using only letters, numbers, '.', '_', '+', '~', or '^'; for example 6-dev.")
endif()

# Preserve source identity when building an archive without its Git checkout.
set(CTA_GIT_COMMIT "" CACHE STRING "Source Git commit override")
if(NOT CTA_GIT_COMMIT)
  if(EXISTS "${PROJECT_SOURCE_DIR}/cmake/cta-git-commit.txt")
    file(READ "${PROJECT_SOURCE_DIR}/cmake/cta-git-commit.txt" CTA_GIT_COMMIT)
    string(STRIP "${CTA_GIT_COMMIT}" CTA_GIT_COMMIT)
  else()
    find_package(Git QUIET)
    if(Git_FOUND)
      execute_process(COMMAND "${GIT_EXECUTABLE}" rev-parse --show-toplevel
        WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
        OUTPUT_VARIABLE _cta_git_root OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
      # An unpacked source tree may sit inside an unrelated checkout.
      file(REAL_PATH "${PROJECT_SOURCE_DIR}" _cta_source_root)
      if(_cta_git_root STREQUAL _cta_source_root)
        execute_process(COMMAND "${GIT_EXECUTABLE}" rev-parse --verify HEAD
          WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
          OUTPUT_VARIABLE CTA_GIT_COMMIT OUTPUT_STRIP_TRAILING_WHITESPACE ERROR_QUIET)
      endif()
    endif()
  endif()
endif()

if(CTA_GIT_COMMIT)
  string(LENGTH "${CTA_GIT_COMMIT}" _cta_commit_length)
  if(NOT CTA_GIT_COMMIT MATCHES "^[0-9a-fA-F]+$" OR
      NOT (_cta_commit_length EQUAL 40 OR _cta_commit_length EQUAL 64))
    message(FATAL_ERROR "CTA_GIT_COMMIT must be a full Git commit hash")
  endif()
endif()

set(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION "v0.0" CACHE STRING
  "XRootD SSI protobuf interface version")

# Catalogue Schema Version
include(catalogue/cta-catalogue-schema/CTACatalogueSchemaVersion.cmake)

# Scheduler Schema Version
set(CTA_SCHEDULER_SCHEMA_VERSION_MAJOR 1)
set(CTA_SCHEDULER_SCHEMA_VERSION_MINOR 0)

# Shared object internal version (used in SONAME)
set(CTA_SOVERSION 0)

# Shared object external version (used in filename)
set(CTA_SOMAJOR ${CTA_SOVERSION})
set(CTA_SOMINOR 1)
set(CTA_SOPATCH 0)

if(CTA_BUILD_MODE STREQUAL "compile")
  # Embed the package version in every C/C++ executable and shared library for systemd-coredump.
  # Pass JSON as one linker argument: -Wl would split its commas.
  # Spaces also make CMake quote the argument, preventing shell brace expansion.
  set(CTA_ELF_PACKAGE_METADATA
    "{ \"type\": \"${CTA_PACKAGE_FORMAT}\", \"name\": \"cta\", \"version\": \"${CTA_VERSION}\"")
  if(CTA_GIT_COMMIT)
    string(APPEND CTA_ELF_PACKAGE_METADATA ", \"gitCommit\": \"${CTA_GIT_COMMIT}\"")
  endif()
  string(APPEND CTA_ELF_PACKAGE_METADATA " }")
  add_link_options("SHELL:-Xlinker '--package-metadata=${CTA_ELF_PACKAGE_METADATA}'")

  configure_file(
    ${PROJECT_SOURCE_DIR}/version.cpp.in
    ${CMAKE_CURRENT_BINARY_DIR}/version.cpp
    @ONLY
  )

  # Share generated version information between compiled targets.
  add_library(ctaversioninfo STATIC
    ${PROJECT_SOURCE_DIR}/version.hpp
    ${CMAKE_CURRENT_BINARY_DIR}/version.cpp
  )

  target_include_directories(ctaversioninfo PUBLIC ${PROJECT_SOURCE_DIR})
endif()

# Shared library versioning
set(CTA_LIBVERSION ${CTA_SOMAJOR}.${CTA_SOMINOR}.${CTA_SOPATCH})
