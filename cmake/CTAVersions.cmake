# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(CTA_VERSION "" CACHE STRING "CTA version-release")

# For e.g. RPMs we need to distinguish between the version and the release.
# For that reason, we require there to be at least one hyphen in the version string.
if(NOT CTA_VERSION MATCHES "^[A-Za-z0-9._+~^]+-[A-Za-z0-9._+~^]+$")
  message(FATAL_ERROR
    "CTA_VERSION must contain two nonempty parts separated by exactly one hyphen, "
    "using only letters, numbers, '.', '_', '+', '~', or '^'; for example 6~dev1-1.")
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
    "{ \"type\": \"${CTA_PACKAGE_FORMAT}\", \"name\": \"cta\", \"version\": \"${CTA_VERSION}\" }")
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
