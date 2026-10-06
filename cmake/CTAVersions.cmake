# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Callers supply one version-release string; CI applies its own naming conventions.
if(DEFINED CTA_RELEASE OR DEFINED VCS_VERSION)
  message(FATAL_ERROR
    "CTA_RELEASE and VCS_VERSION are no longer supported. "
    "Pass -DCTA_VERSION=<version>-<release> "
    "and remove the obsolete variables from the CMake cache.")
endif()

set(CTA_VERSION "" CACHE STRING "CTA version-release")

# RPM fields allow alphanumerics and . _ + ~ ^, with one hyphen separating the fields.
if(NOT CTA_VERSION MATCHES "^[A-Za-z0-9._+~^]+-[A-Za-z0-9._+~^]+$")
  message(FATAL_ERROR
    "CTA_VERSION must contain two nonempty RPM fields separated by exactly one hyphen, "
    "using only letters, numbers, '.', '_', '+', '~', or '^'; for example alice-experiment or 6-dev.")
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

configure_file(
  ${PROJECT_SOURCE_DIR}/version.cpp.in
  ${CMAKE_CURRENT_BINARY_DIR}/version.cpp
  @ONLY
)

# Create a library target for versioning so that we can link against it where needed instead of manually adding the cpp file everywhere
add_library(ctaversioninfo STATIC
  ${PROJECT_SOURCE_DIR}/version.hpp
  ${CMAKE_CURRENT_BINARY_DIR}/version.cpp
)

target_include_directories(ctaversioninfo PUBLIC ${PROJECT_SOURCE_DIR})

# Shared library versioning
set(CTA_LIBVERSION ${CTA_SOMAJOR}.${CTA_SOMINOR}.${CTA_SOPATCH})
