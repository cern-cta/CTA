# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
# Default CTA Version
set(CTA_VERSION 0)
set(CTA_RELEASE 1)
set(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION "v0.0")

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

# Get version number from environment if set.
if(NOT $ENV{CTA_VERSION} STREQUAL "")
  set(CTA_VERSION $ENV{CTA_VERSION})
endif()

# Get xrootd-ssi-protobuf-version-number from environment if set.
if(NOT $ENV{XROOTD_SSI_PROTOBUF_INTERFACE_VERSION} STREQUAL "")
  set(XROOTD_SSI_PROTOBUF_INTERFACE_VERSION $ENV{XROOTD_SSI_PROTOBUF_INTERFACE_VERSION})
endif()

# Get release number from environment if set
if(NOT $ENV{CTA_RELEASE} STREQUAL "")
  set(CTA_RELEASE $ENV{CTA_RELEASE})
endif()

# Change the release number if VCS version is provided
if(DEFINED VCS_VERSION)
  set(CTA_RELEASE ${VCS_VERSION})
endif()

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
