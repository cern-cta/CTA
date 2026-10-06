# SPDX-FileCopyrightText: 2022 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

# Callers provide the complete build version, including variant and platform.
if(DEFINED CTA_RELEASE OR DEFINED VCS_VERSION)
  message(FATAL_ERROR
    "CTA_RELEASE and VCS_VERSION are no longer supported. "
    "Pass the complete version with -DCTA_VERSION=<version>-<release>[.<variant>].<platform> "
    "and remove the obsolete variables from the CMake cache.")
endif()

set(CTA_VERSION "" CACHE STRING "Complete CTA build version, including release, variant and platform")

if(NOT CTA_VERSION MATCHES "^[0-9]+(\\.[0-9]+)*-[a-z0-9]+([.][a-z0-9]+)*$")
  message(FATAL_ERROR
    "CTA_VERSION must be a complete build version without a leading v and with exactly one separating hyphen, "
    "for example 6.12.0.0-1.pgall.el9 or 6-dev.el9.")
endif()

# Match the detected platform literally, including dots in distribution versions.
string(REPLACE "." "[.]" _cta_version_platform_pattern "${PLATFORM}")
if(NOT CTA_VERSION MATCHES "[.]${_cta_version_platform_pattern}$")
  message(FATAL_ERROR "CTA_VERSION must end with the detected platform suffix .${PLATFORM}.")
endif()

# Validate the supplied variant against the actual build options without rewriting CTA_VERSION.
set(_cta_expected_variant "")
if(CTA_USE_PGSCHED)
  if(CTA_WITH_ORACLE)
    set(_cta_expected_variant "pgsched")
  else()
    set(_cta_expected_variant "pgall")
  endif()
elseif(NOT CTA_WITH_ORACLE)
  set(_cta_expected_variant "pgcat")
endif()

set(_cta_expected_suffix ".${PLATFORM}")
if(NOT _cta_expected_variant STREQUAL "")
  set(_cta_expected_suffix ".${_cta_expected_variant}${_cta_expected_suffix}")
endif()

# Remove the platform and at most one terminal variant to detect misplaced or repeated labels.
string(REGEX REPLACE "[.]${_cta_version_platform_pattern}$" "" _cta_version_base "${CTA_VERSION}")
set(_cta_supplied_variant "")
if(_cta_version_base MATCHES "[.](pgsched|pgcat|pgall)$")
  set(_cta_supplied_variant "${CMAKE_MATCH_1}")
  string(REGEX REPLACE "[.](pgsched|pgcat|pgall)$" "" _cta_version_base "${_cta_version_base}")
endif()

if(NOT "${_cta_supplied_variant}" STREQUAL "${_cta_expected_variant}"
    OR _cta_version_base MATCHES "[.-](pgsched|pgcat|pgall)([.]|$)")
  message(FATAL_ERROR
    "CTA_VERSION '${CTA_VERSION}' does not match CTA_USE_PGSCHED=${CTA_USE_PGSCHED}, "
    "CTA_WITH_ORACLE=${CTA_WITH_ORACLE}. Expected suffix '${_cta_expected_suffix}' "
    "with no missing, conflicting, duplicated or misplaced variant labels.")
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
