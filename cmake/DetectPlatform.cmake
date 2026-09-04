# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

set(CTA_OS_RELEASE_FILE "/etc/os-release" CACHE FILEPATH
  "Path to the os-release file used for platform detection")

if(NOT UNIX OR NOT EXISTS "${CTA_OS_RELEASE_FILE}")
  message(FATAL_ERROR
    "CTA platform detection requires an os-release file on a Unix system: ${CTA_OS_RELEASE_FILE}")
endif()

file(READ "${CTA_OS_RELEASE_FILE}" CTA_OS_RELEASE_CONTENTS)

string(REGEX MATCH "(^|\n)ID=\"?([a-zA-Z0-9._-]+)\"?" _cta_os_id_match "${CTA_OS_RELEASE_CONTENTS}")
set(CTA_OS_ID "${CMAKE_MATCH_2}")

string(REGEX MATCH "(^|\n)VERSION_ID=\"?([0-9]+([.][0-9]+)*)" _cta_os_version_match "${CTA_OS_RELEASE_CONTENTS}")
set(CTA_OS_VERSION "${CMAKE_MATCH_2}")
string(REGEX MATCH "^[0-9]+" CTA_OS_VERSION_MAJOR "${CTA_OS_VERSION}")

if(NOT CTA_OS_ID OR NOT CTA_OS_VERSION)
  message(FATAL_ERROR
    "Could not detect ID and VERSION_ID from ${CTA_OS_RELEASE_FILE}")
endif()

set(CTA_PLATFORM_SUPPORTED FALSE)

if(CTA_OS_ID MATCHES "^(centos|rhel|almalinux|rocky)$")
  set(CTA_OS_FAMILY "enterprise-linux")
  set(CTA_PACKAGE_FORMAT "rpm")
  set(PLATFORM "el${CTA_OS_VERSION_MAJOR}")
  set(CTA_PLATFORM_SUPPORTED TRUE)
elseif(CTA_OS_ID MATCHES "^(debian|ubuntu)$")
  set(CTA_OS_FAMILY "debian")
  set(CTA_PACKAGE_FORMAT "deb")
  set(PLATFORM "${CTA_OS_ID}${CTA_OS_VERSION}")
else()
  set(CTA_OS_FAMILY "unknown")
  set(CTA_PACKAGE_FORMAT "unknown")
endif()
