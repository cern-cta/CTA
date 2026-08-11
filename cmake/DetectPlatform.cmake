# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

if(NOT UNIX OR NOT EXISTS "/etc/os-release")
  message(FATAL_ERROR "CTA platform detection requires /etc/os-release on a Unix system")
endif()

file(READ "/etc/os-release" CTA_OS_RELEASE_CONTENTS)

string(REGEX MATCH "ID=\"?([a-zA-Z0-9]+)\"?" _cta_os_id_match "${CTA_OS_RELEASE_CONTENTS}")
set(CTA_OS_ID "${CMAKE_MATCH_1}")

string(REGEX MATCH "VERSION_ID=\"?([0-9]+)" _cta_os_version_match "${CTA_OS_RELEASE_CONTENTS}")
set(CTA_OS_VERSION "${CMAKE_MATCH_1}")

if(CTA_OS_ID MATCHES "^(centos|rhel|almalinux|rocky)$" AND CTA_OS_VERSION)
  set(PLATFORM "el${CTA_OS_VERSION}")
else()
  message(FATAL_ERROR
    "Unsupported CTA build platform: ID='${CTA_OS_ID}', VERSION_ID='${CTA_OS_VERSION}'")
endif()
