# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

find_program(CTA_DPKG_BUILDPACKAGE NAMES dpkg-buildpackage REQUIRED)
find_program(CTA_DPKG NAMES dpkg REQUIRED)
execute_process(COMMAND "${CTA_DPKG}" --print-architecture
  OUTPUT_VARIABLE CTA_DEB_ARCH OUTPUT_STRIP_TRAILING_WHITESPACE COMMAND_ERROR_IS_FATAL ANY)
if(NOT PLATFORM STREQUAL "debian13" OR NOT CTA_DEB_ARCH STREQUAL "amd64")
  message(FATAL_ERROR "Debian packaging currently supports Debian 13 amd64 only")
endif()

# Debian upstream versions must begin with a digit and use Debian's version alphabet.
if(NOT CTA_VERSION MATCHES "^[0-9][A-Za-z0-9.+~]*-[A-Za-z0-9.+~]+$")
  message(FATAL_ERROR "Invalid Debian version: ${CTA_VERSION}; use e.g. 6.0.0-1 or 6-dev")
endif()
string(REGEX REPLACE "-[^-]+$" "" CTA_DEB_UPSTREAM_VERSION "${CTA_VERSION}")
set(CTA_DEB_BUILD_SYSTEM "cmake")
if(CMAKE_GENERATOR STREQUAL "Ninja")
  set(CTA_DEB_BUILD_SYSTEM "cmake+ninja")
elseif(NOT CMAKE_GENERATOR STREQUAL "Unix Makefiles")
  message(FATAL_ERROR "Debian packaging supports Ninja or Unix Makefiles")
endif()

set(CTA_DEB_OUTPUT_DIR "${CMAKE_BINARY_DIR}/DEB")
set(CTA_DEB_SOURCE_DIR "${CTA_DEB_OUTPUT_DIR}/cta-${CTA_VERSION}")
set(CTA_DEB_TEST_DEPENDS "")
if(CTA_BUILD_TEST_PACKAGES)
  set(CTA_DEB_TEST_DEPENDS ", valgrind, postgresql, moreutils, make, libgtest-dev, libtirpc-dev")
endif()
string(TIMESTAMP CTA_DEB_CHANGELOG_DATE "%a, %d %b %Y %H:%M:%S +0000" UTC)
foreach(name control rules changelog)
  configure_file("packaging/deb/${name}.in" "${CTA_DEB_OUTPUT_DIR}/debian/${name}" @ONLY)
endforeach()
foreach(name cta.postinst copyright source/format)
  configure_file("packaging/deb/${name}" "${CTA_DEB_OUTPUT_DIR}/debian/${name}" COPYONLY)
endforeach()
file(CHMOD "${CTA_DEB_OUTPUT_DIR}/debian/rules" "${CTA_DEB_OUTPUT_DIR}/debian/cta.postinst"
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)
configure_file(cmake/BuildDeb.cmake.in BuildDeb.cmake @ONLY)

# Source packages use clean staging; binary packages build directly from the checkout.
add_custom_target(cta_dsc
  COMMAND "${CMAKE_COMMAND}" -DCTA_DEB_SOURCE_ONLY=ON -P "${CMAKE_BINARY_DIR}/BuildDeb.cmake"
  VERBATIM)
if(CTA_BUILD_MODE STREQUAL "binary-package")
  add_custom_target(cta_deb
    COMMAND "${CMAKE_COMMAND}" -DCTA_DEB_SOURCE_ONLY=OFF -P "${CMAKE_BINARY_DIR}/BuildDeb.cmake"
    JOB_POOL console VERBATIM)
endif()
