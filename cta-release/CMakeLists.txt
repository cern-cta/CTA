# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

# As file(DOWNLOAD) fails silently
function(safedl SOURCEURL DESTFILE)
  file(DOWNLOAD "${SOURCEURL}"
    "${DESTFILE}"
    STATUS status
  )

  list(GET status 0 status_code)
  list(GET status 1 status_string)

  if(NOT status_code EQUAL 0)
    message(WARNING "error:
    downloading ${SOURCEURL} failed
    ${status_string}
  ")
  endif()
endfunction()

# We need the el version to set the proper cta versionlock version
include(${CMAKE_SOURCE_DIR}/cmake/UseRPMToolsEnvironment.cmake)

# Download package signing keys
safedl("https://storage-ci.web.cern.ch/storage-ci/storageci.key"
  "${CMAKE_CURRENT_SOURCE_DIR}/RPM-GPG-KEY-storageci"
)

safedl("https://download.ceph.com/keys/release.asc"
  "${CMAKE_CURRENT_SOURCE_DIR}/RPM-GPG-KEY-ceph"
)

safedl("https://yum.oracle.com/RPM-GPG-KEY-oracle-ol${OSV}"
  "${CMAKE_CURRENT_SOURCE_DIR}/RPM-GPG-KEY-oracle"
)

# generate versionlock file

if(NOT CTA_USE_PGSCHED)
execute_process(
    COMMAND grep "%package" ${CMAKE_CURRENT_SOURCE_DIR}/../cta.spec.in
    COMMAND awk "{print $3}"
    COMMAND grep -v "cta-release"
    OUTPUT_VARIABLE RESULT OUTPUT_STRIP_TRAILING_WHITESPACE
    )
else()
execute_process(
    COMMAND grep "%package" ${CMAKE_CURRENT_SOURCE_DIR}/../cta.spec.in
    COMMAND awk "{print $3}"
    COMMAND grep -v "cta-objectstore-tools"
    COMMAND grep -v "cta-release"
    OUTPUT_VARIABLE RESULT OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif()

string(REPLACE "\n" ";" RESULTS ${RESULT})

foreach(PNAME ${RESULTS})
    string(APPEND CTAVERSIONLOCK "${PNAME}-${CTA_VERSION}-${CTA_RELEASE}.${RPMTools_RPMBUILD_DIST}.*\n")
endforeach()

execute_process(
  COMMAND ${CMAKE_COMMAND} -E env PYTHONUNBUFFERED=1 python3 ${CMAKE_SOURCE_DIR}/continuousintegration/project-json/generate_versionlock.py --platform ${RPMTools_RPMBUILD_DIST}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  OUTPUT_VARIABLE VERSIONLOCK
  OUTPUT_STRIP_TRAILING_WHITESPACE
)

# Repos files
file (GLOB REPO_FILES
  "${CMAKE_SOURCE_DIR}/continuousintegration/docker/${RPMTools_RPMBUILD_DIST}/etc/yum.repos.d-public/*.repo"
)

string(APPEND CTAVERSIONLOCK "${VERSIONLOCK}")
file(GENERATE OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/versionlock.cta CONTENT "${CTAVERSIONLOCK}")

# Signing keys
file (GLOB KEY_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/RPM-GPG-KEY-*"
)

# Install package files
install (FILES ${REPO_FILES}
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/yum.repos.d)
install (FILES ${KEY_FILES}
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/pki/rpm-gpg)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/versionlock.cta
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/yum/pluginconf.d)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-versionlock
  DESTINATION usr/${CMAKE_INSTALL_BINDIR})
