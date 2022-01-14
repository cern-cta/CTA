# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

# We need the el version to select the proper key for Oracle repo
include(../cmake/UseRPMToolsEnvironment.cmake)

if( "${RPMTools_RPMBUILD_DIST}" MATCHES "\\.el([0-9])\\.")
  set(OSV "${CMAKE_MATCH_1}")
else()
# Default to el7
  set(OSV "7")
endif()

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
execute_process(
    COMMAND grep "%package" ../cta.spec.in
    COMMAND awk "{print $3}"
    COMMAND grep -v "cta-release"
    OUTPUT_VARIABLE RESULT OUTPUT_STRIP_TRAILING_WHITESPACE
    )

string(REPLACE "\n" ";" RESULTS ${RESULT})

foreach(PNAME ${RESULTS})
    string(APPEND CTAVERSIONLOCK "0:${PNAME}-${CTA_VERSION}-${CTA_RELEASE}${RPMTools_RPMBUILD_DIST}.*\n")
endforeach()

file(READ ../continuousintegration/docker/ctafrontend/cc7/etc/yum/pluginconf.d/versionlock.list VERSIONLOCK)
string(APPEND CTAVERSIONLOCK "${VERSIONLOCK}")
file(GENERATE OUTPUT ${CMAKE_CURRENT_SOURCE_DIR}/versionlock.cta CONTENT "${CTAVERSIONLOCK}")

# Repos files
file (GLOB REPO_FILES
  "${CMAKE_CURRENT_SOURCE_DIR}/*.repo"
)

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
