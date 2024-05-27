# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)

# CTA's cta-taped code
add_subdirectory (daemon)
add_subdirectory (session)
# The tape session's threads are in a separate directory (session, but compiled
# from the previous one to create a single library).
add_subdirectory (tapelabel)
add_subdirectory (readtp)

include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_executable (cta-taped cta-taped.cpp)
find_package(Protobuf3 REQUIRED)
target_link_libraries(cta-taped
  ctatapedaemon ctacommon ${PROTOBUF3_LIBRARIES})
set_property (TARGET cta-taped APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES cta-taped.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-taped.logrotate DESTINATION /etc/logrotate.d RENAME cta-taped)
install (FILES cta-taped.sysconfig DESTINATION /etc/sysconfig RENAME cta-taped)
install (FILES cta-taped.service DESTINATION /etc/systemd/system)
install (FILES cta-taped@.service DESTINATION /etc/systemd/system)

# CTA's cta-taped system tests.
add_library(cta-tapedSystemTests SHARED
  cta-tapedSystemtests.cpp)
set_property(TARGET cta-tapedSystemTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-tapedSystemTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(cta-tapedSystemTests
  unitTestHelper
  ctacommon)

install(TARGETS cta-tapedSystemTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
