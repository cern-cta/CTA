# XRootD SSI/Protocol Buffer Interface Project
# Copyright 2018 CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

find_package(xrootdclient REQUIRED)
find_package(xrootd REQUIRED)
find_package(Protobuf3 REQUIRED)

add_subdirectory(protobuf)

#
# XRootD SSI
#
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

#
# XRootD SSI Protocol Buffer bindings
#
include_directories(${CMAKE_SOURCE_DIR}/)

include_directories (${CMAKE_SOURCE_DIR}/xrootd-ssi-protobuf-interface/include)

#
# Compiled protocol buffers
#
include_directories(${CMAKE_BINARY_DIR}/XRootdSSiRmcd ${PROTOBUF3_INCLUDE_DIRS})

#
# Test Client
#
add_executable(cta-xsmc RmcdClient.cpp)
target_link_libraries(cta-xsmc XrdSsi-4 XrdSsiLib XrdSsiPbRmcd XrdUtils)
set_property(TARGET cta-xsmc APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
install (TARGETS cta-xsmc DESTINATION /usr/bin)
install (FILES cta-xsmc-mount.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-xsmc-dismount.1cta DESTINATION /usr/share/man/man1)

#
# XRootD SSI plugin for Test Server
#
add_library(XrdSsiRmcd MODULE
  RmcdServiceProvider.cpp
  RmcdRequestProc.cpp
  rmc_send_scsi_cmd.cpp
  serrno.cpp
  rmc_smcsubr.cpp
)
target_link_libraries(XrdSsiRmcd XrdSsi-4 XrdSsiLib XrdSsiPbRmcd XrdUtils)
set_property(TARGET XrdSsiRmcd APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
install(TARGETS XrdSsiRmcd DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install (FILES cta-xrmcd.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (FILES cta-xrmcd.logrotate DESTINATION /etc/logrotate.d RENAME cta-xrmcd)
install (FILES cta-xrmcd.sysconfig DESTINATION /etc/sysconfig RENAME cta-xrmcd)
install (FILES cta-xrmcd.service DESTINATION /etc/systemd/system)
