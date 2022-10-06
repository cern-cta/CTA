# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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

add_subdirectory (restore_files)

find_package(xrootdclient REQUIRED)
find_package(Protobuf3 REQUIRED)

# XRootD SSI
include_directories(${XROOTD_INCLUDE_DIR} ${XROOTD_INCLUDE_DIR}/private)

# XRootD SSI Protocol Buffer bindings
include_directories(${XRD_SSI_PB_DIR}/include ${XRD_SSI_PB_DIR}/eos_cta/include)

# Compiled protocol buffers
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

#
# cta-send-event CLOSEW|PREPARE|ABORT_PREPARE
# injects a workflow event into the CTA Frontend
#
add_executable(cta-send-event CtaSendEvent.cpp common/CmdLineArgs.cpp)
target_link_libraries(cta-send-event ctacommon XrdSsiPbEosCta XrdSsiLib XrdUtils)
set_property(TARGET cta-send-event APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

#
# verify-file <archiveFileId> <vid>
# recalls a file from tape without writing a disk copy
#
add_executable(cta-verify-file CtaVerifyFile.cpp common/CmdLineArgs.cpp)
target_link_libraries(cta-verify-file ctacommon XrdSsiPbEosCta XrdSsiLib XrdUtils)
set_property (TARGET cta-verify-file APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})

install(TARGETS cta-send-event DESTINATION usr/bin)
install(TARGETS cta-verify-file DESTINATION usr/bin)