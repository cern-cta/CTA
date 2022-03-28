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

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

find_package( ZLIB REQUIRED )


add_executable(cta-readtp
  ReadtpCmd.cpp
  ReadtpCmdMain.cpp
  ReadtpCmdLineArgs.cpp
  CmdLineTool.cpp
  TapeFseqRange.cpp
  TapeFseqRangeSequence.cpp
  TapeFseqRangeListSequence.cpp
  TapeFseqSequenceParser.cpp)

target_link_libraries (cta-readtp
  ctacommon
  TapeDrive
  ctamediachanger
  ctacatalogue
  SCSI
)

# need to be removed when drop dependencies to taped
find_package(Protobuf3 REQUIRED)
set_property (TARGET cta-readtp APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-readtp APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

install (TARGETS cta-readtp DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-readtp.1cta DESTINATION /usr/share/man/man1)
