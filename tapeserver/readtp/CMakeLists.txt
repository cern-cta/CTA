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
cmake_minimum_required (VERSION 2.6)

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