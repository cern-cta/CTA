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

add_executable(cta-tape-label
  TapeLabelCmd.cpp
  CmdLineTool.cpp
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdMain.cpp)

target_link_libraries (cta-tape-label
  ctacommon
  TapeDrive
  ctamediachanger
  ctacatalogue
  SCSI
)

# need to be removed when drop dependencies to taped
find_package(Protobuf3 REQUIRED)
set_property (TARGET cta-tape-label APPEND PROPERTY INSTALL_RPATH ${PROTOBUF3_RPATH})
if (OCCI_SUPPORT)
  set_property (TARGET cta-tape-label APPEND PROPERTY INSTALL_RPATH ${ORACLE-INSTANTCLIENT_RPATH})
endif (OCCI_SUPPORT)

install (TARGETS cta-tape-label DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-tape-label.1cta DESTINATION /usr/share/man/man1)

add_library(ctatapelabelunittests SHARED
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdLineArgsTest.cpp)

set_property(TARGET ctatapelabelunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapelabelunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctatapelabelunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
