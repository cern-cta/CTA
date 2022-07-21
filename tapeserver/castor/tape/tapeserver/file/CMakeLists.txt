# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2003-2022 CERN
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

find_package (xrootd REQUIRED)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories (${XROOTD_INCLUDE_DIR})

set(TAPESERVER_FILE_LIBRARY_SRCS
  CtaFileReader.cpp
  CtaReadSession.cpp
  FileReader.cpp
  FileReaderFactory.cpp
  FileWriter.cpp
  HeaderChecker.cpp
  LabelSession.cpp
  OsmFileReader.cpp
  OsmReadSession.cpp
  OsmFileStructure.cpp
  CpioFileHeaderStructure.cpp
  ReadSession.cpp
  ReadSessionFactory.cpp
  Structures.cpp
  WriteSession.cpp
)

add_executable(BasicReadWriteTest
  BasicReadWriteTest.cpp)

target_link_libraries(BasicReadWriteTest
  TapeDrive
  ctamediachanger
  SCSI
  System
  ctacommon
  ctaTapeServerDaemon
  gtest
  pthread)

add_library(File
  ${TAPESERVER_FILE_LIBRARY_SRCS})

add_library(ctatapeserverfileunittests SHARED
  StructuresTest.cpp
  FileTest.cpp
)

target_link_libraries(ctatapeserverfileunittests
  ctadisk
)

set_property(TARGET ctatapeserverfileunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverfileunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverfileunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
