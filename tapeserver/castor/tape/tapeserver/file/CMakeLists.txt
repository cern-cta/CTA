# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

find_package (xrootd REQUIRED)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories (${XROOTD_INCLUDE_DIR})
include_directories(/usr/include/tirpc) # rpc headers are now provided by libtirpc-devel

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
  EnstoreFileReader.cpp
  EnstoreReadSession.cpp
  EnstoreLargeFileReader.cpp
  EnstoreLargeReadSession.cpp
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

target_link_libraries(File
  tirpc
)

add_library(ctatapeserverfileunittests SHARED
  StructuresTest.cpp
  FileTest.cpp
  OSMFileTest.cpp
)

target_link_libraries(ctatapeserverfileunittests
  ctadisk
)

set_property(TARGET ctatapeserverfileunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverfileunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverfileunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_executable(cta-osmReaderTest
  OsmReaderTest.cpp)

target_link_libraries(cta-osmReaderTest
  ctacommon
  ctaTapeServerDaemon
  File
  gtest
)

install(TARGETS cta-osmReaderTest DESTINATION usr/bin)
