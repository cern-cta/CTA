# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

find_package (xrootd REQUIRED)

include_directories(/usr/include/shift) # TODO: why?
include_directories (${XROOTD_INCLUDE_DIR})
include_directories(/usr/include/tirpc) # rpc headers are now provided by libtirpc-devel

set(CTATAPEDFILE_LIBRARY_SRCS
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

add_executable(cta-BasicReadWriteTest
  BasicReadWriteTest.cpp)

target_link_libraries(cta-BasicReadWriteTest
  ctatapeddrive
  ctamediachanger
  ctascsi
  ctatapedsystem
  ctacommon
  ctatapedsession
  gtest
  pthread)

add_library(ctatapedfile
  ${CTATAPEDFILE_LIBRARY_SRCS})

target_link_libraries(ctatapedfile
  tirpc
  ctacommon
)

add_library(ctatapedfileunittests SHARED
  StructuresTest.cpp
  FileTest.cpp
  OSMFileTest.cpp
)

target_link_libraries(ctatapedfileunittests
  ctadisk
)

set_property(TARGET ctatapedfileunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedfileunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapedfileunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_executable(cta-osmReaderTest
  OsmReaderTest.cpp)

target_link_libraries(cta-osmReaderTest
  ctacommon
  ctatapedsession
  ctatapedfile
  gtest
)

install(TARGETS cta-osmReaderTest DESTINATION usr/bin)
