# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

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

add_executable(cta-test-basic-read-write
  BasicReadWriteTest.cpp)

target_link_libraries(cta-test-basic-read-write
  ctatapedfile
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
  EnstoreFileTest.cpp
)

target_include_directories(ctatapedfileunittests PRIVATE ${XROOTD_INCLUDE_DIR})

target_link_libraries(ctatapedfileunittests
  ctatapedfile
  ctadisk
)

set_property(TARGET ctatapedfileunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedfileunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS ctatapedfileunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()

# The reader executables pull in mockWrapper and need Google Mock at the final link.
add_executable(cta-test-osm-reader
  OsmReaderTest.cpp)

target_link_libraries(cta-test-osm-reader
  ctacommon
  ctatapedsession
  ctatapedfile
  ctatapeddrive
  ctatapedsystem
  gmock
  gtest
)

add_executable(cta-test-enstore-large-reader
  EnstoreLargeReaderTest.cpp)

target_link_libraries(cta-test-enstore-large-reader
  ctacommon
  ctatapedsession
  ctatapedfile
  ctatapeddrive
  ctatapedsystem
  gmock
  gtest
)

# RPMs retain a separate reader package; Debian includes these in the optional tests package.
if(CTA_BUILD_TEST_PACKAGES OR CTA_PACKAGE_FORMAT STREQUAL "rpm")
  install(TARGETS cta-test-osm-reader cta-test-enstore-large-reader DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
