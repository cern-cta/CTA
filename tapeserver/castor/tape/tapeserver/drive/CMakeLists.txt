# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

set(TAPEDRIVE_LIBRARY_SRCS
  DriveGeneric.cpp
  FakeDrive.cpp)

add_library(TapeDrive ${TAPEDRIVE_LIBRARY_SRCS})
target_link_libraries(TapeDrive SCSI ctatapedaemon)

add_executable(TapeDriveReadWriteTest
  TapeDriveReadWriteTest.cpp)
target_link_libraries(TapeDriveReadWriteTest
  TapeDrive
  ctamediachanger
  SCSI
  System
  ctacommon
  #ctaclient
  ctaTapeServerDaemon
  gtest
  pthread)

add_library(ctatapeserverdriveunittests SHARED
  DriveTest.cpp)
set_property(TARGET ctatapeserverdriveunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverdriveunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeserverdriveunittests
  ctaTapeServerDaemon
  System
  TapeDrive)

install(TARGETS ctatapeserverdriveunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
