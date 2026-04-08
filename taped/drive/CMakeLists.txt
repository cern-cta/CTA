# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)


set(CTATAPEDDRIVE_LIBRARY_SRCS
  DriveGeneric.cpp
  FakeDrive.cpp)

add_library(ctatapeddrive ${CTATAPEDDRIVE_LIBRARY_SRCS})
target_link_libraries(ctatapeddrive ctatapedscsi ctatapedaemon)

add_executable(cta-TapeDriveReadWriteTest
  TapeDriveReadWriteTest.cpp)
target_link_libraries(cta-TapeDriveReadWriteTest
  ctatapeddrive
  ctamediachanger
  ctatapedscsi
  ctatapedsystem
  ctacommon
  ctatapedsession
  gtest
  pthread)

add_library(ctatapeddriveunittests SHARED
  DriveTest.cpp)
set_property(TARGET ctatapeddriveunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeddriveunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeddriveunittests
  ctatapedsession
  ctatapedsystem
  ctatapeddrive)

install(TARGETS ctatapeddriveunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
