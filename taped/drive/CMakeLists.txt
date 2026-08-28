# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


add_library(ctatapeddrive
  DriveGeneric.cpp
  FakeDrive.cpp
  DriveCleaner.cpp
  DriveReservation.cpp
  EmptyDriveProbe.cpp
  EncryptionControl.cpp
  MountedTape.cpp
)
# Cleanup records session state without depending on the session orchestration library.
target_link_libraries(ctatapeddrive
  ctacommon
  ctascsi
  ctacatalogue
  ctamediachanger
  ctatapedsystem
  ctatapedfile
  ctatapedsessionstate
  ctatelemetrytaped
)

add_executable(cta-test-tape-drive-read-write
  TapeDriveReadWriteTest.cpp)
target_link_libraries(cta-test-tape-drive-read-write
  ctatapeddrive
  ctamediachanger
  ctascsi
  ctatapedsystem
  ctacommon
  ctatapedsession
  gtest
  pthread)

add_library(ctatapeddriveunittests SHARED
  DriveTest.cpp
  DriveCleanerTest.cpp
  EmptyDriveProbeTest.cpp
  MountedTapeTest.cpp
)
set_property(TARGET ctatapeddriveunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeddriveunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeddriveunittests
  ctatapedsession
  ctatapedsystem
  ctatapeddrive
)
if(NOT CTA_USE_PGSCHED)
  target_link_libraries(ctatapeddriveunittests ctaobjectstore)
endif()

if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS ctatapeddriveunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
