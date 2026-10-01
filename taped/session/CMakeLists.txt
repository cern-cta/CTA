# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

find_package(ZLIB REQUIRED)

set(CTATAPEDSESSION_LIBRARY_SRCS
  TapeSession.cpp
  TapeSessionReporter.cpp
  TapeSessionState.cpp
  SessionType.cpp

  # These operations depend on session tracking and retain their existing library membership.
  ../drive/DriveCleaner.cpp
  ../drive/EmptyDriveProbe.cpp
  ../drive/EncryptionControl.cpp
  ../drive/MountedTape.cpp

  archive/DiskReadTask.cpp
  archive/DiskReadThreadPool.cpp
  archive/MigrationMemoryManager.cpp
  archive/MigrationReportPacker.cpp
  archive/MigrationTaskInjector.cpp
  archive/TapeWriteSingleThread.cpp
  archive/TapeWriteTask.cpp

  retrieve/DiskWriteTask.cpp
  retrieve/DiskWriteThreadPool.cpp
  retrieve/RecallMemoryManager.cpp
  retrieve/RecallReportPacker.cpp
  retrieve/RecallTaskInjector.cpp
  retrieve/TapeReadSingleThread.cpp

  pipeline/TransferTaskTracker.cpp)

add_library(ctatapedsession
  ${CTATAPEDSESSION_LIBRARY_SRCS})

target_link_libraries(ctatapedsession
  ctacommon
  ctaruntime
  ctascheduler
  ctacatalogue
  ctamediachanger
  ctatapeddrive
  ctatapedfile
  ctatapedrao
  ctatelemetrytaped
  ZLIB::ZLIB)

add_library(ctatapedsessionunittests SHARED
  TapeSessionTest.cpp
  TapeSessionReporterTest.cpp
  TapeSessionTrackerTest.cpp

  ../drive/DriveCleanerTest.cpp
  ../drive/MountedTapeTest.cpp

  archive/DiskReadTaskTest.cpp
  archive/MigrationReportPackerTest.cpp

  retrieve/DiskWriteTaskTest.cpp
  retrieve/DiskWriteThreadPoolTest.cpp
  retrieve/RecallReportPackerTest.cpp
  retrieve/RecallTaskInjectorTest.cpp
)
set_property(TARGET ctatapedsessionunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedsessionunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctatapedsessionunittests PRIVATE ctatapedsession)
if (NOT CTA_USE_PGSCHED)
  target_link_libraries(ctatapedsessionunittests PRIVATE ctaobjectstore)
endif()
install(TARGETS ctatapedsessionunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
