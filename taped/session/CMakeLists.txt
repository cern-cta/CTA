# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

find_package(ZLIB REQUIRED)

# Tracking is shared with drive cleanup; keeping it separate avoids a drive/session library cycle.
add_library(ctatapedsessionstate
  TapeSessionState.cpp
  TapeSessionTracker.cpp
  SessionType.cpp
)
target_link_libraries(ctatapedsessionstate PUBLIC ctacommon)

add_library(ctatapedsession
  DriveSession.cpp
  TapeSession.cpp
  TapeSessionReporter.cpp

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

  pipeline/TransferTaskTracker.cpp
)

target_link_libraries(ctatapedsession
  ctacommon
  ctaruntime
  ctatapedscheduler
  ctatapedsessionstate
  ctascheduler
  ctacatalogue
  ctamediachanger
  ctatapeddrive
  ctatapedfile
  ctatapedrao
  ctatelemetrytaped
  ZLIB::ZLIB)

add_library(ctatapedsessionunittests SHARED
  DriveSessionTest.cpp
  ScopedTapeSessionReporterTest.cpp
  WorkerFailureHandlingTest.cpp
  TapeSessionTest.cpp
  TapeSessionReporterTest.cpp
  TapeSessionTrackerTest.cpp

  archive/DiskReadTaskTest.cpp
  archive/MigrationReportPackerTest.cpp

  retrieve/DiskWriteTaskTest.cpp
  retrieve/DiskWriteThreadPoolTest.cpp
  retrieve/RecallReportPackerTest.cpp
  retrieve/RecallTaskInjectorTest.cpp
)
set_property(TARGET ctatapedsessionunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedsessionunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
target_link_libraries(ctatapedsessionunittests PRIVATE ctatapedsession gtest gmock cta-unit-test-helper)
if (NOT CTA_USE_PGSCHED)
  target_link_libraries(ctatapedsessionunittests PRIVATE ctaobjectstore)
endif()
install(TARGETS ctatapedsessionunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
