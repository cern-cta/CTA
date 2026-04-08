# SPDX-FileCopyrightText: 2003 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

find_package(ZLIB REQUIRED)

set(CTATAPEDSESSION_LIBRARY_SRCS
  CleanerSession.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  EmptyDriveProbe.cpp
  EncryptionControl.cpp
  TapeSessionReporter.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp
  RecallReportPacker.cpp
  Session.cpp
  TapeReadSingleThread.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp
  TransferTaskTracker.cpp
  DriveSessionTracker.cpp)

add_library(ctatapedsession
  ${CTATAPEDSESSION_LIBRARY_SRCS})

target_link_libraries(ctatapedsession ctacommon ctascheduler ctacatalogue ctamediachanger ctatapeddrive ctatapedrao ZLIB::ZLIB)

add_library(ctatapedsessionunittests SHARED
  DataTransferSessionTest.cpp
  DiskReadTaskTest.cpp
  DiskWriteTaskTest.cpp
  DiskWriteThreadPoolTest.cpp
  MigrationReportPackerTest.cpp
  RecallReportPackerTest.cpp
  RecallTaskInjectorTest.cpp
  TaskWatchDogTest.cpp
)
set_property(TARGET ctatapedsessionunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapedsessionunittests PROPERTY   VERSION "${CTA_LIBVERSION}")
if (NOT CTA_USE_PGSCHED)
  target_link_libraries(ctatapedsessionunittests ctaobjectstore)
endif()
install(TARGETS ctatapedsessionunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
