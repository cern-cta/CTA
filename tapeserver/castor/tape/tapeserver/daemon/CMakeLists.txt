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

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver/h)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

find_package(Protobuf3 REQUIRED)
find_package( ZLIB REQUIRED )

set(CTATAPESERVERDAEMON_LIBRARY_SRCS
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
  TapeWriteTask.cpp)

add_library(ctaTapeServerDaemon
  ${CTATAPESERVERDAEMON_LIBRARY_SRCS})

target_link_libraries(ctaTapeServerDaemon ctamessages ctacommon ${PROTOBUF3_LIBRARIES} ctascheduler ctalegacymsg ctacatalogue TapeDrive ctarao)
add_dependencies(ctaTapeServerDaemon ctamessagesprotobuf)

include_directories(${PROTOBUF3_INCLUDE_DIRS})
add_library(ctatapeserverdaemonunittests SHARED
  DataTransferSessionTest.cpp
  DiskReadTaskTest.cpp
  DiskWriteTaskTest.cpp
  DiskWriteThreadPoolTest.cpp
  MigrationReportPackerTest.cpp
  RecallReportPackerTest.cpp
  RecallTaskInjectorTest.cpp
  TaskWatchDogTest.cpp
  #${CMAKE_BINARY_DIR}/catalogue/OracleCatalogueSchema.cpp
)
set_property(TARGET ctatapeserverdaemonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverdaemonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeserverdaemonunittests
  ctamessagesutils)
  #ctaschedulerutils

install(TARGETS ctatapeserverdaemonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
