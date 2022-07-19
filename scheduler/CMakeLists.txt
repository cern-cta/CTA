# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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

include_directories (${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR} ${XROOTD_INCLUDE_DIR})

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  DiskReportRunner.cpp
  LabelMount.cpp
  LogicalLibrary.cpp
  MountType.cpp
  MountType.cpp
  PositioningMethod.cpp
  RepackReportThread.cpp
  RepackRequest.cpp
  RepackRequestManager.cpp
  RetrieveJob.cpp
  RetrieveMount.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SchedulerDatabaseFactory.cpp
  SchedulingInfos.cpp
  TapeMount.cpp)

if(NOT CTA_USE_PGSCHED)
  set (CTA_SCHEDULER_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
    OStoreDB/MemQueues.cpp
    OStoreDB/OStoreDB.cpp
    OStoreDB/OStoreDBWithAgent.cpp
    OStoreDB/QueueItor.cpp)
else()
  set (CTA_SCHEDULER_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
    PostgresSchedDB/PostgresSchedDB.cpp
    PostgresSchedDB/ArchiveMount.cpp
    PostgresSchedDB/ArchiveJob.cpp
    PostgresSchedDB/ArchiveJobQueueItor.cpp
    PostgresSchedDB/RetrieveMount.cpp
    PostgresSchedDB/RetrieveJob.cpp
    PostgresSchedDB/RetrieveJobQueueItor.cpp
    PostgresSchedDB/RepackRequestPromotionStatistics.cpp
    PostgresSchedDB/RepackRequest.cpp
    PostgresSchedDB/RepackReportBatch.cpp
    PostgresSchedDB/TapeMountDecisionInfo.cpp)
endif()

find_package(Protobuf3 REQUIRED)
include_directories (${PROTOBUF3_INCLUDE_DIRS})
add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})
set_property(TARGET ctascheduler PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascheduler PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctascheduler DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

if(NOT CTA_USE_PGSCHED)
  target_link_libraries (ctascheduler ctacommon ctaobjectstore ${PROTOBUF3_LIBRARIES} ctadisk)
else()
  target_link_libraries (ctascheduler ctacommon ${PROTOBUF3_LIBRARIES} ctadisk)
endif()

if(NOT CTA_USE_PGSCHED)
  add_library (ctaschedulerunittests SHARED
    SchedulerDatabaseFactory.cpp
    SchedulerDatabaseTest.cpp
    SchedulerTest.cpp
    OStoreDB/OStoreDBTest.cpp)
else()
  add_library (ctaschedulerunittests SHARED
    SchedulerDatabaseFactory.cpp
    SchedulerDatabaseTest.cpp
    GenericSchedulerTest.cpp
    PostgresSchedDB/PostgresSchedDBTest.cpp)
endif()
set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries (ctaschedulerunittests
  ctascheduler)

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
