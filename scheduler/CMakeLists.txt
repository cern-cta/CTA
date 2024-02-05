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

add_subdirectory(PostgresSchedDB/schema)

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  DiskReportRunner.cpp
  LabelMount.cpp
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
  SchedulerDatabaseFactory.cpp)

find_package(Protobuf3 REQUIRED)
include_directories (${PROTOBUF3_INCLUDE_DIRS})

# Definitions specific to only Objectstore module lib compilation:

set (CTA_SCHEDULER_OS_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
  OStoreDB/MemQueues.cpp
  OStoreDB/OStoreDB.cpp
  OStoreDB/OStoreDBWithAgent.cpp
  OStoreDB/QueueItor.cpp)

# End of definitions specific to Objectstore lib compilation

# Definitions specific to only PostgreSched DB module lib compilation:
# Protobuf shall bot be needed for PostgreSched (commented out)
# set (CTAProtoFiles
#   PostgresSchedDB/rowblobs.proto)

# Generate should be used on protofile in the current directory
# This needs to be changed. todo
PROTOBUF3_GENERATE_CPP(CTAProtoSources0 CTAProtoHeaders0 ${CTAProtoFiles})

# Protobuf shall bot be needed for PostgreSched (commented out)
# set (CTAProtoSources
#   ${CMAKE_BINARY_DIR}/scheduler/PostgresSchedDB/rowblobs.pb.cc)

# set (CTAProtoHeaders
#   ${CMAKE_BINARY_DIR}/scheduler/PostgresSchedDB/rowblobs.pb.h)

SET_SOURCE_FILES_PROPERTIES(${CTAProtoSources} PROPERTIES OBJECT_DEPENDS ${CTAProtoSources0})
SET_SOURCE_FILES_PROPERTIES(${CTAProtoHeaders} PROPERTIES OBJECT_DEPENDS ${CTAProtoHeaders0})

SET_SOURCE_FILES_PROPERTIES(
  ${CTAProtoSources} ${CTAProtoHeaders}
  PROPERTIES GENERATED TRUE)

include_directories (${CMAKE_BINARY_DIR}/scheduler)

set (CTAProtoDependants
  PostgresSchedDB/RetrieveRequest.cpp
  PostgresSchedDB/RepackRequest.cpp)

SET_SOURCE_FILES_PROPERTIES(${CTAProtoDependants}
  PROPERTIES OBJECT_DEPENDS ${CTAProtoHeaders})

set (CTA_SCHEDULER_PG_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
  ${CTAProtoSources}
  PostgresSchedDB/PostgresSchedDB.cpp
  PostgresSchedDB/ArchiveMount.cpp
  PostgresSchedDB/ArchiveJob.cpp
  PostgresSchedDB/ArchiveJobQueueItor.cpp
  PostgresSchedDB/ArchiveRequest.cpp
  PostgresSchedDB/Helpers.cpp
  PostgresSchedDB/RetrieveMount.cpp
  PostgresSchedDB/RetrieveRequest.cpp
  PostgresSchedDB/RetrieveJob.cpp
  PostgresSchedDB/RetrieveJobQueueItor.cpp
  PostgresSchedDB/RepackRequestPromotionStatistics.cpp
  PostgresSchedDB/RepackRequest.cpp
  PostgresSchedDB/RepackReportBatch.cpp
  PostgresSchedDB/TapeMountDecisionInfo.cpp
  PostgresSchedDB/sql/ArchiveJobQueue.cpp
  PostgresSchedDB/sql/RetrieveJobQueue.cpp
  PostgresSchedDB/sql/RepackJobQueue.cpp
  PostgresSchedDB/sql/Transaction.cpp)

# End of definitions specific to Postgres scheduler lib compilation

if(NOT CTA_USE_PGSCHED)
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_OS_SRC_FILES})
    target_link_libraries (ctascheduler ctacommon ctaobjectstore ${PROTOBUF3_LIBRARIES} ctadisk)
    add_library (ctascheduler-postgres SHARED ${CTA_SCHEDULER_PG_SRC_FILES})
    target_link_libraries (ctascheduler-postgres ctacommon ${PROTOBUF3_LIBRARIES} ctadisk)
    set_property(TARGET ctascheduler-postgres PROPERTY SOVERSION "${CTA_SOVERSION}")
    set_property(TARGET ctascheduler-postgres PROPERTY   VERSION "${CTA_LIBVERSION}")
else()
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_PG_SRC_FILES})
    target_link_libraries (ctascheduler ctacommon ${PROTOBUF3_LIBRARIES} ctadisk)
    add_library (ctascheduler-objectstore SHARED ${CTA_SCHEDULER_OS_SRC_FILES})
    target_link_libraries (ctascheduler-objectstore ctacommon ctaobjectstore ${PROTOBUF3_LIBRARIES} ctadisk)
    set_property(TARGET ctascheduler-objectstore PROPERTY SOVERSION "${CTA_SOVERSION}")
    set_property(TARGET ctascheduler-objectstore PROPERTY   VERSION "${CTA_LIBVERSION}")
endif()

set_property(TARGET ctascheduler PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctascheduler PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctascheduler DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

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

target_link_libraries (ctaschedulerunittests ctascheduler)

set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
