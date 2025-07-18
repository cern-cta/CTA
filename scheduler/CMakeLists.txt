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

add_subdirectory(rdbms/schema)

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

# Definitions specific to only Objectstore module lib compilation:

set (CTA_SCHEDULER_OS_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
  OStoreDB/MemQueues.cpp
  OStoreDB/OStoreDB.cpp
  OStoreDB/OStoreDBWithAgent.cpp
  OStoreDB/QueueItor.cpp)

# End of definitions specific to Objectstore lib compilation

# Definitions specific to only PostgreSched DB module lib compilation:

include_directories (${CMAKE_BINARY_DIR}/scheduler)

set (CTA_SCHEDULER_PG_SRC_FILES ${CTA_SCHEDULER_SRC_FILES}
  rdbms/RelationalDB.cpp
  rdbms/ArchiveMount.cpp
  rdbms/ArchiveRdbJob.cpp
  rdbms/ArchiveJobQueueItor.cpp
  rdbms/ArchiveRequest.cpp
  rdbms/Helpers.cpp
  rdbms/RetrieveMount.cpp
  rdbms/RetrieveRequest.cpp
  rdbms/RetrieveRdbJob.cpp
  rdbms/RetrieveJobQueueItor.cpp
  rdbms/RepackRequestPromotionStatistics.cpp
  rdbms/RepackRequest.cpp
  rdbms/RepackReportBatch.cpp
  rdbms/TapeMountDecisionInfo.cpp
  rdbms/postgres/ArchiveJobQueue.cpp
  rdbms/postgres/RetrieveJobQueue.cpp
  rdbms/postgres/RepackJobQueue.cpp
  rdbms/postgres/Transaction.cpp)

# End of definitions specific to Postgres scheduler lib compilation

if(NOT CTA_USE_PGSCHED)
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_OS_SRC_FILES})
    target_link_libraries (ctascheduler ctacommon ctaobjectstore ctadisk)
    add_library (ctascheduler-postgres SHARED ${CTA_SCHEDULER_PG_SRC_FILES})
    target_link_libraries (ctascheduler-postgres ctacommon ctadisk)
    set_property(TARGET ctascheduler-postgres PROPERTY SOVERSION "${CTA_SOVERSION}")
    set_property(TARGET ctascheduler-postgres PROPERTY   VERSION "${CTA_LIBVERSION}")
else()
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_PG_SRC_FILES})
    target_link_libraries (ctascheduler ctacommon ctadisk cryptopp)
    add_library (ctascheduler-objectstore SHARED ${CTA_SCHEDULER_OS_SRC_FILES})
    target_link_libraries (ctascheduler-objectstore ctacommon ctadisk)
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
    rdbms/RelationalDBTest.cpp)
endif()

target_link_libraries (ctaschedulerunittests ctascheduler)

set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
