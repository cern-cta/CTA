# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

include_directories (${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR} ${CMAKE_BINARY_DIR} ${XROOTD_INCLUDE_DIR})

add_subdirectory(rdbms/schema)

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  LabelMount.cpp
  MountType.cpp
  MountType.cpp
  PositioningMethod.cpp
  RepackRequest.cpp
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
  rdbms/TemporaryPostgresInstance.cpp
  rdbms/postgres/ArchiveJobQueue.cpp
  rdbms/postgres/RetrieveJobQueue.cpp
  rdbms/postgres/RepackRequestTracker.cpp
  rdbms/postgres/Transaction.cpp)

# End of definitions specific to Postgres scheduler lib compilation

if(NOT CTA_USE_PGSCHED)
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_OS_SRC_FILES})
    target_link_libraries (ctascheduler ctacommon ctaobjectstore ctadisk)
else()
    add_library (ctascheduler SHARED ${CTA_SCHEDULER_PG_SRC_FILES})
    target_link_libraries (ctascheduler ctaschedulerschema ctacommon ctadisk cryptopp)
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
    RelationalDBSchedulerTest.cpp
    rdbms/RelationalDBTest.cpp)
endif()

target_link_libraries (ctaschedulerunittests ctascheduler)

set_property(TARGET ctaschedulerunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctaschedulerunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctaschedulerunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
