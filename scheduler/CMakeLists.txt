cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (CTA_SCHEDULER_SRC_FILES
  ArchiveJob.cpp
  ArchiveMount.cpp
  ArchiveRequest.cpp
  ArchiveToDirRequest.cpp
  ArchiveToFileRequest.cpp
  ArchiveToTapeCopyRequest.cpp
  TapeMount.cpp
  LogicalLibrary.cpp
  mockDB/MockSchedulerDatabase.cpp
  mockDB/MockSchedulerDatabaseFactory.cpp
  RetrieveJob.cpp
  RetrieveFromTapeCopyRequest.cpp
  RetrieveMount.cpp
  RetrieveRequest.cpp
  RetrieveToDirRequest.cpp
  RetrieveToFileRequest.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SchedulerDatabaseFactory.cpp
  mockDB/SqliteColumnNameToIndex.cpp
  mockDB/SQLiteStatementDeleter.cpp
  MountType.cpp
  UserRequest.cpp)

add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})

target_link_libraries (ctascheduler ${SQLITE3_LIBRARY_RELEASE} ctacommon)

include_directories(${CMAKE_BINARY_DIR})
add_library(ctaOStoreSchedulerDB SHARED
  OStoreDB/OStoreDB.cpp)

target_link_libraries(ctaOStoreSchedulerDB
  CTAObjectStore)

add_library (ctaschedulerdummies SHARED
  DummyScheduler.cpp)

add_library (ctaschedulerunittests SHARED
  mockDB/MockSchedulerDatabase.cpp
  mockDB/MockSchedulerDatabaseFactory.cpp
  SchedulerDatabaseFactory.cpp
  SchedulerDatabaseTest.cpp
  SchedulerTest.cpp)

target_link_libraries (ctaschedulerunittests ${SQLITE3_LIBRARY_RELEASE})

target_link_libraries(ctaschedulerunittests
  ctascheduler
  ctaOStoreSchedulerDB)
