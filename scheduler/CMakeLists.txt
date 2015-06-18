cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (CTA_SCHEDULER_SRC_FILES
  AdminHost.cpp
  AdminUser.cpp
  ArchivalFileTransfer.cpp
  ArchivalMount.cpp
  ArchivalRoute.cpp
  ArchivalRouteId.cpp
  ArchiveRequest.cpp
  ArchiveToDirRequest.cpp
  ArchiveToFileRequest.cpp
  ArchiveToTapeCopyRequest.cpp
  ConfigurationItem.cpp
  CreationLog.cpp
  DiskException.cpp
  DriveQuota.cpp
  FileAttribute.cpp
  FileSystemDirEntry.cpp
  FileSystemNode.cpp
  FileSystemStorageClass.cpp
  FileSystemStorageClasses.cpp
  FileTransfer.cpp
  LogicalLibrary.cpp
  MockSchedulerDatabase.cpp
  MockSchedulerDatabaseFactory.cpp
  Mount.cpp
  MountCriteria.cpp
  RetrievalFileTransfer.cpp
  RetrievalMount.cpp
  RetrieveFromTapeCopyRequest.cpp
  RetrieveRequest.cpp
  RetrieveToDirRequest.cpp
  RetrieveToFileRequest.cpp
  Scheduler.cpp
  SchedulerDatabase.cpp
  SchedulerDatabaseFactory.cpp
  SecurityIdentity.cpp
  SqliteColumnNameToIndex.cpp
  SQLiteStatementDeleter.cpp
  StorageClass.cpp
  Tape.cpp
  TapeCopyLocation.cpp
  TapeException.cpp
  TapePool.cpp
  UserGroup.cpp
  UserRequest.cpp
  VO.cpp)

add_library (ctascheduler SHARED
  ${CTA_SCHEDULER_SRC_FILES})

target_link_libraries (ctascheduler ${SQLITE3_LIBRARY_RELEASE})

add_library (ctaschedulertest SHARED
  MockSchedulerDatabase.cpp
  MockSchedulerDatabaseFactory.cpp
  SchedulerDatabaseFactory.cpp
  SchedulerDatabaseTest.cpp
  SchedulerTest.cpp
  UserIdentityTest.cpp)

target_link_libraries (ctaschedulertest ${SQLITE3_LIBRARY_RELEASE})

include_directories(${CMAKE_BINARY_DIR})
add_library(ctaOStoreSchedulerDB SHARED
  OStoreDB/OStoreDB.cpp)

target_link_libraries(ctaschedulertest
  ctascheduler
  ctaOStoreSchedulerDB)
