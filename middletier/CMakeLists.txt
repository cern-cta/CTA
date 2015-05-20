cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (CTA_COMMON_SRC_FILES
  common/AdminHost.cpp
  common/AdminUser.cpp
  common/ArchivalFileTransfer.cpp
  common/ArchivalJob.cpp
  common/ArchivalJobQueues.cpp
  common/ArchivalJobState.cpp
  common/ArchivalMount.cpp
  common/ArchivalRequest.cpp
  common/ArchivalRoute.cpp
  common/ArchivalRouteId.cpp
  common/ArchiveToDirRequest.cpp
  common/ArchiveToFileRequest.cpp
  common/ConfigurationItem.cpp
  common/DirEntry.cpp
  common/DirIterator.cpp
  common/DiskException.cpp
  common/DriveQuota.cpp
  common/EosRemoteStorage.cpp
  common/FileAttribute.cpp
  common/FileSystemDirEntry.cpp
  common/FileSystemNode.cpp
  common/FileSystemStorageClass.cpp
  common/FileSystemStorageClasses.cpp
  common/FileTransfer.cpp
  common/LogicalLibrary.cpp
  common/MiddleTierAdmin.cpp
  common/MiddleTierUser.cpp
  common/Mount.cpp
  common/MountCriteria.cpp
  common/RemoteStorage.cpp
  common/RetrievalFileTransfer.cpp
  common/RetrievalJob.cpp
  common/RetrievalJobQueues.cpp
  common/RetrievalJobState.cpp
  common/RetrievalMount.cpp
  common/RetrievalRequest.cpp
  common/RetrieveToDirRequest.cpp
  common/RetrieveToFileRequest.cpp
  common/SecurityIdentity.cpp
  common/StorageClass.cpp
  common/Tape.cpp
  common/TapeException.cpp
  common/TapeFileLocation.cpp
  common/TapePool.cpp
  common/UserIdentity.cpp
  common/UserGroup.cpp
  common/UserRequest.cpp
  common/VO.cpp)

add_library (ctacommon SHARED
  ${CTA_COMMON_SRC_FILES})

set (MIDDLE_TIER_COMMON_UNIT_TESTS_LIB_SRC_FILES
  sharedtest/MiddleTierAdminAbstractTest.cpp
  sharedtest/MiddleTierUserAbstractTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_COMMON_UNIT_TESTS_LIB_SRC_FILES})

add_library (ctamiddletiersqlite SHARED
  SQLite/SqliteColumnNameToIndex.cpp
  SQLite/SqliteDatabase.cpp
  SQLite/SqliteMiddleTierAdmin.cpp
  SQLite/SqliteMiddleTierUser.cpp)

target_link_libraries (ctamiddletiersqlite ${SQLITE3_LIBRARY_RELEASE})

add_library (ctamiddletiersqliteunittests SHARED
  SQLite/SqliteMiddleTierTest.cpp)

include_directories(${CMAKE_BINARY_DIR})

add_library(ctamiddletierobjectstore
  objectstore/ObjectStoreMiddleTierAdmin.cpp
  objectstore/ObjectStoreMiddleTierUser.cpp)

add_library (ctamiddletierobjectstoreunittests SHARED
  objectstore/ObjectStoreMiddleTierTest.cpp)
