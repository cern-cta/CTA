cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (MIDDLE_TIER_INTERFACE_SRC_FILES
  interface/AdminHost.cpp
  interface/AdminUser.cpp
  interface/ArchivalFileTransfer.cpp
  interface/ArchivalJob.cpp
  interface/ArchivalJobQueues.cpp
  interface/ArchivalJobState.cpp
  interface/ArchivalMount.cpp
  interface/ArchivalRequest.cpp
  interface/ArchivalRoute.cpp
  interface/ArchivalRouteId.cpp
  interface/ArchiveToDirRequest.cpp
  interface/ArchiveToFileRequest.cpp
  interface/ConfigurationItem.cpp
  interface/DirEntry.cpp
  interface/DirIterator.cpp
  interface/DiskException.cpp
  interface/DriveQuota.cpp
  interface/EosRemoteStorage.cpp
  interface/FileAttribute.cpp
  interface/FileSystemDirEntry.cpp
  interface/FileSystemNode.cpp
  interface/FileSystemStorageClass.cpp
  interface/FileSystemStorageClasses.cpp
  interface/FileTransfer.cpp
  interface/LogicalLibrary.cpp
  interface/MiddleTierAdmin.cpp
  interface/MiddleTierUser.cpp
  interface/MockNameServer.cpp
  interface/Mount.cpp
  interface/MountCriteria.cpp
  interface/NameServer.cpp
  interface/RemoteStorage.cpp
  interface/RetrievalFileTransfer.cpp
  interface/RetrievalJob.cpp
  interface/RetrievalJobQueues.cpp
  interface/RetrievalJobState.cpp
  interface/RetrievalMount.cpp
  interface/RetrievalRequest.cpp
  interface/RetrieveToDirRequest.cpp
  interface/RetrieveToFileRequest.cpp
  interface/SecurityIdentity.cpp
  interface/StorageClass.cpp
  interface/Tape.cpp
  interface/TapeException.cpp
  interface/TapeFileLocation.cpp
  interface/TapePool.cpp
  interface/UserIdentity.cpp
  interface/UserGroup.cpp
  interface/UserRequest.cpp
  interface/VO.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_INTERFACE_SRC_FILES})

set (MIDDLE_TIER_INTERFACE_UNIT_TESTS_LIB_SRC_FILES
  interface/MockNameServerTest.cpp
  sharedtest/MiddleTierAdminAbstractTest.cpp
  sharedtest/MiddleTierUserAbstractTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_INTERFACE_UNIT_TESTS_LIB_SRC_FILES})

add_library (ctamiddletiersqlite SHARED
  SQLite/SqliteColumnNameToIndex.cpp
  SQLite/SqliteDatabase.cpp
  SQLite/SqliteMiddleTierAdmin.cpp
  SQLite/SqliteMiddleTierUser.cpp)

target_link_libraries (ctamiddletiersqlite ${SQLITE3_LIBRARY_RELEASE})

add_library (ctamiddletiersqliteunittests SHARED
  SQLite/SqliteMiddleTierTest.cpp)

include_directories(${CMAKE_BINARY_DIR})

add_library(ctamiddletiersqliteobjectstore
  objectstore/ObjectStoreMiddleTierAdmin.cpp)

#add_library (ctamiddletiersqliteobjectstoreunittests SHARED
#  objectstore/ObjectStoreMiddleTierTest.cpp)
