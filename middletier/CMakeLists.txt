cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR} 
  ${PROJECT_SOURCE_DIR})

set (MIDDLE_TIER_LIB_SRC_FILES
  cta/AdminHost.cpp
  cta/AdminUser.cpp
  cta/ArchivalFileTransfer.cpp
  cta/ArchivalJob.cpp
  cta/ArchivalJobQueues.cpp
  cta/ArchivalJobState.cpp
  cta/ArchivalMount.cpp
  cta/ArchivalRequest.cpp
  cta/ArchivalRoute.cpp
  cta/ArchivalRouteId.cpp
  cta/ArchiveToDirRequest.cpp
  cta/ArchiveToFileRequest.cpp
  cta/ConfigurationItem.cpp
  cta/DirEntry.cpp
  cta/DirIterator.cpp
  cta/DiskException.cpp
  cta/DriveQuota.cpp
  cta/EosRemoteStorage.cpp
  cta/Exception.cpp
  cta/FileAttribute.cpp
  cta/FileSystemDirEntry.cpp
  cta/FileSystemNode.cpp
  cta/FileSystemStorageClass.cpp
  cta/FileSystemStorageClasses.cpp
  cta/FileTransfer.cpp
  cta/LogicalLibrary.cpp
  cta/MiddleTierAdmin.cpp
  cta/MiddleTierUser.cpp
  cta/MockNameServer.cpp
  cta/Mount.cpp
  cta/MountCriteria.cpp
  cta/NameServer.cpp
  cta/RemoteStorage.cpp
  cta/RetrievalFileTransfer.cpp
  cta/RetrievalJob.cpp
  cta/RetrievalJobQueues.cpp
  cta/RetrievalJobState.cpp
  cta/RetrievalMount.cpp
  cta/RetrievalRequest.cpp
  cta/RetrieveToDirRequest.cpp
  cta/RetrieveToFileRequest.cpp
  cta/SecurityIdentity.cpp
  cta/SqliteColumnNameToIndex.cpp
  cta/SqliteDatabase.cpp
  cta/SqliteMiddleTierAdmin.cpp
  cta/SqliteMiddleTierUser.cpp
  cta/StorageClass.cpp
  cta/Tape.cpp
  cta/TapeException.cpp
  cta/TapeFileLocation.cpp
  cta/TapePool.cpp
  cta/UserIdentity.cpp
  cta/UserGroup.cpp
  cta/UserRequest.cpp
  cta/Utils.cpp
  cta/Vfs.cpp
  cta/VO.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_LIB_SRC_FILES})

target_link_libraries (ctamiddletier ${SQLITE3_LIBRARY_RELEASE}
  CTAException)

set (MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES
  cta/SqliteMiddleTierAdminTest.cpp
  cta/SqliteMiddleTierUserTest.cpp
  cta/UtilsTest.cpp
  cta/MockNameServerTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctamiddletierunittests ${SQLITE3_LIBRARY_RELEASE})
