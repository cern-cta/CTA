cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_CURRENT_SOURCE_DIR})

set (MIDDLE_TIER_LIB_SRC_FILES
  cta/AdminHost.cpp
  cta/AdminUser.cpp
  cta/ArchivalJob.cpp
  cta/ArchivalJobState.cpp
  cta/ArchiveRoute.cpp
  cta/ArchiveRouteId.cpp
  cta/DirectoryEntry.cpp
  cta/DirectoryIterator.cpp
  cta/Exception.cpp
  cta/FileAttribute.cpp
  cta/FileSystemDirectoryEntry.cpp
  cta/FileSystemNode.cpp
  cta/FileSystemStorageClass.cpp
  cta/FileSystemStorageClasses.cpp
  cta/LogicalLibrary.cpp
  cta/MiddleTierAdmin.cpp
  cta/MiddleTierUser.cpp
  cta/RetrievalJob.cpp
  cta/RetrievalJobState.cpp
  cta/SecurityIdentity.cpp
  cta/SqliteDatabase.cpp
  cta/SqliteMiddleTierAdmin.cpp
  cta/SqliteMiddleTierUser.cpp
  cta/StorageClass.cpp
  cta/Tape.cpp
  cta/TapePool.cpp
  cta/UserIdentity.cpp
  cta/Utils.cpp
  cta/Vfs.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_LIB_SRC_FILES})

target_link_libraries (ctamiddletier ${SQLITE3_LIBRARY_RELEASE})

set (MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES
  cta/SqliteMiddleTierAdminTest.cpp
  cta/SqliteMiddleTierUserTest.cpp
  cta/UtilsTest.cpp
  cta/VfsTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctamiddletierunittests ${SQLITE3_LIBRARY_RELEASE})
