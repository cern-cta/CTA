cmake_minimum_required (VERSION 2.6)

find_package (sqlite REQUIRED)
include_directories (${SQLITE3_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

set (MIDDLE_TIER_LIB_SRC_FILES
  AdminHost.cpp
  AdminUser.cpp
  ArchivalJob.cpp
  ArchivalJobState.cpp
  ArchiveRoute.cpp
  ArchiveRouteId.cpp
  DirectoryEntry.cpp
  DirectoryIterator.cpp
  Exception.cpp
  FileAttribute.cpp
  FileSystemDirectoryEntry.cpp
  FileSystemNode.cpp
  FileSystemStorageClass.cpp
  FileSystemStorageClasses.cpp
  LogicalLibrary.cpp
  MiddleTierAdmin.cpp
  MiddleTierUser.cpp
  MockArchivalJobTable.cpp
  MockArchiveRouteTable.cpp
  MockAdminHostTable.cpp
  MockAdminUserTable.cpp
  MockDatabase.cpp
  MockLogicalLibraryTable.cpp
  MockMiddleTierAdmin.cpp
  MockMiddleTierUser.cpp
  MockMiddleTierUser.cpp
  MockRetrievalJobTable.cpp
  MockTapeTable.cpp
  MockTapePoolTable.cpp
  RetrievalJob.cpp
  RetrievalJobState.cpp
  SecurityIdentity.cpp
  SqliteDatabase.cpp
  SqliteMiddleTierAdmin.cpp
  SqliteMiddleTierUser.cpp
  StorageClass.cpp
  Tape.cpp
  TapePool.cpp
  UserIdentity.cpp
  Utils.cpp
  Vfs.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_LIB_SRC_FILES})

target_link_libraries (ctamiddletier ${SQLITE3_LIBRARY_RELEASE})

set (MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES
  MockAdminHostTableTest.cpp
  MockAdminUserTableTest.cpp
  MockArchivalJobTableTest.cpp
  MockLogicalLibraryTableTest.cpp
  MockMiddleTierAdminTest.cpp
  MockMiddleTierUserTest.cpp
  MockTapeTableTest.cpp
  MockTapePoolTableTest.cpp
  SqliteMiddleTierAdminTest.cpp
  SqliteMiddleTierUserTest.cpp
  UtilsTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES})

target_link_libraries (ctamiddletierunittests ${SQLITE3_LIBRARY_RELEASE})
