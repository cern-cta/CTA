cmake_minimum_required (VERSION 2.6)

set (MIDDLE_TIER_LIB_SRC_FILES
  AdminHost.cpp
  AdminUser.cpp
  ArchivalJob.cpp
  ArchivalJobState.cpp
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
  MigrationRoute.cpp
  MigrationRouteId.cpp
  MockAdminHostTable.cpp
  MockAdminUserTable.cpp
  MockDatabase.cpp
  MockLogicalLibraryTable.cpp
  MockMiddleTierAdmin.cpp
  MockMiddleTierUser.cpp
  MockMiddleTierUser.cpp
  MockMigrationRouteTable.cpp
  MockTapeTable.cpp
  MockTapePoolTable.cpp
  RetrievalJob.cpp
  RetrievalJobState.cpp
  SecurityIdentity.cpp
  StorageClass.cpp
  Tape.cpp
  TapePool.cpp
  UserIdentity.cpp
  Utils.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_LIB_SRC_FILES})

set (MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES
  MockAdminHostTableTest.cpp
  MockAdminUserTableTest.cpp
  MockLogicalLibraryTableTest.cpp
  MockMiddleTierAdminTest.cpp
  MockMiddleTierUserTest.cpp
  MockTapeTableTest.cpp
  MockTapePoolTableTest.cpp
  UtilsTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES})
