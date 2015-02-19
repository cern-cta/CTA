cmake_minimum_required (VERSION 2.6)

set (MIDDLE_TIER_LIB_SRC_FILES
  AdminHost.cpp
  AdminUser.cpp
  ArchiveJob.cpp
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
  MigrationRoutes.cpp
  MockMiddleTierDatabase.cpp
  MockMiddleTierAdmin.cpp
  MockMiddleTierUser.cpp
  MockMiddleTierUser.cpp
  SecurityIdentity.cpp
  StorageClass.cpp
  Tape.cpp
  TapePool.cpp
  UserIdentity.cpp
  Utils.cpp)

add_library (ctamiddletier SHARED
  ${MIDDLE_TIER_LIB_SRC_FILES})

set (MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES
  MockMiddleTierAdminTest.cpp
  MockMiddleTierUserTest.cpp
  UtilsTest.cpp)

add_library (ctamiddletierunittests SHARED
  ${MIDDLE_TIER_UNIT_TESTS_LIB_SRC_FILES})
