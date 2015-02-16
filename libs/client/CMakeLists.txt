cmake_minimum_required (VERSION 2.6)

set (CLIENT_LIB_SRC_FILES
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
  TapePool.cpp
  UserIdentity.cpp
  Utils.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})
