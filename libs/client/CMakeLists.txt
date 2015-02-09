cmake_minimum_required (VERSION 2.6)

set (CLIENT_LIB_SRC_FILES
  ArchiveJob.cpp
  ClientAPI.cpp
  DirectoryEntry.cpp
  DirectoryIterator.cpp
  Exception.cpp
  FileAttribute.cpp
  FileSystemDirectoryEntry.cpp
  FileSystemNode.cpp
  FileSystemStorageClass.cpp
  FileSystemStorageClasses.cpp
  MigrationRoute.cpp
  MigrationRouteId.cpp
  MigrationRoutes.cpp
  MockClientAPI.cpp
  SecurityIdentity.cpp
  StorageClass.cpp
  TapePool.cpp
  UserIdentity.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})
