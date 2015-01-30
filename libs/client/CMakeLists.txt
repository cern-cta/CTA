cmake_minimum_required (VERSION 2.6)

set (CLIENT_LIB_SRC_FILES
  ClientAPI.cpp
  DirectoryEntry.cpp
  DirectoryIterator.cpp
  Exception.cpp
  FileAttribute.cpp
  MockAPITest.cpp
  MockClientAPI.cpp
  StorageClass.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})
