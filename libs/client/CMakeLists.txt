cmake_minimum_required (VERSION 2.6)

set (CLIENT_LIB_SRC_FILES
  API.cpp
  DirectoryEntry.cpp
  DirectoryIterator.cpp
  Exception.cpp
  MockAPI.cpp
  MockAPITest.cpp
  StorageClass.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})
