cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set (COMMON_LIB_SRC_FILES
  cta/filesystem/DirectoryEntry.cpp
  cta/filesystem/DirectoryConstIterator.cpp
  cta/exception/Exception.cpp
  cta/StorageClass.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})
