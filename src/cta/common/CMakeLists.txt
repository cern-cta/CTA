cmake_minimum_required (VERSION 2.6)

set (COMMON_LIB_SRC_FILES
  DirectoryEntry.cpp
  DirectoryConstIterator.cpp
  Exception.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})
