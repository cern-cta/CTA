cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${CMAKE_BINARY_DIR})

add_library (CTAObjectStoreMiddleTier
  ObjectStoreMiddleTier.cpp
)