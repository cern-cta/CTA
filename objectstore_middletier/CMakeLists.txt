cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/..)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../middletier)
include_directories(${CMAKE_BINARY_DIR})

add_library (CTAObjectStoreMiddleTier
  ObjectStoreMiddleTierAdmin.cpp
)

set(MiddleTierUnitTests
  MiddleTierAdminAbstractTest.cpp
  MiddleTierTest.cpp
)

add_executable(ObjectStoreMiddleTierUnitTests unit_tests.cpp ${MiddleTierUnitTests})
target_link_libraries(ObjectStoreMiddleTierUnitTests
  CTAObjectStoreMiddleTier
  protobuf rados CTAObjectStore gtest gmock ctamiddletier)
