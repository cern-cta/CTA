cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(unittests
  unit_tests.cpp)

target_link_libraries(unittests
  gmock
  gtest
  pthread)
