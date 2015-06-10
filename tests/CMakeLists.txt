cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/libs/ctacommon)
include_directories(${PROJECT_SOURCE_DIR}/libs/ctaclient)

add_executable(unittests
  unit_tests.cpp)

target_link_libraries(unittests
  ctacommon
  ctacommonunittests
  CTAObjectStore
  ctaobjectstoreunittests
  ctanameservertest
  ctaschedulertest
  gmock
  gtest
  pthread)
