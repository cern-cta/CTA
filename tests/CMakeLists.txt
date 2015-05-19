cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/libs/ctacommon)
include_directories(${PROJECT_SOURCE_DIR}/libs/ctaclient)

add_executable(unittests
  unit_tests.cpp)

target_link_libraries(unittests
  ctautils
  ctautilsunittests
  CTAObjectStore
  ctaobjectstoreunittests
  ctamiddletier
  ctamiddletierunittests
  ctamiddletiersqlite
  ctamiddletiersqliteunittests
  #ctamiddletierobjectstoreunittests
  ctamiddletierobjectstore
  ctanamespacevfs
  ctanamespacevfsunittests
  ctamocknameservertest
  ctamocknameserver
  ctanameserver
  gmock
  gtest
  pthread)
