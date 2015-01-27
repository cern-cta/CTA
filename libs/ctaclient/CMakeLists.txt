cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})
include_directories(${PROJECT_SOURCE_DIR}/libs/ctacommon)

set (CLIENT_LIB_SRC_FILES
  cta/client/API.cpp
  cta/client/MockAPI.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})

target_link_libraries(ctaclient ctacommon)
