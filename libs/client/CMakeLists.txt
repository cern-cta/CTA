cmake_minimum_required (VERSION 2.6)

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

set (CLIENT_LIB_SRC_FILES
  cta/client/API.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})

target_link_libraries(ctaclient ctacommon)
