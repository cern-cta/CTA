cmake_minimum_required (VERSION 2.6)

set (CLIENT_LIB_SRC_FILES
  API.cpp)

add_library (ctaclient SHARED
  ${CLIENT_LIB_SRC_FILES})

target_link_libraries(ctaclient ctacommon)
