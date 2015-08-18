cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (castorlegacymsgunittests SHARED
  CommonMarshalTest.cpp
  GenericMarshalTest.cpp
  TapeMarshalTest.cpp)
target_link_libraries (castorlegacymsgunittests
  castorclient
  castorlegacymsg
  castormessages)
