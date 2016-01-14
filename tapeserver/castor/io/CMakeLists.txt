cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (ctaio
  io.cpp
  IpAndPort.cpp
  StreamAddress.cpp)

add_library (ctaiounittests SHARED
  IoTest.cpp)
target_link_libraries (ctaiounittests
  ctaio)
