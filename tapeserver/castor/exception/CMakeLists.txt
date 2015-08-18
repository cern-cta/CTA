cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)

add_library (castorexceptionunittests SHARED
  ExceptionTest.cpp)
