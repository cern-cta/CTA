cmake_minimum_required (VERSION 2.6)

include_directories(${PROJECT_SOURCE_DIR}/tapeserver)
include_directories(${PROJECT_SOURCE_DIR}/tapeserver/h)

add_library (castorlogdummies SHARED
  DummyLogger.cpp)

set (LOG_LIB_SRC_FILES
  DummyLogger.cpp
  Logger.cpp
  StringLogger.cpp
  SyslogLogger.cpp
  LogContext.cpp
  Message.cpp
  Param.cpp)
add_library (castorlog ${LOG_LIB_SRC_FILES})

add_library (castorlogunittests SHARED
  ParamTest.cpp
  LogContextTest.cpp
  StringLoggerTest.cpp
  SyslogLoggerTest.cpp)
