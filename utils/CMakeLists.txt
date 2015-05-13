cmake_minimum_required (VERSION 2.6)

set (UTILS_LIB_SRC_FILES
  Timer.cpp
  Utils.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/strerror_r_wrapper.cpp
  threading/ChildProcess.cpp
  threading/Mutex.cpp
  threading/Threading.cpp)

add_library (ctautils SHARED
  ${UTILS_LIB_SRC_FILES})

target_link_libraries (ctautils ${SQLITE3_LIBRARY_RELEASE}
  pthread)

set (UTILS_UNIT_TESTS_LIB_SRC_FILES
  UtilsTest.cpp)

add_library (ctautilsunittests SHARED
  ${UTILS_UNIT_TESTS_LIB_SRC_FILES})

