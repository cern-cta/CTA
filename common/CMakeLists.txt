cmake_minimum_required (VERSION 2.6)

set (COMMON_LIB_SRC_FILES
  ArchiveFileStatus.cpp
  DirEntry.cpp
  DirIterator.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  RemotePath.cpp
  SmartFd.cpp
  strerror_r_wrapper.cpp
  Timer.cpp
  threading/ChildProcess.cpp
  threading/Mutex.cpp
  threading/Threading.cpp
  UserIdentity.cpp
  Utils.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})

target_link_libraries (ctacommon
  ${SQLITE3_LIBRARY_RELEASE}
  pthread
  uuid)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  RemotePathTest.cpp
  SmartFdTest.cpp
  UtilsTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
