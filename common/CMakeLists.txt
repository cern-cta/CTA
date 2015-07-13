cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)

set (COMMON_LIB_SRC_FILES
  ArchiveDirEntry.cpp
  ArchiveDirIterator.cpp
  ArchiveFileStatus.cpp
  ByteArray.cpp
  Checksum.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/Serrnum.cpp
  RemoteFileStatus.cpp
  RemotePath.cpp
  RemotePathAndStatus.cpp
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
  castorcommon
  pthread
  ${SQLITE3_LIBRARY_RELEASE}
  uuid)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  ByteArrayTest.cpp
  ChecksumTest.cpp
  RemotePathTest.cpp
  SmartFdTest.cpp
  UtilsTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
