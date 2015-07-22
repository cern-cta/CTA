cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)

set (COMMON_LIB_SRC_FILES
  admin/AdminHost.cpp
  admin/AdminUser.cpp
  archiveRoutes/ArchiveRoute.cpp
  archiveRoutes/ArchiveRouteId.cpp
  archiveNS/ArchiveDirEntry.cpp
  archiveNS/ArchiveDirIterator.cpp
  archiveNS/ArchiveFileStatus.cpp
  archiveNS/StorageClass.cpp
  archiveNS/Tape.cpp
  archiveNS/TapeCopyLocation.cpp
  archiveNS/TapeCopyLocationAndStatus.cpp
  CreationLog.cpp
  checksum/ByteArray.cpp
  checksum/Checksum.cpp
  exception/Backtrace.cpp
  exception/DiskException.hpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/Serrnum.cpp
  exception/TapeException.cpp
  priorities/DriveQuota.cpp
  priorities/MountCriteria.cpp
  priorities/UserGroup.cpp
  remoteFS/RemoteFileStatus.cpp
  remoteFS/RemotePath.cpp
  remoteFS/RemotePathAndStatus.cpp
  SecurityIdentity.cpp
  strerror_r_wrapper.cpp
  TapePool.cpp
  Timer.cpp
  threading/ChildProcess.cpp
  threading/Mutex.cpp
  threading/Threading.cpp
  UserIdentity.cpp
  Utils.cpp
  VO.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})

target_link_libraries (ctacommon
  castorcommon
  pthread
  ${SQLITE3_LIBRARY_RELEASE}
  uuid)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  checksum/ByteArrayTest.cpp
  checksum/ChecksumTest.cpp
  remoteFS/RemotePathTest.cpp
  UserIdentityTest.cpp
  UtilsTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
