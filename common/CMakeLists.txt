cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)

set (COMMON_LIB_SRC_FILES
  dataStructures/AdminHost.cpp
  dataStructures/AdminUser.cpp
  dataStructures/ArchiveFile.cpp
  dataStructures/ArchiveJob.cpp
  dataStructures/ArchiveMount.cpp
  dataStructures/ArchiveRequest.cpp
  dataStructures/ArchiveRoute.cpp
  dataStructures/CancelRetrieveRequest.cpp
  dataStructures/Dedication.cpp
  dataStructures/DeleteArchiveRequest.cpp
  dataStructures/DRData.cpp
  dataStructures/DriveState.cpp
  dataStructures/EntryLog.cpp
  dataStructures/ListStorageClassRequest.cpp
  dataStructures/ReadTestResult.cpp
  dataStructures/RepackInfo.cpp
  dataStructures/Requester.cpp
  dataStructures/RetrieveJob.cpp
  dataStructures/RetrieveMount.cpp
  dataStructures/RetrieveRequest.cpp
  dataStructures/SecurityIdentity.cpp
  dataStructures/StorageClass.cpp
  dataStructures/Tape.cpp
  dataStructures/TapeFileLocation.cpp
  dataStructures/TapeMount.cpp
  dataStructures/TapePool.cpp
  dataStructures/UpdateFileInfoRequest.cpp
  dataStructures/User.cpp
  dataStructures/UserGroup.cpp
  dataStructures/UserIdentity.cpp
  dataStructures/VerifyInfo.cpp
  dataStructures/WriteTestResult.cpp
  admin/AdminHost.cpp
  admin/AdminUser.cpp
  archiveRoutes/ArchiveRoute.cpp
  archiveNS/ArchiveDirEntry.cpp
  archiveNS/ArchiveDirIterator.cpp
  archiveNS/ArchiveFile.cpp
  archiveNS/ArchiveFileStatus.cpp
  archiveNS/StorageClass.cpp
  archiveNS/Tape.cpp
  archiveNS/TapeFileLocation.cpp
  ArchiveRequest.cpp
  CreationLog.cpp
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
install (TARGETS ctacommon DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacommon
  ctautils
  pthread
  ${SQLITE3_LIBRARY_RELEASE}
  uuid
  z
  Utils)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  checksum/ChecksumTest.cpp
  remoteFS/RemotePathTest.cpp
  UserIdentityTest.cpp
  UtilsTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
