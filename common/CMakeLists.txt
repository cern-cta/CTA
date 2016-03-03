cmake_minimum_required (VERSION 2.6)

set (COMMON_LIB_SRC_FILES
  dataStructures/AdminHost.cpp
  dataStructures/AdminUser.cpp
  dataStructures/ArchiveFile.cpp
  dataStructures/ArchiveFileSummary.cpp
  dataStructures/ArchiveJob.cpp
  dataStructures/ArchiveRequest.cpp
  dataStructures/ArchiveRoute.cpp
  dataStructures/CancelRetrieveRequest.cpp
  dataStructures/Dedication.cpp
  dataStructures/DedicationType.cpp
  dataStructures/DeleteArchiveRequest.cpp
  dataStructures/DRData.cpp
  dataStructures/DriveState.cpp
  dataStructures/DriveStatus.cpp
  dataStructures/EntryLog.cpp
  dataStructures/ListStorageClassRequest.cpp
  dataStructures/MountGroup.cpp
  dataStructures/MountType.cpp
  dataStructures/MountPolicy.cpp
  dataStructures/ReadTestResult.cpp
  dataStructures/RepackInfo.cpp
  dataStructures/RepackType.cpp
  dataStructures/Requester.cpp
  dataStructures/RetrieveJob.cpp
  dataStructures/RetrieveRequest.cpp
  dataStructures/SecurityIdentity.cpp
  dataStructures/StorageClass.cpp
  dataStructures/Tape.cpp
  dataStructures/TapeFileLocation.cpp
  dataStructures/TapePool.cpp
  dataStructures/TestSourceType.cpp
  dataStructures/UpdateFileInfoRequest.cpp
  dataStructures/User.cpp
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
  checksum/Checksum.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  log/DummyLogger.cpp
  log/LogContext.cpp
  log/Logger.cpp
  log/Message.cpp
  log/Param.cpp
  log/StringLogger.cpp
  log/SyslogLogger.cpp
  log/StdoutLogger.cpp
  priorities/DriveQuota.cpp
  priorities/MountCriteria.cpp
  priorities/UserGroup.cpp
  processCap/ProcessCap.cpp
  processCap/SmartCap.cpp
  remoteFS/RemoteFileStatus.cpp
  remoteFS/RemotePath.cpp
  remoteFS/RemotePathAndStatus.cpp
  threading/ChildProcess.cpp
  threading/Daemon.cpp
  threading/Mutex.cpp
  threading/System.cpp
  threading/Threading.cpp
  utils/utils.cpp
  utils/strerror_r_wrapper.cpp
  CreationLog.cpp
  Configuration.cpp
  SecurityIdentity.cpp
  TapePool.cpp
  Timer.cpp
  UserIdentity.cpp
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
  Utils
  cap)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  checksum/ChecksumTest.cpp
  log/LogContextTest.cpp
  log/ParamTest.cpp
  log/SyslogLoggerTest.cpp
  log/StringLoggerTest.cpp
  remoteFS/RemotePathTest.cpp
  threading/DaemonTest.cpp
  utils/UtilsTest.cpp
  UserIdentityTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})

install(TARGETS ctacommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
