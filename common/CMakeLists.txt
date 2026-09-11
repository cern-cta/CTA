# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


find_package (binutils REQUIRED)
find_package (libcap REQUIRED)
find_package (libuuid REQUIRED)
find_package (json-c REQUIRED)
find_package (ZLIB REQUIRED)

add_subdirectory (exception)

include_directories (${XROOTD_INCLUDE_DIR})

set_source_files_properties(checksum/CRC.cpp PROPERTIES COMPILE_OPTIONS -O2)

set (COMMON_LIB_SRC_FILES
  CmdLineTool.cpp
  CmdLineParams.cpp
  checksum/ChecksumBlob.cpp
  checksum/CRC.cpp
  config/Config.cpp
  config/ConfigurationFile.cpp
  config/SourcedParameter.cpp
  dataStructures/CreationLog.cpp
  dataStructures/AdminUser.cpp
  dataStructures/ArchiveFile.cpp
  dataStructures/ArchiveFileQueueCriteriaAndFileId.cpp
  dataStructures/ArchiveFileSummary.cpp
  dataStructures/ArchiveJob.cpp
  dataStructures/ArchiveRequest.cpp
  dataStructures/ArchiveRoute.cpp
  dataStructures/ArchiveRouteType.cpp
  dataStructures/CancelRetrieveRequest.cpp
  dataStructures/DiskSpaceReservationRequest.cpp
  dataStructures/DeleteArchiveRequest.cpp
  dataStructures/DesiredDriveState.cpp
  dataStructures/DiskFileInfo.cpp
  dataStructures/DiskInstance.cpp
  dataStructures/DiskInstanceSpace.cpp
  dataStructures/DriveInfo.cpp
  dataStructures/DriveState.cpp
  dataStructures/DriveStatus.cpp
  dataStructures/EntryLog.cpp
  dataStructures/FrontendReturnCode.cpp
  dataStructures/JobQueueType.cpp
  dataStructures/LifecycleTimings.cpp
  dataStructures/ListStorageClassRequest.cpp
  dataStructures/LogicalLibrary.cpp
  dataStructures/MountPolicy.cpp
  dataStructures/MountType.cpp
  dataStructures/OwnerIdentity.cpp
  dataStructures/QueueAndMountSummary.cpp
  dataStructures/ReadTestResult.cpp
  dataStructures/RepackInfo.cpp
  dataStructures/RepackQueueType.cpp
  dataStructures/RequesterGroupMountRule.cpp
  dataStructures/RequesterIdentity.cpp
  dataStructures/RequesterMountRule.cpp
  dataStructures/RequesterActivityMountRule.cpp
  dataStructures/RetrieveJob.cpp
  dataStructures/RetrieveJobToAdd.cpp
  dataStructures/RetrieveRequest.cpp
  dataStructures/SecurityIdentity.cpp
  dataStructures/StorageClass.cpp
  dataStructures/Tape.cpp
  dataStructures/TapeDrive.cpp
  dataStructures/TapeFile.cpp
  dataStructures/TapeLog.cpp
  dataStructures/TestSourceType.cpp
  dataStructures/UpdateFileStorageClassRequest.cpp
  dataStructures/utils.cpp
  dataStructures/VirtualOrganization.cpp
  dataStructures/WriteTestResult.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/InvalidConfigEntry.cpp
  exception/NoPortInRange.cpp
  json/object/JSONCObject.cpp
  log/FileLogger.cpp
  log/LogContext.cpp
  log/Logger.cpp
  log/LogLevel.cpp
  log/Message.cpp
  log/Param.cpp
  log/PriorityMaps.cpp
  log/StdoutLogger.cpp
  log/StringLogger.cpp
  log/TimingList.cpp
  priorities/DriveQuota.cpp
  priorities/MountCriteria.cpp
  priorities/UserGroup.cpp
  process/ProcessCap.cpp
  process/threading/ChildProcess.cpp
  process/threading/CondVar.cpp
  process/threading/Daemon.cpp
  process/threading/Mutex.cpp
  process/threading/RWLock.cpp
  process/threading/RWLockRdLocker.cpp
  process/threading/RWLockWrLocker.cpp
  process/threading/Semaphores.cpp
  process/threading/SocketPair.cpp
  process/threading/SubProcess.cpp
  process/threading/System.cpp
  process/threading/Thread.cpp
  remoteFS/RemoteFileStatus.cpp
  remoteFS/RemotePath.cpp
  remoteFS/RemotePathAndStatus.cpp
  utils/Timer.cpp
  utils/Base64.cpp
  utils/FileUtils.cpp
  utils/GetOptThreadSafe.cpp
  utils/Regex.cpp
  utils/strerror_r_wrapper.cpp
  utils/utils.cpp
  utils/StringConversions.cpp
  utils/ErrorUtils.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})
set_property(TARGET ctacommon PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacommon PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctacommon DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacommon
  ctaversioninfo
  uuid
  cap
  json-c
  cryptopp
  ZLIB::ZLIB
)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  auth/jwt/JwkCacheTest.cpp
  auth/jwt/JwtValidationTest.cpp
  checksum/ChecksumBlobTest.cpp
  checksum/CRCTest.cpp
  config/ConfigurationFileTests.cpp
  config/SourcedParameterTests.cpp
  dataStructures/ArchiveFileTest.cpp
  dataStructures/LogicalLibraryTest.cpp
  dataStructures/StorageClassTest.cpp
  log/FileLoggerTest.cpp
  log/LogContextTest.cpp
  log/LogLevelTest.cpp
  log/ParamTest.cpp
  log/LoggerTest.cpp
  log/JsonTest.cpp
  log/StringLoggerTest.cpp
  log/LoggerParamTypeTest.cpp
  remoteFS/RemotePathTest.cpp
  process/threading/CondVarTest.cpp
  process/threading/DaemonTest.cpp
  process/threading/RWLockTest.cpp
  process/threading/SocketPairTest.cpp
  process/threading/ThreadingBlockingQTests.cpp
# process/threading/ThreadingMPTests.cpp is commented out because of errors caused by libust
  process/threading/ThreadingMTTests.cpp
  process/threading/ThreadingTests.cpp
  utils/GetOptThreadSafeTest.cpp
  utils/RegexTest.cpp
  utils/UtilsTest.cpp
  json/test/JSONCTestObject.cpp
  json/test/JSONCObjectTest.cpp
)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctacommonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacommonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")


set (AUTH_LIB_SRC_FILES
  auth/jwt/JwkCache.cpp
  auth/jwt/JwksFetcher.cpp
  auth/jwt/JwtAuthManager.cpp
)

find_package(OpenSSL REQUIRED)
add_library(ctaauth STATIC ${AUTH_LIB_SRC_FILES})
target_include_directories(ctaauth PUBLIC ${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)
target_link_libraries(ctaauth PRIVATE curl OpenSSL::Crypto)
target_link_libraries(ctacommonunittests ctaauth)

install(TARGETS ctacommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
add_executable(mutexLtrace process/threading/MutexLtrace.cpp)
target_link_libraries(mutexLtrace ctacommon)
