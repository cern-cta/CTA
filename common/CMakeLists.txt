# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package (binutils REQUIRED)
find_package (libcap REQUIRED)
find_package (libuuid REQUIRED)
find_package (json-c REQUIRED)
find_package (opentelemetry-cpp CONFIG REQUIRED)

add_subdirectory (exception)

include_directories (${XROOTD_INCLUDE_DIR})

#
# Compiled protocol buffers (used for ChecksumBlob serialization)
#
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set_source_files_properties(CRC.cpp PROPERTIES COMPILE_FLAGS -O2)

set (COMMON_LIB_SRC_FILES
  checksum/ChecksumBlob.cpp
  CmdLineParams.cpp
  CmdLineTool.cpp
  config/Config.cpp
  ConfigurationFile.cpp
  CRC.cpp
  CreationLog.cpp
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
  process/signals/SignalReactor.cpp
  process/signals/SignalReactorBuilder.cpp
  process/signals/SignalUtils.cpp
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
  telemetry/CtaTelemetryLogHandler.cpp
  telemetry/TelemetryInit.cpp
  telemetry/config/TelemetryConfig.cpp
  telemetry/metrics/MetricsUtils.cpp
  telemetry/metrics/InstrumentRegistry.cpp
  telemetry/metrics/instruments/FrontendInstruments.cpp
  telemetry/metrics/instruments/ObjectstoreInstruments.cpp
  telemetry/metrics/instruments/RdbmsInstruments.cpp
  telemetry/metrics/instruments/SchedulerInstruments.cpp
  telemetry/metrics/instruments/TapedInstruments.cpp
  telemetry/metrics/instruments/MaintdInstruments.cpp
  SourcedParameter.cpp
  Timer.cpp
  utils/Base64.cpp
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

target_include_directories(ctacommon PRIVATE ${OPENTELEMETRY_CPP_INCLUDE_DIRS})

target_link_libraries (ctacommon
  ctaprotobuf
  uuid
  cap
  json-c
  cryptopp
  ${OPENTELEMETRY_CPP_LIBRARIES}
)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  checksum/ChecksumBlobTest.cpp
  ConfigurationFileTests.cpp
  SourcedParameterTests.cpp
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
  CRCTest.cpp
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
  JwkCacheTests.cpp
)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})
set_property(TARGET ctacommonunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacommonunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

find_package(OpenSSL REQUIRED)
include_directories(${CMAKE_SOURCE_DIR}/common/jwt-cpp/include/)
add_library(JwkCache STATIC JwkCache.cpp)
target_link_libraries(JwkCache PRIVATE curl OpenSSL::Crypto)
target_link_libraries(ctacommonunittests JwkCache)

install(TARGETS ctacommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
add_executable(mutexLtrace process/threading/MutexLtrace.cpp)
target_link_libraries(mutexLtrace ctacommon)
