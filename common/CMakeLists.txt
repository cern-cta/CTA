# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

cmake_minimum_required (VERSION 3.17)

find_package (binutils REQUIRED)
find_package (libattr REQUIRED)
find_package (libcap REQUIRED)
find_package (libuuid REQUIRED)
find_package(json-c REQUIRED)

add_subdirectory (exception)

include_directories (${XROOTD_INCLUDE_DIR})

#
# Compiled protocol buffers (used for ChecksumBlob serialization)
#
include_directories(${CMAKE_BINARY_DIR}/eos_cta ${PROTOBUF3_INCLUDE_DIRS})

set_source_files_properties(CRC.cpp PROPERTIES COMPILE_FLAGS -O2)

set (COMMON_LIB_SRC_FILES
  checksum/ChecksumBlob.cpp
  CmdLineTool.cpp
  Configuration.cpp
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
  exception/AcceptConnectionInterrupted.cpp
  exception/Backtrace.cpp
  exception/CommandLineNotParsed.cpp
  exception/DismountFailed.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/GrpcError.cpp
  exception/InvalidConfigEntry.cpp
  exception/Mismatch.cpp
  exception/MissingOperand.cpp
  exception/NoEntry.cpp
  exception/NonRetryableError.cpp
  exception/NoPortInRange.cpp
  exception/NoSupportedDB.cpp
  exception/OutOfMemory.cpp
  exception/RequestFailed.cpp
  exception/RetryableError.cpp
  exception/TimeOut.cpp
  exception/UserError.cpp
  exception/XrootCl.cpp
  exception/EncryptionException.cpp
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
  processCap/ProcessCap.cpp
  remoteFS/RemoteFileStatus.cpp
  remoteFS/RemotePath.cpp
  remoteFS/RemotePathAndStatus.cpp
  SourcedParameter.cpp
  threading/Async.cpp
  threading/ChildProcess.cpp
  threading/CondVar.cpp
  threading/Daemon.cpp
  threading/Mutex.cpp
  threading/RWLock.cpp
  threading/RWLockRdLocker.cpp
  threading/RWLockWrLocker.cpp
  threading/Semaphores.cpp
  threading/SocketPair.cpp
  threading/SubProcess.cpp
  threading/System.cpp
  threading/Thread.cpp
  Timer.cpp
  utils/GetOptThreadSafe.cpp
  utils/Regex.cpp
  utils/strerror_r_wrapper.cpp
  utils/utils.cpp)

add_library (ctacommon SHARED
  ${COMMON_LIB_SRC_FILES})
set_property(TARGET ctacommon PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctacommon PROPERTY   VERSION "${CTA_LIBVERSION}")

install (TARGETS ctacommon DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

target_link_libraries (ctacommon
  ProtoCtaFrontend
  pthread
  uuid
  z
  cap
  XrdCl
  json-c
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
  log/JsonTest.cpp
  log/StringLoggerTest.cpp
  log/LoggerParamTypeTest.cpp
  remoteFS/RemotePathTest.cpp
  CRCTest.cpp
  threading/CondVarTest.cpp
  threading/DaemonTest.cpp
  threading/RWLockTest.cpp
  threading/SocketPairTest.cpp
  threading/ThreadingBlockingQTests.cpp
# threading/ThreadingMPTests.cpp is commented out because of errors caused by libust
  threading/ThreadingMTTests.cpp
  threading/ThreadingTests.cpp
  threading/AsyncTests.cpp
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

install(TARGETS ctacommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
add_executable(mutexLtrace threading/MutexLtrace.cpp)
target_link_libraries(mutexLtrace ctacommon)
