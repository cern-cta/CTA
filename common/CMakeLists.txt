# The CERN Tape Archive(CTA) project
# Copyright(C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cmake_minimum_required (VERSION 2.6)

find_package (binutils REQUIRED)
find_package (libattr REQUIRED)
find_package (libcap REQUIRED)
find_package (libuuid REQUIRED)

set (COMMON_LIB_SRC_FILES
  dataStructures/AdminHost.cpp
  dataStructures/AdminUser.cpp
  dataStructures/ArchiveFile.cpp
  dataStructures/ArchiveFileQueueCriteria.cpp
  dataStructures/ArchiveFileSummary.cpp
  dataStructures/ArchiveJob.cpp
  dataStructures/ArchiveRequest.cpp
  dataStructures/ArchiveRoute.cpp
  dataStructures/CancelRetrieveRequest.cpp
  dataStructures/DeleteArchiveRequest.cpp
  dataStructures/DiskFileInfo.cpp
  dataStructures/DriveState.cpp
  dataStructures/DriveStatus.cpp
  dataStructures/EntryLog.cpp
  dataStructures/FrontendReturnCode.cpp
  dataStructures/ListStorageClassRequest.cpp
  dataStructures/LogicalLibrary.cpp
  dataStructures/MountType.cpp
  dataStructures/MountPolicy.cpp
  dataStructures/ReadTestResult.cpp
  dataStructures/RepackInfo.cpp
  dataStructures/RepackType.cpp
  dataStructures/RequesterGroupMountRule.cpp
  dataStructures/RequesterMountRule.cpp
  dataStructures/RetrieveJob.cpp
  dataStructures/RetrieveRequest.cpp
  dataStructures/SecurityIdentity.cpp
  dataStructures/StorageClass.cpp
  dataStructures/Tape.cpp
  dataStructures/TapeFile.cpp
  dataStructures/TapeLog.cpp
  dataStructures/TapePool.cpp
  dataStructures/TestSourceType.cpp
  dataStructures/UpdateFileInfoRequest.cpp
  dataStructures/UpdateFileStorageClassRequest.cpp
  dataStructures/UserIdentity.cpp
  dataStructures/VerifyInfo.cpp
  dataStructures/WriteTestResult.cpp
  dataStructures/utils.cpp
  admin/AdminHost.cpp
  admin/AdminUser.cpp
  archiveRoutes/ArchiveRoute.cpp
  checksum/Checksum.cpp
  exception/Backtrace.cpp
  exception/Errnum.cpp
  exception/Exception.cpp
  exception/NonRetryableError.cpp
  exception/RetryableError.cpp
  exception/UserError.cpp
  exception/AcceptConnectionInterrupted.cpp
  exception/BadAlloc.cpp
  exception/CommandLineNotParsed.cpp
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
  threading/SocketPair.cpp
  threading/System.cpp
  threading/Threading.cpp
  utils/utils.cpp
  utils/strerror_r_wrapper.cpp
  CreationLog.cpp
  Configuration.cpp
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
  uuid
  z
  Utils
  cap)

set (COMMON_UNIT_TESTS_LIB_SRC_FILES
  checksum/ChecksumTest.cpp
  dataStructures/ArchiveFileTest.cpp
  dataStructures/StorageClassTest.cpp
  log/LogContextTest.cpp
  log/ParamTest.cpp
  log/SyslogLoggerTest.cpp
  log/StringLoggerTest.cpp
  remoteFS/RemotePathTest.cpp
  threading/DaemonTest.cpp
  threading/SocketPairTest.cpp
  utils/UtilsTest.cpp
  UserIdentityTest.cpp
  optionalTest.cpp)

add_library (ctacommonunittests SHARED
  ${COMMON_UNIT_TESTS_LIB_SRC_FILES})

install(TARGETS ctacommonunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
