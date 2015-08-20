# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# @author Castor Dev team, castor-dev@cern.ch
#
cmake_minimum_required (VERSION 2.6)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver/h)
include_directories(${PROJECT_BINARY_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${PROJECT_BINARY_DIR})

find_package( ZLIB REQUIRED )

add_library(castorTapeServerDaemon
  AdminAcceptHandler.cpp
  AdminConnectionHandler.cpp
  Catalogue.cpp
  CatalogueDrive.cpp
  CatalogueDriveState.cpp
  CatalogueSession.cpp
  CatalogueLabelSession.cpp
  CatalogueTransferSession.cpp
  CatalogueCleanerSession.cpp
  CatalogueConfig.cpp
  CleanerSession.cpp
  DataTransferConfig.cpp
  DiskReadThreadPool.cpp
  DiskReadTask.cpp
  DiskWriteTask.cpp
  DiskWriteThreadPool.cpp
  DriveConfig.cpp
  DriveConfigMap.cpp
  EmptyDriveProbe.cpp
  TapeServerReporter.cpp
  LabelCmdAcceptHandler.cpp
  LabelCmdConnectionHandler.cpp
  LabelSession.cpp
  MigrationMemoryManager.cpp
  MigrationReportPacker.cpp 
  MigrationTaskInjector.cpp
  DataTransferSession.cpp
  ProcessForker.cpp
  ProcessForkerConnectionHandler.cpp
  ProcessForkerProxy.cpp
  ProcessForkerProxyDummy.cpp
  ProcessForkerProxySocket.cpp
  ProcessForkerUtils.cpp
  RecallMemoryManager.cpp
  RecallTaskInjector.cpp 
  RecallReportPacker.cpp
  Session.cpp
  TapeMessageHandler.cpp
  TapeDaemonConfig.cpp
  TapeDaemonMain.cpp
  TapeReadSingleThread.cpp
  TapeWriteSingleThread.cpp
  TapeWriteTask.cpp
  TpconfigLine.cpp
  TpconfigLines.cpp)

target_link_libraries(castorTapeServerDaemon castormessages castortapereactor  ctacommon ctanameserver ctaremotens ctaOStoreSchedulerDB protobuf CTAObjectStore ctascheduler)
add_dependencies(castorTapeServerDaemon castormessagesprotobuf)

#add_library(castorTapeServerDaemonTestDummies
#  ../client/ClientSimulator.cpp 
#  ../../tpcp/TpcpCommand.cpp
#  ../../tpcp/StreamOperators.cpp
#  ../../tpcp/Helper.cpp
#  ../../tpcp/TapeFseqRange.cpp)

add_executable(tapeserverd TapeDaemon.cpp)
target_link_libraries(tapeserverd castorTapeServerDaemon SCSI System Utils File TapeDrive castorcommon castorclient castorlegacymsg castorserver castortapegatewayprotocol castortapereactor ${LIBCAP_LIB} ${ZLIB_LIBRARIES} castormessages zmq)

# The 
add_library(castortapeserverdaemonunittests SHARED
  CatalogueDriveStateTest.cpp
  CatalogueTest.cpp
  DiskReadTaskTest.cpp
# DiskWriteTaskTest.cpp
# DiskWriteThreadPoolTest.cpp
  DriveConfigTest.cpp
# MigrationReportPackerTest.cpp
  ProcessForkerTest.cpp
# RecallReportPackerTest.cpp
# RecallTaskInjectorTest.cpp

# We dot know why but the TapeDaemonTest causes the following segmentation
# fault to occur in another test:
#
# [0;32m[----------] [m48 tests from OStoreDBPlusMockSchedulerTest/SchedulerTest
# [0;32m[ RUN	 ] [mOStoreDBPlusMockSchedulerTest/SchedulerTest.createStorageClass_new_as_adminOnAdminHost/0
# 
# 
# Program received signal SIGSEGV, Segmentation fault.
# 0x0000003b3a69c0d0 in std::basic_string<char, std::char_traits<char>, std::allocator<char> >::size() const () from /usr/lib64/libstdc++.so.6
# Missing separate debuginfos, use: debuginfo-install boost-system-1.41.0-25.el6.x86_64 boost-thread-1.41.0-25.el6.x86_64 castor-lib-2.1.15-2657.slc6.x86_64 castor-lib-server-2.1.15-2657.slc6.x86_64 cryptopp-5.6.2-2.el6.x86_6
# 4 glibc-2.12-1.149.el6_6.9.x86_64 gmock-1.5.0-4.el6.x86_64 gtest-1.5.0-5.el6.x86_64 keyutils-libs-1.4-5.el6.x86_64 krb5-libs-1.10.3-33.el6.x86_64 libattr-2.4.44-7.el6.x86_64 libcap-2.16-5.5.el6.x86_64 libcom_err-1.41.12-21.
# el6.x86_64 libgcc-4.4.7-11.el6.x86_64 librados2-0.87.1-0.el6.x86_64 libselinux-2.0.94-5.8.el6.x86_64 libstdc++-4.4.7-11.el6.x86_64 libuuid-2.17.2-12.18.el6.x86_64 lttng-ust-2.4.1-1.el6.x86_64 nspr-4.10.6-1.el6_5.x86_64 nss-
# 3.16.2.3-3.el6_6.cern.x86_64 nss-util-3.16.2.3-2.el6_6.x86_64 openpgm-5.1.118-3.el6.x86_64 openssl-1.0.1e-30.el6_6.7.x86_64 protobuf-2.3.0-9.el6.x86_64 sqlite-3.6.20-1.el6.x86_64 userspace-rcu-0.7.7-1.el6.x86_64 xrootd-clie
# nt-libs-4.2.1-2.el6.x86_64 xrootd-libs-4.2.1-2.el6.x86_64 zeromq3-3.2.5-1.el6.x86_64 zlib-1.2.3-29.el6.x86_64
# (gdb) bt
# #0  0x0000003b3a69c0d0 in std::basic_string<char, std::char_traits<char>, std::allocator<char> >::size() const () from /usr/lib64/libstdc++.so.6
# #1  0x00007ffff5746416 in cta::objectstore::RootEntry::getAgentRegisterAddress (this=0x7fffffffdb10) at /home/murrayc3/cta/checkout/CTA/objectstore/RootEntry.cpp:639
# #2  0x00007ffff5746589 in cta::objectstore::RootEntry::addOrGetAgentRegisterPointerAndCommit (this=0x7fffffffdb10, agent=..., log=...) at /home/murrayc3/cta/checkout/CTA/objectstore/RootEntry.cpp:649
# #3  0x00007ffff4f0ed69 in cta::(anonymous namespace)::OStoreDBWrapper<cta::objectstore::BackendVFS>::OStoreDBWrapper (this=0x822950, context="UnitTest") at /home/murrayc3/cta/checkout/CTA/scheduler/OStoreDB/OStoreDBFactory.
# hpp:61
# #4  0x00007ffff4f1a081 in cta::OStoreDBFactory<cta::objectstore::BackendVFS>::create (this=0x7ffff51bceb8) at /home/murrayc3/cta/checkout/CTA/scheduler/OStoreDB/OStoreDBFactory.hpp:257
# #5  0x00007ffff4f60800 in unitTests::SchedulerTest::SetUp (this=0x7f6510) at /home/murrayc3/cta/checkout/CTA/scheduler/SchedulerTest.cpp:89
# #6  0x00007ffff49ec023 in testing::Test::Run() () from /usr/lib64/libgtest.so.0
# #7  0x00007ffff49ec188 in testing::internal::TestInfoImpl::Run() () from /usr/lib64/libgtest.so.0
# #8  0x00007ffff49ec245 in testing::TestCase::Run() () from /usr/lib64/libgtest.so.0
# #9  0x00007ffff49ef10f in testing::internal::UnitTestImpl::RunAllTests() () from /usr/lib64/libgtest.so.0
# #10 0x00000000004adbe3 in main (argc=1, argv=0x7fffffffe1b8) at /home/murrayc3/cta/checkout/CTA/tests/unit_tests.cpp:26
# (gdb) 
# TapeDaemon.cpp
# TapeDaemonTest.cpp
)

target_link_libraries(castortapeserverdaemonunittests
  castorserverdummies)
