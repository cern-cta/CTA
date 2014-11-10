#                      CMakeLists.txt
#
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
# Eric.Cano@cern.ch
#

cmake_minimum_required (VERSION 2.6)
find_package( ZLIB REQUIRED )
# Try to find the four STK client libaries: libaExcpi.so, libutl.so, libipc.so 
# and libcl.so
find_library (STK_API_LIB api PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
if (STK_API_LIB)
  message(STATUS "Found the STK api library: ${STK_API_LIB}")
endif (STK_API_LIB)
find_library (STK_UTL_LIB utl PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
if (STK_UTL_LIB)
  message(STATUS "Found the STK utl library: ${STK_UTL_LIB}")
endif (STK_UTL_LIB)
find_library (STK_IPC_LIB ipc PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
if (STK_IPC_LIB)
  message(STATUS "Found the STK ipc library: ${STK_IPC_LIB}")
endif (STK_IPC_LIB)
find_library (STK_CL_LIB cl PATHS /usr/lib64/CDK /usr/lib/CDK NO_DEFAULT_PATH)
if (STK_CL_LIB)
  message(STATUS "Found the STK cl library: ${STK_CL_LIB}")
endif (STK_CL_LIB)
set (STK_UNITTEST_SRC_FILES)

# Add STK related tests if all four STK client libaries
# (libapi.so, libutl.so, libipc.so and libcl.so) have been found
if (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
  message(STATUS "Found all four STK libraries - adding STK related unit tests")
  set (STK_UNITTEST_SRC_FILES
    ../castor/acs/Acs.cpp
    ../castor/acs/AcsCmd.cpp
    ../castor/acs/AcsCmdLine.cpp
    ../castor/acs/AcsDaemon.cpp
    ../castor/acs/AcsDaemonConfig.cpp
    ../castor/acs/AcsDaemonTest.cpp
    ../castor/acs/AcsDismountCmd.cpp
    ../castor/acs/AcsDismountCmdLine.cpp
    ../castor/acs/AcsDismountCmdLineTest.cpp
    ../castor/acs/AcsDismountTape.cpp
    ../castor/acs/AcsImpl.cpp
    ../castor/acs/AcsLibraryInteraction.cpp
    ../castor/acs/AcsMessageHandler.cpp
    ../castor/acs/AcsMountCmd.cpp
    ../castor/acs/AcsMountCmdLine.cpp
    ../castor/acs/AcsMountCmdLineTest.cpp
    ../castor/acs/AcsMountTapeReadOnly.cpp
    ../castor/acs/AcsMountTapeReadWrite.cpp
    ../castor/acs/AcsPendingRequests.cpp
    ../castor/acs/AcsQueryVolumeCmd.cpp
    ../castor/acs/AcsQueryVolumeCmdLine.cpp
    ../castor/acs/AcsQueryVolumeCmdLineTest.cpp
    ../castor/acs/AcsRequest.cpp
    ../castor/acs/AcsRequestDismountTape.cpp
    ../castor/acs/CmdLineTool.cpp)

  foreach(SRC_FILE ${STK_UNITTEST_SRC_FILES})
    set_property(SOURCE ${SRC_FILE}
      PROPERTY COMPILE_FLAGS "-DCDK -DLINUX -I/usr/include/CDK")
  endforeach ()
endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)

# Create a shared librbary for the dummies supporting the tests
add_library(TestDummies
  ../castor/mediachanger/MmcProxyDummy.cpp
  ../castor/messages/TapeserverProxyDummy.cpp
  ../castor/legacymsg/CupvProxyDummy.cpp
  ../castor/legacymsg/RmcProxyDummy.cpp
  ../castor/legacymsg/VdqmProxyDummy.cpp
  ../castor/legacymsg/VdqmProxyDummyFactory.cpp
  ../castor/legacymsg/VmgrProxyDummy.cpp
  ../castor/legacymsg/VmgrProxyDummyFactory.cpp
  ../castor/log/DummyLogger.cpp
  ../castor/tape/reactor/DummyPollEventHandler.cpp
  ../castor/tape/reactor/DummyPollReactor.cpp
  ../castor/server/ProcessCapDummy.cpp
)

# Those tests are single threaded, functionnal and error tests. They could
# actually fail in helgrind/drd (but memcheck is recommended)
set_property(SOURCE
  ../castor/io/IoTest.cpp
APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
add_executable(castorUnitTests
  castorUnitTests.cpp
  ../castor/exception/ExceptionTest.cpp
  ../castor/io/IoTest.cpp
  ../castor/messages/TapeserverProxy.cpp
  ../castor/messages/TapeserverProxyZmq.cpp
  ../castor/legacymsg/CommonMarshalTest.cpp
  ../castor/legacymsg/CupvMarshalTest.cpp
  ../castor/legacymsg/CupvProxyDummy.cpp
  ../castor/legacymsg/GenericMarshalTest.cpp
  ../castor/legacymsg/NsProxy.cpp
  ../castor/legacymsg/NsProxyDummy.cpp
  ../castor/legacymsg/NsProxyDummyFactory.cpp
  ../castor/legacymsg/VdqmProxyDummy.cpp
  ../castor/legacymsg/VdqmProxyDummyFactory.cpp
  ../castor/legacymsg/VmgrMarshalTest.cpp
  ../castor/legacymsg/RmcMarshalTest.cpp
  ../castor/legacymsg/RmcProxy.cpp
  ../castor/legacymsg/RmcProxyTcpIp.cpp
  ../castor/legacymsg/TapeMarshalTest.cpp
  ../castor/legacymsg/VdqmMarshalTest.cpp
  ../castor/legacymsg/VdqmProxy.cpp
  ../castor/legacymsg/VdqmProxyTcpIp.cpp
  ../castor/legacymsg/VmgrProxy.cpp
  ../castor/legacymsg/VmgrProxyTcpIp.cpp
  ../castor/log/ParamTest.cpp
  ../castor/log/LogContextTest.cpp
  ../castor/log/StringLoggerTest.cpp
  ../castor/log/SyslogLoggerTest.cpp
  ../castor/mediachanger/AcsLibrarySlot.cpp
  ../castor/mediachanger/AcsLibrarySlotTest.cpp
  ../castor/mediachanger/GenericLibrarySlotTest.cpp
  ../castor/mediachanger/MediaChangerFacadeTest.cpp
  ../castor/mediachanger/ScsiLibrarySlot.cpp
  ../castor/mediachanger/ScsiLibrarySlotTest.cpp
  ../castor/messages/MessagesTest.cpp
  ../castor/server/DaemonTest.cpp
  ../castor/server/SmartCapTest.cpp
  ../castor/tape/reactor/PollReactorImplTest.cpp
  ../castor/tape/reactor/ZMQReactorTest.cpp
  ../castor/tape/tapeserver/client/ClientProxyTest.cpp
  ../castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
#  ../castor/tape/tapeserver/daemon/DiskReadTaskTest.cpp
  ../castor/tape/tapeserver/daemon/Catalogue.cpp
  ../castor/tape/tapeserver/daemon/CatalogueDrive.cpp
  ../castor/tape/tapeserver/daemon/CatalogueDriveTest.cpp
  ../castor/tape/tapeserver/daemon/CatalogueTest.cpp
  ../castor/tape/tapeserver/daemon/ProcessForker.cpp
  ../castor/tape/tapeserver/daemon/ProcessForkerTest.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemon.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemonTest.cpp
  ../castor/tape/tapeserver/daemon/VdqmAcceptHandler.cpp
  ../castor/tape/tapeserver/daemon/VdqmConnectionHandler.cpp
  ../castor/tape/tapeserver/daemon/RecallTaskInjectorTest.cpp
  ../castor/tape/tapeserver/daemon/MigrationReportPackerTest.cpp
  ../castor/tape/tapeserver/daemon/RecallReportPackerTest.cpp
  ../castor/tape/tapeserver/daemon/DiskWriteThreadPoolTest.cpp
  ../castor/tape/tapeserver/daemon/DiskWriteTaskTest.cpp
  ../castor/tape/tapeserver/drive/DriveTest.cpp
  ../castor/tape/tapeserver/file/StructuresTest.cpp
  ../castor/tape/tapeserver/file/FileTest.cpp
  ../castor/tape/tapeserver/SCSI/DeviceTest.cpp
  ../castor/tape/tapeserver/SCSI/StructuresTest.cpp
  ../castor/tape/tapeserver/utils/RegexTest.cpp
  ../castor/server/ThreadingTests.cpp
  ../castor/server/AtomicCounterTest.cpp
  ../castor/tape/utils/TpconfigLineTest.cpp
  ../castor/utils/SmartArrayPtrTest.cpp
  ../castor/utils/SmartFdTest.cpp
  ../castor/utils/UtilsTest.cpp
  ${LIBCAPUNITTEST_SRC_FILES}
  ${STK_UNITTEST_SRC_FILES})

set_property(SOURCE ../SCSI/StructuresTest.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

target_link_libraries(castorUnitTests
  castorcommon
  castorclient
  castorlegacymsg
  castortapeutils
  SCSI
  System
  Utils
  File
  TestDummies
  castorserver
  castortapegatewayprotocol
  castorTapeServerDaemonTestDummies
  castorTapeServerDaemon
  ${GTEST_LIBRARY}
  gmock
  pthread
  castorcommon
  castorclient
  castortapeutils
  castorvmgr
  castorvdqm
  ${LIBCAP_LIB}
  ${STK_API_LIB}
  ${STK_UTL_LIB}
  ${STK_IPC_LIB}
  ${STK_CL_LIB}
  TapeDrive
  SCSI
  System
  Utils
  ${ZLIB_LIBRARIES})

find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR})

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (and also memcheck)
add_executable(castorThreadedUnitTests
  castorUnitTests.cpp
  ../castor/server/ThreadingMTTests.cpp
  ../castor/tape/tapeserver/daemon/DataTransferSessionTest.cpp
  ../castor/server/ThreadingBlockingQTests.cpp
  ../castor/tape/tapeserver/daemon/RecallTaskInjectorTest.cpp
  ../castor/tape/tapeserver/daemon/MigrationReportPackerTest.cpp
  ../castor/tape/tapeserver/daemon/TaskWatchDogTest.cpp
  ../castor/tape/tapeserver/file/CryptoPPTest.cpp
)

target_link_libraries(castorThreadedUnitTests
  castorclient
  castorcommon
  castorlegacymsg
  castorserver
  castortapegatewayprotocol
  castorTapeServerDaemonTestDummies
  castorTapeServerDaemon
  TestDummies
  castortapegatewayprotocol
  castortapeutils
  castorvdqm
  castorvmgr
  castorrfio
  gmock
  ${GTEST_LIBRARY}
  pthread
  TapeDrive
  SCSI
  System 
  File 
  ${ZLIB_LIBRARIES}
  Utils
  cryptopp)

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (but not memcheck)
add_executable(castorMultiprocessUnitTests
  castorUnitTests.cpp
  ../castor/server/ThreadingMPTests.cpp
)
target_link_libraries(castorMultiprocessUnitTests
  TapeDrive
  SCSI
  System 
  Utils File 
  castorserver
  ${GTEST_LIBRARY} gmock pthread castorcommon castorclient)

install(TARGETS
  castorUnitTests
  castorThreadedUnitTests
  castorMultiprocessUnitTests
  DESTINATION ${CASTOR_DEST_BIN_DIR})
