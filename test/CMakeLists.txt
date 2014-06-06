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
# Try to find the four STK client libaries: libapi.so, libutl.so, libipc.so 
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

# Add STK related tests if all four STK client libaries
# (libapi.so, libutl.so, libipc.so and libcl.so) have been found
if (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)
  message(STATUS "Found all four STK libraries - adding STK related unit tests")
  set (STK_UNITTEST_SRC_FILES
    ../castor/tape/rmc/AcceptHandler.cpp
    ../castor/tape/rmc/Acs.cpp
    ../castor/tape/rmc/AcsCmd.cpp
    ../castor/tape/rmc/AcsDismountCmd.cpp
    ../castor/tape/rmc/AcsDismountCmdLine.cpp
    ../castor/tape/rmc/AcsDismountCmdLineTest.cpp
    ../castor/tape/rmc/AcsDismountCmdTest.cpp
    ../castor/tape/rmc/AcsMountCmd.cpp
    ../castor/tape/rmc/AcsMountCmdLine.cpp
    ../castor/tape/rmc/AcsMountCmdLineTest.cpp
    ../castor/tape/rmc/AcsMountCmdTest.cpp
    ../castor/tape/rmc/AcsQueryVolumeCmd.cpp
    ../castor/tape/rmc/AcsQueryVolumeCmdLine.cpp
    ../castor/tape/rmc/AcsQueryVolumeCmdLineTest.cpp
    ../castor/tape/rmc/AcsQueryVolumeCmdTest.cpp
    ../castor/tape/rmc/AcsTest.cpp
    ../castor/tape/rmc/Cmd.cpp
    ../castor/tape/rmc/ConnectionHandler.cpp
    ../castor/tape/rmc/RmcDaemon.cpp
    ../castor/tape/rmc/RmcDaemonTest.cpp)

  foreach(SRC_FILE ${STK_UNITTEST_SRC_FILES})
    set_property(SOURCE ${SRC_FILE}
      PROPERTY COMPILE_FLAGS "-DCDK -DLINUX -I/usr/include/CDK")
  endforeach ()
endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)

# Try to find the libcap library because it will not exist on Mac OS X
find_library (LIBCAP_LIB cap)
if (LIBCAP_LIB)
  message(STATUS "Found the libcap library")
  set (LIBCAPUNITTEST_SRC_FILES
    ../castor/tape/tapeserver/daemon/SmartCapTest.cpp)
endif (LIBCAP_LIB)

# Create a shared librbary for the dummies supporting the tests
add_library(TestDummies
  ../castor/io/DummyPollEventHandler.cpp
  ../castor/io/DummyPollReactor.cpp
  ../castor/legacymsg/CupvProxyDummy.cpp
  ../castor/legacymsg/RmcProxyDummy.cpp
  ../castor/legacymsg/RmcProxyDummyFactory.cpp
  ../castor/legacymsg/VdqmProxyDummy.cpp
  ../castor/legacymsg/VdqmProxyDummyFactory.cpp
  ../castor/legacymsg/VmgrProxyDummy.cpp
  ../castor/legacymsg/VmgrProxyDummyFactory.cpp
  ../castor/legacymsg/TapeserverProxyDummy.cpp
  ../castor/legacymsg/TapeserverProxyDummyFactory.cpp
  ../castor/log/DummyLogger.cpp
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
  ../castor/io/PollReactorImplTest.cpp
  ../castor/legacymsg/CommonMarshalTest.cpp
  ../castor/legacymsg/CupvMarshalTest.cpp
  ../castor/legacymsg/CupvProxyDummy.cpp
  ../castor/legacymsg/GenericMarshalTest.cpp
  ../castor/legacymsg/NsProxy.cpp
  ../castor/legacymsg/NsProxyDummy.cpp
  ../castor/legacymsg/NsProxyDummyFactory.cpp
  ../castor/legacymsg/RmcProxyDummy.cpp
  ../castor/legacymsg/RmcProxyDummyFactory.cpp
  ../castor/legacymsg/VdqmProxyDummy.cpp
  ../castor/legacymsg/VdqmProxyDummyFactory.cpp
  ../castor/legacymsg/VmgrMarshalTest.cpp
  ../castor/legacymsg/RmcMarshalTest.cpp
  ../castor/legacymsg/RmcProxy.cpp
  ../castor/legacymsg/RmcProxyTcpIp.cpp
  ../castor/legacymsg/RmcProxyTcpIpTest.cpp
  ../castor/legacymsg/ScsiLibrarySlot.cpp
  ../castor/legacymsg/ScsiLibrarySlotTest.cpp
  ../castor/legacymsg/TapeMarshalTest.cpp
  ../castor/legacymsg/TapeserverProxy.cpp
  ../castor/legacymsg/TapeserverProxyTcpIp.cpp
  ../castor/legacymsg/VdqmMarshalTest.cpp
  ../castor/legacymsg/VdqmProxy.cpp
  ../castor/legacymsg/VdqmProxyTcpIp.cpp
  ../castor/legacymsg/VmgrProxy.cpp
  ../castor/legacymsg/VmgrProxyTcpIp.cpp
  ../castor/log/ParamTest.cpp
  ../castor/log/LogContextTest.cpp
  ../castor/log/StringLoggerTest.cpp
  ../castor/log/SyslogLoggerTest.cpp
  ../castor/server/DaemonTest.cpp
  ../castor/tape/tapeserver/client/ClientProxyTest.cpp
  ../castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
  ../castor/tape/tapeserver/daemon/CapabilityUtilsDummy.cpp
#  ../castor/tape/tapeserver/daemon/DiskReadTaskTest.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogue.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogueEntry.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogueEntryTest.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogueTest.cpp
  ../castor/tape/tapeserver/daemon/MountSessionAcceptHandler.cpp
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
  ../castor/tape/tapeserver/exception/ExceptionTest.cpp
  ../castor/tape/tapeserver/file/StructuresTest.cpp
  ../castor/tape/tapeserver/file/FileTest.cpp
  ../castor/tape/tapeserver/SCSI/DeviceTest.cpp
  ../castor/tape/tapeserver/SCSI/StructuresTest.cpp
  ../castor/tape/tapeserver/utils/RegexTest.cpp
  ../castor/tape/tapeserver/threading/ThreadingTests.cpp
  ../castor/tape/tapeserver/threading/AtomicCounterTest.cpp
  ../castor/tape/utils/TpconfigLineTest.cpp
  ../castor/utils/SmartArrayPtrTest.cpp
  ../castor/utils/SmartFdTest.cpp
  ../castor/utils/UtilsTest.cpp
  ${STK_UNITTEST_SRC_FILES}
  ${LIBCAPUNITTEST_SRC_FILES})

set_property(SOURCE ../SCSI/StructuresTest.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)
target_link_libraries(castorUnitTests
  castorcommon
  castorclient
  castortapelegacymsg
  castorTapeServerThreading
  castortapeutils
  Exception
  SCSI
  System
  Utils
  File
  TestDummies
  castortapegatewayprotocol
  castorTapeServerThreading
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

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (and also memcheck)
add_executable(castorThreadedUnitTests
  castorUnitTests.cpp
  ../castor/tape/tapeserver/threading/ThreadingMTTests.cpp
  ../castor/tape/tapeserver/daemon/MountSessionTest.cpp
  ../castor/tape/tapeserver/threading/ThreadingBlockingQTests.cpp
  ../castor/tape/tapeserver/daemon/CapabilityUtilsDummy.cpp
  ../castor/tape/tapeserver/daemon/RecallTaskInjectorTest.cpp
  ../castor/tape/tapeserver/daemon/MigrationReportPackerTest.cpp
)

target_link_libraries(castorThreadedUnitTests
  Exception
  castorclient
  castorcommon
  castortapegatewayprotocol
  castortapelegacymsg
  castorTapeServerThreading
  castorTapeServerDaemonTestDummies
  castorTapeServerDaemon
  TestDummies
  SCSI
  System
  Utils
  castortapegatewayprotocol
  castortapelegacymsg
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
  Utils
  File 
  ${ZLIB_LIBRARIES})

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (but not memcheck)
add_executable(castorMultiprocessUnitTests
  castorUnitTests.cpp
  ../castor/tape/tapeserver/threading/ThreadingMPTests.cpp
)
target_link_libraries(castorMultiprocessUnitTests
  TapeDrive
  Exception
  SCSI
  System 
  Utils File castorTapeServerThreading 
  ${GTEST_LIBRARY} gmock pthread castorcommon castorclient)

install(TARGETS
  castorUnitTests
  castorThreadedUnitTests
  castorMultiprocessUnitTests
  DESTINATION ${CASTOR_DEST_BIN_DIR})
