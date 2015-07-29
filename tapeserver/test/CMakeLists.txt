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

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver/h)
include_directories(${CMAKE_BINARY_DIR}/tapeserver)

find_package( ZLIB REQUIRED )

# Create a shared librbary for the dummies supporting the tests
add_library(TestDummies
  ../castor/mediachanger/MmcProxyDummy.cpp
  ../castor/messages/TapeserverProxyDummy.cpp
  ../castor/legacymsg/RmcProxyDummy.cpp
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
  ../castor/legacymsg/GenericMarshalTest.cpp
  ../castor/legacymsg/RmcMarshalTest.cpp
  ../castor/legacymsg/RmcProxy.cpp
  ../castor/legacymsg/RmcProxyTcpIp.cpp
  ../castor/legacymsg/TapeMarshalTest.cpp
  ../castor/log/ParamTest.cpp
  ../castor/log/LogContextTest.cpp
  ../castor/log/StringLoggerTest.cpp
  ../castor/log/SyslogLoggerTest.cpp
  ../castor/mediachanger/AcsLibrarySlot.cpp
  ../castor/mediachanger/AcsLibrarySlotTest.cpp
  ../castor/mediachanger/CmdLine.cpp
  ../castor/mediachanger/DismountCmdLine.cpp
  ../castor/mediachanger/DismountCmdLineTest.cpp
  ../castor/mediachanger/LibrarySlotParserTest.cpp
  ../castor/mediachanger/ManualLibrarySlotTest.cpp
  ../castor/mediachanger/MediaChangerFacade.cpp
  ../castor/mediachanger/MediaChangerFacadeTest.cpp
  ../castor/mediachanger/MountCmdLine.cpp
  ../castor/mediachanger/MountCmdLineTest.cpp
  ../castor/mediachanger/ScsiLibrarySlot.cpp
  ../castor/mediachanger/ScsiLibrarySlotTest.cpp
  ../castor/messages/MessagesTest.cpp
  ../castor/messages/MutexLockerTest.cpp
  ../castor/messages/MutexTest.cpp
  ../castor/server/DaemonTest.cpp
  ../castor/server/SmartCapTest.cpp
  ../castor/tape/reactor/PollReactorImplTest.cpp
  ../castor/tape/reactor/ZMQReactorTest.cpp
  ../castor/tape/tapeserver/client/ClientProxyTest.cpp
  ../castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
#  ../castor/tape/tapeserver/daemon/DiskReadTaskTest.cpp
  ../castor/tape/tapeserver/daemon/Catalogue.cpp
  ../castor/tape/tapeserver/daemon/CatalogueDrive.cpp
  ../castor/tape/tapeserver/daemon/CatalogueDriveStateTest.cpp
  ../castor/tape/tapeserver/daemon/CatalogueTest.cpp
  ../castor/tape/tapeserver/daemon/DriveConfigTest.cpp
  ../castor/tape/tapeserver/daemon/ProcessForker.cpp
  ../castor/tape/tapeserver/daemon/ProcessForkerTest.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemon.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemonTest.cpp
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
  ../castor/utils/SmartArrayPtrTest.cpp
  ../castor/utils/SmartFdTest.cpp
  ../castor/utils/UtilsTest.cpp
  ${LIBCAPUNITTEST_SRC_FILES})

set_property(SOURCE ../SCSI/StructuresTest.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

# With the exception of shared-library plugins, the CASTOR rpms only install the
# /usr/lib64/libcastor*.so symbolic links for libraries used by end-user
# developers.  Therefore the locations of the internal CASTOR libraries required
# by tapeserved and not by end-user developers need to be imported into cmake.
add_library(castorlegacymsg SHARED IMPORTED)
set_target_properties(castorlegacymsg PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorlegacymsg.so.2.1)
add_library(castorserver SHARED IMPORTED)
set_target_properties(castorserver PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorserver.so.2.1)
add_library(castortapegatewayprotocol SHARED IMPORTED)
set_target_properties(castortapegatewayprotocol PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastortapegatewayprotocol.so.2.1)
add_library(castorvdqm SHARED IMPORTED)
set_target_properties(castorvdqm PROPERTIES
  IMPORTED_LOCATION /usr/lib64/libcastorvdqm.so.2.1)

target_link_libraries(castorUnitTests
  castorcommon
  castorclient
  castorlegacymsg
  SCSI
  System
  Utils
  File
  TestDummies
  castorserver
  castortapegatewayprotocol
  castorTapeServerDaemonTestDummies
  castorTapeServerDaemon
  gtest
  gmock
  pthread
  castorcommon
  castorclient
  castorvmgr
  castorvdqm
  ${LIBCAP_LIB}
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
  castorvdqm
  castorvmgr
  castorrfio
  gmock
  gtest
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
  gtest gmock pthread castorcommon castorclient)
