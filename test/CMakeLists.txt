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
    ../castor/tape/rmc/DebugBuf.cpp
    ../castor/tape/rmc/RmcDaemon.cpp
    ../castor/tape/rmc/RmcDaemonTest.cpp)

  foreach(SRC_FILE ${STK_UNITTEST_SRC_FILES})
    set_property(SOURCE ${SRC_FILE}
      PROPERTY COMPILE_FLAGS "-DCDK -DLINUX -I/usr/include/CDK")
  endforeach ()
endif (STK_API_LIB AND STK_UTL_LIB AND STK_IPC_LIB AND STK_CL_LIB)

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
  ../castor/io/DummyPollEventHandler.cpp
  ../castor/io/DummyPollReactor.cpp
  ../castor/legacymsg/RmcMarshalTest.cpp
  ../castor/legacymsg/TapeMarshalTest.cpp
  ../castor/legacymsg/VdqmMarshalTest.cpp
  ../castor/log/DummyLogger.cpp
  ../castor/log/LoggerImplementationTest.cpp
  ../castor/log/ParamTest.cpp
  ../castor/log/LogContextTest.cpp
  ../castor/log/StringLoggerTest.cpp
  ../castor/server/DaemonTest.cpp
  ../castor/tape/tapeserver/client/ClientProxyTest.cpp
  ../castor/tape/tapeserver/daemon/AdminAcceptHandler.cpp
  ../castor/tape/tapeserver/daemon/DebugMountSessionForVdqmProtocol.cpp
#  ../castor/tape/tapeserver/daemon/DiskReadTaskTest.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogue.cpp
  ../castor/tape/tapeserver/daemon/DriveCatalogueTest.cpp
  ../castor/tape/tapeserver/daemon/DummyRmc.cpp
  ../castor/tape/tapeserver/daemon/DummyVdqm.cpp
  ../castor/tape/tapeserver/daemon/DummyVmgr.cpp
  ../castor/tape/tapeserver/daemon/Rmc.cpp
  ../castor/tape/tapeserver/daemon/RmcImpl.cpp
  ../castor/tape/tapeserver/daemon/RmcImplTest.cpp
  ../castor/tape/tapeserver/daemon/ScsiLibrarySlot.cpp
  ../castor/tape/tapeserver/daemon/ScsiLibrarySlotTest.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemon.cpp
  ../castor/tape/tapeserver/daemon/TapeDaemonTest.cpp
  ../castor/tape/tapeserver/daemon/Vdqm.cpp
  ../castor/tape/tapeserver/daemon/VdqmAcceptHandler.cpp
  ../castor/tape/tapeserver/daemon/VdqmConnectionHandler.cpp
  ../castor/tape/tapeserver/daemon/RecallTaskInjectorTest.cpp
  ../castor/tape/tapeserver/daemon/MigrationReportPackerTest.cpp
  ../castor/tape/tapeserver/daemon/RecallReportPackerTest.cpp
  ../castor/tape/tapeserver/daemon/DiskWriteThreadPoolTest.cpp
  ../castor/tape/tapeserver/daemon/DiskWriteTaskTest.cpp
  ../castor/tape/tapeserver/daemon/VdqmImpl.cpp
  ../castor/tape/tapeserver/daemon/Vmgr.cpp
  ../castor/tape/tapeserver/daemon/VmgrImpl.cpp
  ../castor/tape/tapeserver/drive/DriveTest.cpp
  ../castor/tape/tapeserver/exception/ExceptionTest.cpp
  ../castor/tape/tapeserver/file/StructuresTest.cpp
  ../castor/tape/tapeserver/file/FileTest.cpp
  ../castor/tape/tapeserver/SCSI/DeviceTest.cpp
  ../castor/tape/tapeserver/SCSI/StructuresTest.cpp
  ../castor/tape/tapeserver/utils/RegexTest.cpp
  ../castor/tape/tapeserver/threading/ThreadingTests.cpp
  ../castor/tape/tapeserver/threading/AtomicCounterTest.cpp
  ../castor/utils/SmartArrayPtrTest.cpp
  ../castor/utils/SmartFdTest.cpp
  ../castor/utils/UtilsTest.cpp
  ${STK_UNITTEST_SRC_FILES})

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
  tapeserver
  tapeserverdTest
  castortapegatewayprotocol
  castorTapeServerThreading
  ${GTEST_LIBRARY}
  gmock
  pthread
  castorcommon
  castorclient
  castortapeutils
  castorvmgr
  castorvdqm
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
  ../castor/tape/tapeserver/daemon/tapeserverdTest.cpp
  ../castor/tape/tapeserver/threading/ThreadingBlockingQTests.cpp
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
  tapeserverdTest
  tapeserver
  SCSI
  Utils
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
  tapeserverdTest
  tapeserver 
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
