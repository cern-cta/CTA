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
include_directories(${CMAKE_BINARY_DIR})

find_package( ZLIB REQUIRED )

# Create a shared librbary for the dummies supporting the tests
#add_library(TestDummies
#  ../castor/mediachanger/MmcProxyDummy.cpp
#  ../castor/messages/TapeserverProxyDummy.cpp
#  ../castor/legacymsg/RmcProxyDummy.cpp
#  ../castor/log/DummyLogger.cpp
#  ../castor/tape/reactor/DummyPollEventHandler.cpp
#  ../castor/tape/reactor/DummyPollReactor.cpp
#  ../castor/server/ProcessCapDummy.cpp
#)

find_package (xrootd REQUIRED)
include_directories (${XROOTD_INCLUDE_DIR})

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (and also memcheck)
#add_executable(castorThreadedUnitTests
 # castorUnitTests.cpp
 # ../castor/server/ThreadingMTTests.cpp
 # ../castor/tape/tapeserver/daemon/DataTransferSessionTest.cpp
# ../castor/server/ThreadingBlockingQTests.cpp
 # ../castor/tape/tapeserver/daemon/RecallTaskInjectorTest.cpp
 # ../castor/tape/tapeserver/daemon/MigrationReportPackerTest.cpp
 # ../castor/tape/tapeserver/daemon/TaskWatchDogTest.cpp
#  ../castor/tape/tapeserver/file/CryptoPPTest.cpp
#)
