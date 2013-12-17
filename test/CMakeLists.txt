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

# Those tests are single threaded, functionnal and error tests. They could
# actually fail in helgrind/drd (but memcheck is recommended)
add_executable(castorUnitTests
  castorUnitTests.cpp
  #../castor/exception/ExceptionUnitTest.cpp
  ../castor/tape/tapeserver/SCSI/DeviceTest.cpp
  ../castor/tape/tapeserver/SCSI/StructuresTest.cpp
  ../castor/tape/tapeserver/utils/RegexTest.cpp
  ../castor/tape/tapeserver/drive/DriveTest.cpp
  ../castor/tape/tapeserver/exception/ExceptionTest.cpp
  ../castor/tape/tapeserver/file/StructuresTest.cpp
  ../castor/tape/tapeserver/threading/ThreadingTests.cpp
)
set_property(SOURCE ../SCSI/StructuresTest.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)
target_link_libraries(castorUnitTests TapeDrive Exception SCSI System Utils File 
  castorTapeServerThreading ${GTEST_LIBRARY} gmock pthread castorcommon)

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (and also memcheck)
add_executable(castorThreadedUnitTests
  castorUnitTests.cpp
  ../castor/tape/tapeserver/threading/ThreadingMTTests.cpp
)
target_link_libraries(castorThreadedUnitTests TapeDrive Exception SCSI System 
  Utils File castorTapeServerThreading 
  ${GTEST_LIBRARY} gmock pthread castorcommon)

# Those tests are multi threaded. They could should also be run in 
# helgrind/drd (but not memcheck)
add_executable(castorMultiprocessUnitTests
  castorUnitTests.cpp
  ../castor/tape/tapeserver/threading/ThreadingMPTests.cpp
)
target_link_libraries(castorMultiprocessUnitTests TapeDrive Exception SCSI System 
  Utils File castorTapeServerThreading 
  ${GTEST_LIBRARY} gmock pthread castorcommon)

install(TARGETS castorUnitTests castorThreadedUnitTests castorMultiprocessUnitTests
  DESTINATION ${CASTOR_DEST_BIN_DIR})
