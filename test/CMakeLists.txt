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

add_executable(castorUnitTests
  castorUnitTests.cpp
  #../castor/exception/ExceptionUnitTest.cpp
  ../castor/tape/tapeserver/SCSI/DeviceTest.cpp
  ../castor/tape/tapeserver/SCSI/StructuresTest.cpp
  ../castor/tape/tapeserver/Utils/RegexTest.cpp
  ../castor/tape/tapeserver/Drive/DriveTest.cpp
  ../castor/tape/tapeserver/Exception/ExceptionTest.cpp
  ../castor/tape/tapeserver/File/StructuresTest.cpp
)

set_property(SOURCE ../SCSI/StructuresTest.cpp
  PROPERTY COMPILE_FLAGS -fno-strict-aliasing
)

target_link_libraries(castorUnitTests TapeDrive Exception SCSI System Utils File 
  ${GTEST_LIBRARY} gmock pthread castorcommon)

install(TARGETS castorUnitTests
  DESTINATION ${CASTOR_DEST_BIN_DIR})
