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
include_directories(${PROJECT_SOURCE_DIR}/tapeserver)

set_source_files_properties(CRC.cpp PROPERTIES COMPILE_FLAGS -O2)
set (UTILS_LIB_SRC_FILES
  SmartFd.cpp
  SmartFILEPtr.cpp
  Timer.cpp
  CRC.cpp
  utils.cpp
  ../common/CastorConfiguration.cpp
  ../exception/Backtrace.cpp
  ../exception/NoEntry.cpp
  ../exception/InvalidArgument.cpp
  ../exception/Errnum.cpp
  ../exception/NotAnOwner.cpp
  ../exception/BadAlloc.cpp
  ../exception/InvalidConfigEntry.cpp
  ../exception/MissingOperand.cpp
  ../exception/NoPortInRange.cpp
  ../exception/TimeOut.cpp
  ../exception/CommandLineNotParsed.cpp
  ../exception/OutOfMemory.cpp
  ../BaseObject.cpp
  ../Services.cpp
  ../Factories.cpp
  ../System.cpp
  ../server/ProcessCapDummy.cpp
  ../server/Semaphores.cpp
  ../server/Mutex.cpp
  ../server/Threading.cpp
  ../server/ProcessCap.cpp
  ../server/SmartCap.cpp
  ../server/Daemon.cpp)

add_library (ctautils SHARED ${UTILS_LIB_SRC_FILES})

target_link_libraries(ctautils castorlog)

install(TARGETS ctautils DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_library (ctautilsunittests SHARED
  SmartArrayPtrTest.cpp
  SmartFdTest.cpp
  UtilsTest.cpp)

install(TARGETS ctautilsunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
