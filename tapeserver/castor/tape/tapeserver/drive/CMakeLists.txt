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

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

set(TAPEDRIVE_LIBRARY_SRCS
  DriveGeneric.cpp
  FakeDrive.cpp)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    foreach(TAPEDRIVE_LIBRARY_SRC ${TAPEDRIVE_LIBRARY_SRCS})
      set_property(SOURCE ${TAPEDRIVE_LIBRARY_SRC}
        PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    endforeach(TAPEDRIVE_LIBRARY_SRC)
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(TapeDrive ${TAPEDRIVE_LIBRARY_SRCS})
target_link_libraries(TapeDrive SCSI ctatapedaemon)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    set_property(SOURCE TapeDriveReadWriteTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_executable(TapeDriveReadWriteTest
  TapeDriveReadWriteTest.cpp)
target_link_libraries(TapeDriveReadWriteTest
  TapeDrive
  ctamediachanger
  SCSI
  System
  ctacommon
  #ctaclient
  ctaTapeServerDaemon
  gtest
  pthread)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    set_property(SOURCE DriveTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(ctatapeserverdriveunittests SHARED
  DriveTest.cpp)
set_property(TARGET ctatapeserverdriveunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverdriveunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(ctatapeserverdriveunittests
  ctaTapeServerDaemon
  System
  TapeDrive
  ctautils)

install(TARGETS ctatapeserverdriveunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
