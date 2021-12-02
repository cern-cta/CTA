# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2003-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 3.17)

include_directories(${CMAKE_SOURCE_DIR}/tapeserver)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    set_property(SOURCE Device.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    set_property(SOURCE DeviceTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    set_property(SOURCE StructuresTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(SCSI Device.cpp Structures.cpp Constants.cpp Exception.cpp)

add_library(ctatapeserverscsiunittests SHARED
  DeviceTest.cpp
  StructuresTest.cpp)
set_property(TARGET ctatapeserverscsiunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverscsiunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverscsiunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
