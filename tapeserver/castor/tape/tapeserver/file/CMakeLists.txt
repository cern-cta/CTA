# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2003-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

find_package (xrootd REQUIRED)

include_directories(/usr/include/shift)
include_directories(${CMAKE_SOURCE_DIR}/tapeserver)
include_directories (${XROOTD_INCLUDE_DIR})

set(TAPESERVER_FILE_LIBRARY_SRCS
  File.cpp
  Structures.cpp)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    foreach(TAPESERVER_FILE_LIBRARY_SRC ${TAPESERVER_FILE_LIBRARY_SRCS})
      set_property(SOURCE ${TAPESERVER_FILE_LIBRARY_SRC}
        PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    endforeach(TAPESERVER_FILE_LIBRARY_SRC)
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    set_property(SOURCE BasicReadWriteTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_executable(BasicReadWriteTest
  BasicReadWriteTest.cpp)

target_link_libraries(BasicReadWriteTest
  TapeDrive
  ctamediachanger
  SCSI
  System
  ctacommon
  ctaTapeServerDaemon
  gtest
  pthread)

add_library(File
  ${TAPESERVER_FILE_LIBRARY_SRCS})

if(CMAKE_COMPILER_IS_GNUCC)
  if(GCC_VERSION_GE_4_8_0)
    set_property(SOURCE StructuresTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    set_property(SOURCE FileTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
    set_property(SOURCE CryptoPPTest.cpp
      PROPERTY COMPILE_FLAGS " -Wno-unused-local-typedefs")
  endif(GCC_VERSION_GE_4_8_0)
endif(CMAKE_COMPILER_IS_GNUCC)

add_library(ctatapeserverfileunittests SHARED
  StructuresTest.cpp
  FileTest.cpp
)

target_link_libraries(ctatapeserverfileunittests
  ctadisk
)

set_property(TARGET ctatapeserverfileunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapeserverfileunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctatapeserverfileunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
