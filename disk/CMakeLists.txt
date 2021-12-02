# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
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

find_package (xrootd REQUIRED)
find_package (xrootdclient REQUIRED)

include_directories (${XROOTD_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_library(ctadisk SHARED
  DiskReporter.cpp
  DiskReporterFactory.cpp
  EOSReporter.cpp
  DiskFile.cpp
  RadosStriperPool.cpp
  DiskSystem.cpp
  JSONDiskSystem.cpp
  JSONFreeSpace.cpp
)

target_link_libraries (ctadisk XrdCl cryptopp radosstriper)

set_property(TARGET ctadisk PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadisk PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctadiskunittests SHARED
  CryptoPPTest.cpp
  DiskSystemTest.cpp
)

set_property(TARGET ctadiskunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadiskunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctadisk DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadiskunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
