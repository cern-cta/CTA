# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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
