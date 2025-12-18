# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

find_package (xrootd REQUIRED)
find_package (xrootdclient REQUIRED)

include_directories (${XROOTD_INCLUDE_DIR} ${CMAKE_SOURCE_DIR})

add_library(ctadisk SHARED
  DiskReporter.cpp
  DiskReporterFactory.cpp
  EOSReporter.cpp
  DiskFile.cpp
  DiskSystem.cpp
  JSONDiskSystem.cpp
  JSONFreeSpace.cpp
  XrdClException.cpp
)

target_link_libraries (ctadisk XrdCl)

set_property(TARGET ctadisk PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadisk PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctadiskunittests SHARED
  DiskSystemTest.cpp
)

set_property(TARGET ctadiskunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadiskunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctadisk DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadiskunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install(FILES scripts/cta-eosdf.sh DESTINATION /usr/bin)
