# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later


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

target_include_directories(ctadisk PRIVATE ${XROOTD_INCLUDE_DIR})

target_link_libraries (ctadisk XrdCl ctacommon)

set_property(TARGET ctadisk PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadisk PROPERTY   VERSION "${CTA_LIBVERSION}")

add_library(ctadiskunittests SHARED
  DiskSystemTest.cpp
)

target_include_directories(ctadiskunittests PRIVATE ${XROOTD_INCLUDE_DIR})

set_property(TARGET ctadiskunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctadiskunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

install(TARGETS ctadisk DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(TARGETS ctadiskunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

install(PROGRAMS scripts/cta-eosdf.sh DESTINATION /usr/bin)
