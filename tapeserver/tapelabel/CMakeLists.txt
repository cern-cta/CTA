# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_executable(cta-tape-label
  TapeLabelCmd.cpp
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdMain.cpp)

target_link_libraries (cta-tape-label
  ctacommon
  TapeDrive
  ctamediachanger
  ctacatalogue
  SCSI
)

add_library(ctatapelabelunittests SHARED
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdLineArgsTest.cpp)

set_property(TARGET ctatapelabelunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapelabelunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

add_manpage(cta-tape-label)

install(TARGETS cta-tape-label DESTINATION /usr/bin)
install(TARGETS ctatapelabelunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-tape-label.1cta DESTINATION /usr/share/man/man1)
