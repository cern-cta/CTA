# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-tape-label
  TapeLabelCmd.cpp
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdMain.cpp)

target_link_libraries (cta-tape-label
  ctacommon
  ctatapeddrive
  ctamediachanger
  ctacatalogue
  ctascsi
)

add_library(ctatapelabelunittests SHARED
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdLineArgsTest.cpp)

set_property(TARGET ctatapelabelunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctatapelabelunittests PROPERTY   VERSION "${CTA_LIBVERSION}")

add_manpage(cta-tape-label)

install(TARGETS cta-tape-label DESTINATION ${CMAKE_INSTALL_BINDIR})
install(TARGETS ctatapelabelunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-tape-label.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
