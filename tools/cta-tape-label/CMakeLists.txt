# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_executable(cta-tape-label
  TapeLabelCmd.cpp
  TapeLabelCmdLineArgs.cpp
  TapeLabelCmdMain.cpp)

if(CTA_SANITIZERS MATCHES "(^|,)address(,|$)")
  target_compile_definitions(cta-tape-label PRIVATE CTA_TAPE_LABEL_ASAN)
endif()

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
if(CTA_BUILD_TEST_PACKAGES)
  install(TARGETS ctatapelabelunittests DESTINATION ${CMAKE_INSTALL_LIBDIR})
endif()
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-tape-label.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
