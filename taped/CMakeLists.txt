# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

add_subdirectory (scsi)
add_subdirectory (drive)
add_subdirectory (system)
add_subdirectory (file)
add_subdirectory (session)
add_subdirectory (rao)
add_subdirectory (daemon)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctatapedaemon ctacommon ctatelemetry)

add_manpage(cta-taped)

install (TARGETS cta-taped DESTINATION ${CMAKE_INSTALL_BINDIR})
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1 DESTINATION ${CMAKE_INSTALL_MANDIR}/man1)
install (FILES cta-taped.sysconfig DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/sysconfig RENAME cta-taped)

# CTA's cta-taped integration tests.
add_library(cta-taped-integration-tests SHARED
  cta-tapedIntegrationTests.cpp)
set_property(TARGET cta-taped-integration-tests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-taped-integration-tests PROPERTY VERSION "${CTA_LIBVERSION}")

target_link_libraries(cta-taped-integration-tests
  cta-unit-test-helper
  ctacommon)

install(TARGETS cta-taped-integration-tests DESTINATION ${CMAKE_INSTALL_LIBDIR})
