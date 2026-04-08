# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

# Old CASTOR's tapeserverd daemon code
add_subdirectory (castor)

# CTA's cta-taped code
add_subdirectory (daemon)
add_subdirectory (session)
# The tape session's threads are in a separate directory (session, but compiled
# from the previous one to create a single library).
add_subdirectory (tapelabel)
add_subdirectory (readtp)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctatapedaemon ctacommon)

add_manpage(cta-taped)

install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-taped.logrotate DESTINATION /etc/logrotate.d RENAME cta-taped)
install (FILES cta-taped.sysconfig DESTINATION /etc/sysconfig RENAME cta-taped)
install (FILES cta-taped.service DESTINATION /etc/systemd/system)

# CTA's cta-taped integration tests.
add_library(cta-tapedIntegrationTests SHARED
  cta-tapedIntegrationTests.cpp)
set_property(TARGET cta-tapedIntegrationTests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET cta-tapedIntegrationTests PROPERTY   VERSION "${CTA_LIBVERSION}")

target_link_libraries(cta-tapedIntegrationTests
  unitTestHelper
  ctacommon)

install(TARGETS cta-tapedIntegrationTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
