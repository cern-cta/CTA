# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory (scsi)
add_subdirectory (drive)
add_subdirectory (system)
add_subdirectory (file)
add_subdirectory (session)
add_subdirectory (rao)

add_library(ctatapedutils
  TapedUtils.cpp
)
target_link_libraries(ctatapedutils PUBLIC ctacommon)

add_library(ctataped
  TapedApp.cpp
  DriveHandler.cpp
)

target_link_libraries(ctataped
	ctacommon
	ctatapedutils
)

add_executable (cta-taped cta-taped.cpp)
target_link_libraries(cta-taped ctataped)

if(NOT CTA_USE_PGSCHED)
  set(CTA_TAPED_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-taped.example.toml)
else()
  set(CTA_TAPED_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-taped.pg.example.toml)
endif()

add_custom_target(cta-taped-config-check
  COMMAND $<TARGET_FILE:cta-taped>
    --config-strict
    --config ${CTA_TAPED_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-taped
  COMMENT "Validating the cta-taped example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-taped-config-check)

add_manpage (cta-taped)

install (TARGETS cta-taped DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-taped.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-taped.logrotate DESTINATION /etc/logrotate.d RENAME cta-taped)
install (FILES cta-taped.service DESTINATION ${CTA_SYSTEMD_UNIT_DIR})
if(NOT CTA_USE_PGSCHED)
  install (FILES cta-taped.example.toml
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
else()
  install (FILES cta-taped.pg.example.toml
    DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
    RENAME cta-taped.example.toml
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()
