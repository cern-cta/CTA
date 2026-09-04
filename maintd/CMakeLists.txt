# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory(routines/disk)
add_subdirectory(routines/repack)

add_library(ctamaintd
  MaintdApp.cpp
  RoutineRunner.cpp
  RoutineRunnerFactory.cpp
)


if(NOT CTA_USE_PGSCHED)
    add_subdirectory(routines/scheduler/objectstore)
    set(SCHEDROUTINES ctaosdbroutines)
else()
    add_subdirectory(routines/scheduler/rdbms)
    set(SCHEDROUTINES ctardbroutines)
endif()

target_link_libraries(ctamaintd
	ctacommon
	ctacatalogue
	ctascheduler
	${SCHEDROUTINES}
	ctadiskroutines
	ctarepackroutines
)

add_library(ctamaintdunittests SHARED
  MaintdConfigTest.cpp
)
target_link_libraries(ctamaintdunittests
  ctacommon
  gtest
)
set_property(TARGET ctamaintdunittests PROPERTY SOVERSION "${CTA_SOVERSION}")
set_property(TARGET ctamaintdunittests PROPERTY VERSION "${CTA_LIBVERSION}")
install(TARGETS ctamaintdunittests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})

add_executable (cta-maintd cta-maintd.cpp)
target_link_libraries(cta-maintd ctamaintd)

if(NOT CTA_USE_PGSCHED)
  set(CTA_MAINTD_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-maintd.example.toml)
else()
  set(CTA_MAINTD_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-maintd.pg.example.toml)
endif()

add_custom_target(cta-maintd-config-check
  COMMAND $<TARGET_FILE:cta-maintd>
    --config-strict
    --config ${CTA_MAINTD_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-maintd
  COMMENT "Validating the cta-maintd example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-maintd-config-check)

add_manpage (cta-maintd)

install (TARGETS cta-maintd DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-maintd.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-maintd.logrotate DESTINATION /etc/logrotate.d RENAME cta-maintd)
install (FILES cta-maintd.service DESTINATION ${CTA_SYSTEMD_UNIT_DIR})
if(NOT CTA_USE_PGSCHED)
  install (FILES cta-maintd.example.toml
    DESTINATION usr/${CMAKE_INSTALL_DOCDIR}
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
else()
  install (FILES cta-maintd.pg.example.toml
    DESTINATION usr/${CMAKE_INSTALL_DOCDIR}
    RENAME cta-maintd.example.toml
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
endif()
