# SPDX-FileCopyrightText: 2021 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

add_subdirectory(routines/disk)
add_subdirectory(routines/repack)

add_library(ctamaintd
  MaintenanceDaemon.cpp
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

add_executable (cta-maintd cta-maintd.cpp)
target_link_libraries(cta-maintd ctamaintd)

add_manpage (cta-maintd)

install (TARGETS cta-maintd DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-maintd.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-maintd.logrotate DESTINATION /etc/logrotate.d RENAME cta-maintd)
install (FILES cta-maintd.service DESTINATION /etc/systemd/system)
install (FILES cta-maintd.conf.example
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

