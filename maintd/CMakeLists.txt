# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2021-2025 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.
cmake_minimum_required (VERSION 3.17)

add_subdirectory(routines/disk)
add_subdirectory(routines/repack)
add_subdirectory(routines/scheduler/objectstore)
add_subdirectory(routines/scheduler/rdbms)

add_library(ctamaintd
  RoutineRunner.cpp
  RoutineRunnerFactory.cpp
)

if(NOT CTA_USE_PGSCHED)
    set(SCHEDROUTINES ctaosdbroutines)
else()
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

