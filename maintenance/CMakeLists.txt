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

add_subdirectory(diskbufferrunners)
add_subdirectory(osrunners)
add_subdirectory(repackrequestrunner)

add_library(ctamaintenancedaemon
  Maintenance.cpp
)

target_link_libraries(ctamaintenancedaemon
	ctacommon
	ctardbms
	ctacatalogue
	ctascheduler
	ctaosdbrunner
	ctadiskbufferrunner
	ctarepackrequestrunner
)

add_executable (cta-maintenance cta-maintenance.cpp)
target_link_libraries(cta-maintenance ctamaintenancedaemon)

add_manpage (cta-maintenance)

install (TARGETS cta-maintenance DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-maintenance.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-maintenance.logrotate DESTINATION /etc/logrotate.d RENAME cta-maintenance)
install (FILES cta-maintenance.sysconfig DESTINATION /etc/sysconfig RENAME cta-maintenance)
install (FILES cta-maintenance.service DESTINATION /etc/systemd/system)
install (FILES cta-maintenance.conf.example 
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

# CTA's cta-taped integration tests.
#add_library(cta-maintenanceIntegrationTests SHARED
#  cta-maintenanceIntegrationTests.cpp)
#set_property(TARGET cta-maintenanceIntegrationTests PROPERTY SOVERSION "${CTA_SOVERSION}")
#set_property(TARGET cta-maintenanceIntegrationTests PROPERTY   VERSION "${CTA_LIBVERSION}")

#target_link_libraries(cta-maintenanceIntegrationTests
#  unitTestHelper
#  ctacommon)

#install(TARGETS cta-maintenanceIntegrationTests DESTINATION usr/${CMAKE_INSTALL_LIBDIR})
