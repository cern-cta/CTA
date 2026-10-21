# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

install(DIRECTORY DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/cta)
install(FILES sysusers/cta.conf
  DESTINATION lib/sysusers.d
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
install(FILES ../lib/runtime/cta-logging.schema.json
  DESTINATION ${CMAKE_INSTALL_DATADIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

foreach(service cta-frontend-grpc cta-maintd cta-rmcd cta-taped)
  set(unit_source "${CMAKE_CURRENT_SOURCE_DIR}/systemd/${service}.service")
  # Regenerate the installed unit when its source changes.
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${unit_source}")
  file(READ "${unit_source}" unit)
  string(REPLACE "/etc/sysconfig" "${CTA_SERVICE_ENV_DIR}" unit "${unit}")
  file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/${service}.service" "${unit}")
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${service}.service" DESTINATION ${CTA_SYSTEMD_UNIT_DIR})
endforeach()

# cta-frontend-grpc does not come with a logrotate file yet
install(FILES logrotate/cta-maintd.logrotate
  DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/logrotate.d
  RENAME cta-maintd)
install(FILES logrotate/cta-rmcd.logrotate
  DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/logrotate.d
  RENAME cta-rmcd)
install(FILES logrotate/cta-taped.logrotate
  DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/logrotate.d
  RENAME cta-taped)
