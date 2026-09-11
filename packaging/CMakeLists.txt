# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

install(DIRECTORY DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install(FILES sysusers/cta.conf
  DESTINATION usr/lib/sysusers.d
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
install(FILES ../lib/runtime/cta-logging.schema.json
  DESTINATION usr/${CMAKE_INSTALL_DATADIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)

install(FILES
  systemd/cta-frontend-grpc.service
  systemd/cta-maintd.service
  systemd/cta-rmcd.service
  systemd/cta-taped.service
  DESTINATION ${CTA_SYSTEMD_UNIT_DIR})

# cta-frontend-grpc does not come with a logrotate file yet
install(FILES logrotate/cta-maintd.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-maintd)
install(FILES logrotate/cta-rmcd.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-rmcd)
install(FILES logrotate/cta-taped.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-taped)
