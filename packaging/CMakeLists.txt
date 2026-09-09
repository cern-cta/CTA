# SPDX-FileCopyrightText: 2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

install(FILES
  systemd/cta-frontend-grpc.service
  systemd/cta-frontend.service
  systemd/cta-maintd.service
  systemd/cta-rmcd.service
  systemd/cta-taped.service
  DESTINATION ${CTA_SYSTEMD_UNIT_DIR})

install(FILES logrotate/cta-frontend.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-frontend)
install(FILES logrotate/cta-maintd.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-maintd)
install(FILES logrotate/cta-rmcd.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-rmcd)
install(FILES logrotate/cta-taped.logrotate
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/logrotate.d
  RENAME cta-taped)
