# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
cmake_minimum_required (VERSION 3.17)

set(RMCD_SRC_FILES
  cta-rmcd.cpp
  RmcdApp.cpp
  rmc_serv.cpp
  rmc_marshall_element.cpp
  rmc_procreq.cpp
  rmc_sendrep.cpp
  rmc_send_scsi_cmd.cpp
  rmc_smcsubr.cpp)

add_executable(cta-rmcd ${RMCD_SRC_FILES})
target_link_libraries(cta-rmcd ctarmc ctacommon)

set(CTA_RMCD_EXAMPLE_CONFIG ${CMAKE_CURRENT_SOURCE_DIR}/cta-rmcd.example.toml)

add_custom_target(cta-rmcd-config-check
  COMMAND $<TARGET_FILE:cta-rmcd>
    --config-strict
    --config ${CTA_RMCD_EXAMPLE_CONFIG}
    --config-check
  DEPENDS cta-rmcd
  COMMENT "Validating the cta-rmcd example configuration" VERBATIM)
add_dependencies(cta-example-config-checks cta-rmcd-config-check)

add_manpage (cta-rmcd)

install (TARGETS cta-rmcd DESTINATION usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-rmcd.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-rmcd.logrotate DESTINATION /etc/logrotate.d RENAME cta-rmcd)
install (FILES cta-rmcd.service DESTINATION ${CTA_SYSTEMD_UNIT_DIR})
install (FILES cta-rmcd.example.toml
  DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta
  PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ)
