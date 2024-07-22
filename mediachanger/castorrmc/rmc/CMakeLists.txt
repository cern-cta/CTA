# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
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
include_directories (${PROJECT_SOURCE_DIR}/mediachanger/castorrmc/h)

set (RMCD_SRC_FILES
  rmc_serv.c
  rmc_logit.c
  rmc_logreq.c
  rmc_marshall_element.c
  rmc_procreq.c
  rmc_sendrep.c
  rmc_send_scsi_cmd.c
  rmc_serv.c
  rmc_smcsubr.c
  spectra_like_libs.c
)

# Add -Wno-format-overflow compiler flag
set_property(SOURCE rmc_send_scsi_cmd.c APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-format-overflow")

add_executable(cta-rmcd ${RMCD_SRC_FILES})
target_link_libraries(cta-rmcd ctarmccommon)
install (FILES cta-rmcd.conf.example DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

install (TARGETS cta-rmcd DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-rmcd.1cta DESTINATION /usr/share/man/man1)
install (FILES cta-rmcd.logrotate DESTINATION /etc/logrotate.d RENAME cta-rmcd)
install (FILES cta-rmcd.sysconfig DESTINATION /etc/sysconfig RENAME cta-rmcd)
install (FILES cta-rmcd.service DESTINATION /etc/systemd/system)

set (SMC_SRC_FILES
  rmc_dismount.c
  rmc_errmsg.c
  rmc_export.c
  rmc_find_cartridge.c
  rmc_get_geometry.c
  rmc_import.c
  rmc_mount.c
  rmc_read_elem_status.c
  send2rmc.c
  spectra_like_libs.c
  smc.c)
add_executable(cta-smc ${SMC_SRC_FILES})
target_link_libraries(cta-smc ctarmccommon)

add_manpage(cta-rmcd cta-smc)

install (TARGETS cta-smc DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-smc.1cta DESTINATION /usr/share/man/man1)
