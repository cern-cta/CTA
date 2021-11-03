# @project        The CERN Tape Archive (CTA)
# @copyright      Copyright(C) 2015-2021 CERN
# @license        This program is free software: you can redistribute it and/or modify
#                 it under the terms of the GNU General Public License as published by
#                 the Free Software Foundation, either version 3 of the License, or
#                 (at your option) any later version.
#
#                 This program is distributed in the hope that it will be useful,
#                 but WITHOUT ANY WARRANTY; without even the implied warranty of
#                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#                 GNU General Public License for more details.
#
#                 You should have received a copy of the GNU General Public License
#                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
cmake_minimum_required (VERSION 2.6)
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

if(CMAKE_COMPILER_IS_GNUCC)
  # Add -Wno-format-overflow compiler flag if using gcc version 7 or greater
  if (GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
    set_property(SOURCE rmc_send_scsi_cmd.c APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-format-overflow")
  endif (GCC_VERSION VERSION_EQUAL 7 OR GCC_VERSION VERSION_GREATER 7)
endif(CMAKE_COMPILER_IS_GNUCC)

add_executable(cta-rmcd ${RMCD_SRC_FILES})
target_link_libraries(cta-rmcd ctarmccommon)
install (FILES cta-rmcd.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
install (FILES cta-smc.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)

install (TARGETS cta-rmcd DESTINATION /usr/bin)
install (FILES cta-rmcd.1cta DESTINATION /usr/share/man/man1)
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
install (TARGETS cta-smc DESTINATION /usr/bin)
install (FILES ${CMAKE_CURRENT_SOURCE_DIR}/cta-smc.1cta DESTINATION /usr/share/man/man1)
