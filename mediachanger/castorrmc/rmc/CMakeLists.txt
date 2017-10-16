# The CERN Tape Archive (CTA) project
# Copyright (C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
)
add_executable(rmcd ${RMCD_SRC_FILES}) 
target_link_libraries(rmcd ctarmccommon)

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
  smc.c)
add_executable(smc ${SMC_SRC_FILES}) 
target_link_libraries(smc ctarmccommon)
