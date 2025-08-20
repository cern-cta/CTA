# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2025 CERN
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

#
# rmcd
#

set(RMCD_SRC_FILES
  rmc_serv.cpp
  rmc_logit.cpp
  rmc_logreq.cpp
  rmc_marshall_element.cpp
  rmc_procreq.cpp
  rmc_sendrep.cpp
  rmc_send_scsi_cmd.cpp
  rmc_smcsubr.cpp)

# Add -Wno-format-overflow compiler flag
# set_property(SOURCE rmc_send_scsi_cmd.cpp APPEND_STRING PROPERTY COMPILE_FLAGS " -Wno-format-overflow")

add_executable(cta-rmcd ${RMCD_SRC_FILES})
target_link_libraries(cta-rmcd ctarmc)
install(TARGETS cta-rmcd DESTINATION /usr/bin)
install(FILES cta-rmcd.logrotate DESTINATION /etc/logrotate.d RENAME cta-rmcd)
install(FILES cta-rmcd.sysconfig DESTINATION /etc/sysconfig RENAME cta-rmcd)
install(FILES cta-rmcd.service DESTINATION /etc/systemd/system)

#
# documentation and example config
#

add_manpage(cta-rmcd)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-rmcd.1cta DESTINATION /usr/share/man/man1)
install(FILES cta-rmcd.conf.example DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
