# SPDX-FileCopyrightText: 2015 CERN
# SPDX-License-Identifier: GPL-3.0-or-later
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
install(FILES cta-rmcd.example.conf DESTINATION ${CMAKE_INSTALL_SYSCONFDIR}/cta)
