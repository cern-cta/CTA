# SPDX-FileCopyrightText: 2015 CERN
#
# SPDX-License-Identifier: GPL-3.0-or-later

cmake_minimum_required (VERSION 3.17)

#
# smc
#

set(SMC_SRC_FILES
  rmc_dismount.cpp
  rmc_errmsg.cpp
  rmc_export.cpp
  rmc_find_cartridge.cpp
  rmc_get_geometry.cpp
  rmc_import.cpp
  rmc_mount.cpp
  rmc_read_elem_status.cpp
  send2rmc.cpp
  smc.cpp)

add_executable(cta-smc ${SMC_SRC_FILES})
target_link_libraries(cta-smc ctarmc)
install(TARGETS cta-smc DESTINATION /usr/bin)

#
# documentation
#

add_manpage(cta-smc)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/cta-smc.1cta DESTINATION /usr/share/man/man1)
