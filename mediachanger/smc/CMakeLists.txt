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
