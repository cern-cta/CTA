#
#                      rmc/CMakeLists.txt
#
# This file is part of the Castor project.
# See http://castor.web.cern.ch/castor
#
# Copyright (C) 2003  CERN
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
#
#
# Steven.Murray@cern.ch
#
cmake_minimum_required (VERSION 2.6)

include_directories ("..")
include_directories ("../h")

set (RMC_LIB_SRC_FILES rmc_dismount.c rmc_errmsg.c rmc_export.c
  rmc_find_cartridge.c rmc_get_geometry.c rmc_import.c rmc_mount.c
  rmc_read_elem_status.c send2rmc.c)
add_library (castorrmc SHARED ${RMC_LIB_SRC_FILES})
set_target_properties (castorrmc
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castorrmc castorcommon)
install (TARGETS castorrmc DESTINATION ${CASTOR_DEST_LIB_DIR})
