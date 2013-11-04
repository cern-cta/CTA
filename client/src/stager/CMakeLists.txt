#                      CMakeLists.txt
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
# Steven.Murray@cern.ch Eric.Cano@cern.ch
#
cmake_minimum_required (VERSION 2.6)

################################################################################
# Rules to build and install the stager client tools
################################################################################

set(STAGER_CLIENT_EXE_SRC_FILES
  castor.c
  stager_abort.cpp
  stager_addprivilege.c
  stager_get.c
  stager_listprivileges.cpp
  stager_update.c
  stager_put.c
  stager_putdone.c
  stager_qry.cpp
  stager_removeprivilege.c
  stager_rm.c
  stager_setfilegcweight.c
)
foreach(SRC_FILE ${STAGER_CLIENT_EXE_SRC_FILES})
  string (REGEX REPLACE "[.]cp*$" "" EXE_FILE ${SRC_FILE})
  add_executable(${EXE_FILE} ${SRC_FILE})
  target_link_libraries (${EXE_FILE} castorcommon castorns castorclient)
  install(TARGETS ${EXE_FILE} DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallExeManPage(${EXE_FILE})
endforeach ()

# Required by castor-srmv2
install(FILES
  stager_client_api_query.hpp
  DESTINATION ${CASTOR_DEST_INCLUDE_DIR}/castor/client/src/stager
  PERMISSIONS ${CASTOR_HEADER_PERMS})
