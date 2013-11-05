#
#                      vdqm/CMakeLists.txt
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

################################################################################
# Rules to build and install libcastorvdqm.so
################################################################################
set (VDQM_LIB_SRC_FILES vdqmapi.c vdqm_SendRecv.c)
add_library (castorvdqm SHARED ${VDQM_LIB_SRC_FILES})
set_target_properties (castorvdqm
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castorvdqm castorcommon)
install (TARGETS castorvdqm LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)
CastorInstallLibManPage (vdqm_Connect)
CastorInstallLibManPage (vdqm_DelDrive)
CastorInstallLibManPage (vdqm_DelVolumeReq)
CastorInstallLibManPage (vdqm_Disconnect)
CastorInstallLibManPage (vdqm_NextDrive)
CastorInstallLibManPage (vdqm_NextVol)
CastorInstallLibManPage (vdqm_SendVolReq)
CastorInstallLibManPage (vdqm_UnitStatus)

################################################################################
# Rules to install and build showqueues
################################################################################
add_executable (showqueues showqueues.c)
target_link_libraries (showqueues castorcommon castorvdqm)
install (TARGETS showqueues DESTINATION ${CASTOR_DEST_BIN_DIR})
CastorInstallExeManPage (showqueues)

################################################################################
# Rules to install and build vdqm_admin
################################################################################
add_executable (vdqm_admin vdqm_admin.c)
install (TARGETS vdqm_admin DESTINATION ${CASTOR_DEST_BIN_DIR})
target_link_libraries (vdqm_admin castorcommon castorvdqm)
CastorInstallExeManPage (vdqm_admin)
