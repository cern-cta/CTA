#
#                      vmgr/CMakeLists.txt
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

set (VMGR_LIB_SRC_FILES
  vmgr_apiinit.c
  vmgr_deletedenmap.c
  vmgr_deletedgnmap.c
  vmgr_deletelibrary.c
  vmgr_deletemodel.c
  vmgr_deletepool.c
  vmgr_deletetape.c
  vmgr_deltag.c
  vmgr_enterdenmap.c
  vmgr_enterdenmap_byte_u64.c
  vmgr_enterdgnmap.c
  vmgr_enterlibrary.c
  vmgr_entermodel.c
  vmgr_enterpool.c
  vmgr_entertape.c
  vmgr_errmsg.c
  vmgr_gettag.c
  vmgr_gettape.c
  vmgr_listdenmap.c
  vmgr_listdenmap_byte_u64.c
  vmgr_listdgnmap.c
  vmgr_listlibrary.c
  vmgr_listmodel.c
  vmgr_listpool.c
  vmgr_listtape.c
  vmgr_listtape_byte_u64.c
  vmgr_modifylibrary.c
  vmgr_modifymodel.c
  vmgr_modifypool.c
  vmgr_modifytape.c
  vmgr_qrytapeblksize.c
  vmgr_querylibrary.c
  vmgr_querymodel.c
  vmgr_querypool.c
  vmgr_querytape.c
  vmgr_querytape_byte_u64.c
  vmgr_reclaim.c
  vmgr_settag.c
  vmgr_tpmounted.c
  vmgr_updatetape.c
  send2vmgr.c
  vmgrcheck.c)

add_library (castorvmgr SHARED ${VMGR_LIB_SRC_FILES})
set_target_properties (castorvmgr
  PROPERTIES SOVERSION ${MAJOR_CASTOR_VERSION})
target_link_libraries (castorvmgr castorcommon castorupv)
install (TARGETS castorvmgr DESTINATION ${CASTOR_DEST_LIB_DIR})
