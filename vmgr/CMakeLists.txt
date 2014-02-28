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

################################################################################
# Rules to build and install libcastorvmgr.so
################################################################################
set_property (SOURCE
  vmgr_enterdenmap_byte_u64.c
  vmgr_gettape.c
  vmgr_listdenmap_byte_u64.c
  vmgr_listpool.c
  vmgr_listtape.c
  vmgr_listtape_byte_u64.c
  vmgr_qrytapeblksize.c
  vmgr_querypool.c
  vmgr_querytape.c
  vmgr_querytape_byte_u64.c
  vmgr_updatetape.c
  APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
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
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE vmgr_apiinit.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
CastorSetLibraryVersions (castorvmgr)
target_link_libraries (castorvmgr castorcommon castorupv)
install (TARGETS castorvmgr DESTINATION ${CASTOR_DEST_LIB_DIR})
CastorInstallLibManPage (vmgr_deletedenmap)
CastorInstallLibManPage (vmgr_deletedgnmap)
CastorInstallLibManPage (vmgr_deletelibrary)
CastorInstallLibManPage (vmgr_deletemodel)
CastorInstallLibManPage (vmgr_deletepool)
CastorInstallLibManPage (vmgr_deletetape)
CastorInstallLibManPage (vmgr_deltag)
CastorInstallLibManPage (vmgr_enterdenmap)
CastorInstallLibManPage (vmgr_enterdenmap_byte_u64)
CastorInstallLibManPage (vmgr_enterdgnmap)
CastorInstallLibManPage (vmgr_enterlibrary)
CastorInstallLibManPage (vmgr_entermodel)
CastorInstallLibManPage (vmgr_enterpool)
CastorInstallLibManPage (vmgr_entertape)
CastorInstallLibManPage (vmgr_gettag)
CastorInstallLibManPage (vmgr_gettape)
CastorInstallLibManPage (vmgr_listdenmap)
CastorInstallLibManPage (vmgr_listdenmap_byte_u64)
CastorInstallLibManPage (vmgr_listdgnmap)
CastorInstallLibManPage (vmgr_listlibrary)
CastorInstallLibManPage (vmgr_listmodel)
CastorInstallLibManPage (vmgr_listpool)
CastorInstallLibManPage (vmgr_listtape)
CastorInstallLibManPage (vmgr_listtape_byte_u64)
CastorInstallLibManPage (vmgr_modifylibrary)
CastorInstallLibManPage (vmgr_modifymodel)
CastorInstallLibManPage (vmgr_modifypool)
CastorInstallLibManPage (vmgr_modifytape)
CastorInstallLibManPage (vmgr_querylibrary)
CastorInstallLibManPage (vmgr_querymodel)
CastorInstallLibManPage (vmgr_querypool)
CastorInstallLibManPage (vmgr_querytape)
CastorInstallLibManPage (vmgr_querytape_byte_u64)
CastorInstallLibManPage (vmgr_reclaim)
CastorInstallLibManPage (vmgr_seterrbuf)
CastorInstallLibManPage (vmgr_settag)
CastorInstallLibManPage (vmgr_tpmounted)
CastorInstallLibManPage (vmgr_updatetape)

if (${COMPILE_SERVER} STREQUAL "1")
  ##############################################################################
  # Rules to build and install vmgrd
  ##############################################################################
  CastorAddProC(vmgr_oracle_ifce)
  set_property (SOURCE
    vmgr_procreq.c
    APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing
  )
  set (VMGR_SRC_FILES vmgr_main.c vmgr_oracle_ifce.c vmgr_procreq.c sendrep.c
    vmgrlogit.c)
  add_executable(vmgrd ${VMGR_SRC_FILES})
  target_link_libraries (vmgrd castorcommon castorupv
    ${ORACLE_LIBDIR}/libclntsh.so)
  install (TARGETS vmgrd DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallAdmManPage (vmgrd)
  CastorInstallLogrotate (castor-vmgr-server)
  CastorInstallSysconfigExample (vmgrd)
  CastorInstallInitScript (vmgrd)
  CastorInstallConfigFile (VMGR)
endif (${COMPILE_SERVER} STREQUAL "1")

################################################################################
# Rule to install the vmgr "umbrella" man page which gives an overview of all
# the vmgr client executables
################################################################################
CastorInstallExeManPage (vmgr)

################################################################################
# Rules to build and install the vmgr client executables
################################################################################
set (VMGR_CLIENT_EXES
  vmgrdeletedenmap
  vmgrdeletedgnmap
  vmgrdeletelibrary
  vmgrdeletemodel
  vmgrdeletepool
  vmgrdeletetape
  vmgrdeltag
  vmgrenterdenmap
  vmgrenterdgnmap
  vmgrenterlibrary
  vmgrentermodel
  vmgrenterpool
  vmgrentertape
  vmgrgettag
  vmgrlistdenmap
  vmgrlistdgnmap
  vmgrlistlibrary
  vmgrlistmodel
  vmgrlistpool
  vmgrlisttape
  vmgrmodifylibrary
  vmgrmodifypool
  vmgrmodifytape
  vmgrsettag)
foreach (CLIENT_EXE ${VMGR_CLIENT_EXES})
  add_executable (${CLIENT_EXE} ${CLIENT_EXE}.c)
  target_link_libraries (${CLIENT_EXE} castorcommon castorvmgr)
  CastorInstallExeManPage (${CLIENT_EXE})
endforeach ()
target_link_libraries (vmgrdeletetape castorns)
install(TARGETS ${VMGR_CLIENT_EXES} DESTINATION ${CASTOR_DEST_BIN_DIR})
