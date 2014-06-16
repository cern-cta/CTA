#
#                      upv/CMakeLists.txt
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

set (UPV_LIB_SRC_FILES Cupv_apiinit.c Cupv_errmsg.c send2Cupv.c Cupv_delete.c
Cupv_list.c Cupv_util.c Cupv_check.c Cupv_modify.c Cupv_add.c)

add_library (castorupv SHARED ${UPV_LIB_SRC_FILES})
# Relax compilation of old code full of pointer casting. Funnily enough,
# the compiler only complains in -O2 mode (RelWithDebInfo in cmake).
set_property(SOURCE Cupv_apiinit.c APPEND PROPERTY COMPILE_FLAGS -fno-strict-aliasing)
CastorSetLibraryVersions (castorupv)
target_link_libraries (castorupv castorcommon)
install (TARGETS castorupv LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)

set(UPV_API_MAN_PAGES
  Cupv_add
  Cupv_check
  Cupv_delete
  Cupv_list
  Cupv_modify)

foreach(MAN_PAGE ${UPV_API_MAN_PAGES})
  CastorInstallLibManPage(${MAN_PAGE})
endforeach(MAN_PAGE)

set(UPV_CLI_PROGS Cupvadd Cupvmodify Cupvcheck Cupvdelete Cupvlist)
foreach(CLI ${UPV_CLI_PROGS})
  add_executable(${CLI} ${CLI}.c)
  target_link_libraries(${CLI} castorcommon castorupv)
  CastorInstallExeManPage(${CLI})
endforeach(CLI)
install(TARGETS ${UPV_CLI_PROGS} DESTINATION ${CASTOR_DEST_BIN_DIR})

if (${COMPILE_SERVER} STREQUAL "1")

CastorAddProC(Cupv_oracle_ifce)
  set(CUPVD_SOURCE_FILES Cupv_main.c Cupv_oracle_ifce.c Cupv_procreq.c sendrep.c
    Cupv_util.c Cupvlogit.c)
  add_executable(cupvd ${CUPVD_SOURCE_FILES})
  target_link_libraries(cupvd castorcommon castorupv)
  set_target_properties(cupvd PROPERTIES
    COMPILE_FLAGS ${ORACLE_CPPFLAGS}
    LINK_FLAGS "-L${ORACLE_LIBDIR} -lclntsh")
  install(TARGETS cupvd DESTINATION ${CASTOR_DEST_BIN_DIR})
  CastorInstallAdmManPage(cupvd)
  CastorInstallConfigFile(CUPV)
  CastorInstallInitScript(cupvd)
  CastorInstallSysconfigExample(cupvd)
  CastorInstallLogrotate(castor-upv-server)
endif (${COMPILE_SERVER} STREQUAL "1")

################################################################################
# Rules to build and install the CUPV oracle SQL schema
################################################################################
add_custom_target(cupv_oracle_create.sql ALL
  COMMAND ${CMAKE_SOURCE_DIR}/tools/makeSqlScripts.sh cupv ${CASTOR_DB_VERSION} ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}
  DEPENDS oracleSchema.sql oracleTrailer.sql
  COMMENT Creating cupv_oracle_create.sql
)
CastorInstallSQL(cupv_oracle_create.sql)
