#
#                      castor/db/ora/CMakeLists.txt
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
# Rules to build and install libcastorcommonora.so
################################################################################
set (COMMON_ORA_LIB_SRC_FILES
  OraCnvSvc.cpp
  OraStatement.cpp
  OraResultSet.cpp
  OraCommonSvc.cpp
  OraRHSvc.cpp
  OraStagerSvc.cpp
  OraJobSvc.cpp
  OraGCSvc.cpp
  OraQuerySvc.cpp
  SmartOcciResultSet.cpp
  DLFInit.cpp)
add_library (castorcommonora SHARED ${COMMON_ORA_LIB_SRC_FILES})
CastorSetLibraryVersions (castorcommonora)
set_target_properties (castorcommonora PROPERTIES
  COMPILE_FLAGS ${ORACLE_CPPFLAGS}
  LINK_FLAGS "-L${ORACLE_LIBDIR} -locci -lclntsh")
target_link_libraries (castorcommonora castorclient )
install (TARGETS castorcommonora LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)
CastorInstallConfigFile (ORASTAGER)

################################################################################
# Rules to build and install libcastorvdqmora
################################################################################
add_library (castorvdqmora SHARED OraVdqmSvc.cpp)
CastorSetLibraryVersions (castorvdqmora)
set_target_properties (castorvdqmora PROPERTIES
  COMPILE_FLAGS ${ORACLE_CPPFLAGS})
install (TARGETS castorvdqmora LIBRARY DESTINATION ${CASTOR_DEST_LIB_DIR}
  NAMELINK_SKIP)
CastorInstallConfigFile(ORAVDQM)
