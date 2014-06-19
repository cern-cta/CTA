#
#                      gridftp2/CMakeLists.txt
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

find_package (globus REQUIRED)
find_package (ceph REQUIRED)
include_directories(${RADOS_INCLUDE_DIR})

execute_process (COMMAND uname -m OUTPUT_VARIABLE CASTOR_GLOBUS_UNAME)
string (REGEX REPLACE "\n" "" CASTOR_GLOBUS_UNAME ${CASTOR_GLOBUS_UNAME})
message (STATUS "CASTOR_GLOBUS_UNAME = ${CASTOR_GLOBUS_UNAME}")

if ("${CASTOR_GLOBUS_UNAME}" MATCHES "x86_64")
  set (CASTOR_GLOBUS_MOPTION "-m64")
else ()
  set (CASTOR_GLOBUS_MOPTION "-m32")
endif ()

message (STATUS "CASTOR_GLOBUS_MOPTION = ${CASTOR_GLOBUS_MOPTION}")

include_directories(${GLOBUS_CONFINC_PATH} ${GLOBUS_INC_PATH})

set_property (SOURCE globus_gridftp_server_CASTOR2int.c
  PROPERTY COMPILE_FLAGS "${CASTOR_GLOBUS_MOPTION}")
add_library (globus_gridftp_server_CASTOR2int SHARED
  globus_gridftp_server_CASTOR2int.c)
target_link_libraries(globus_gridftp_server_CASTOR2 ${RADOS_LIBS} castorcephposix)
set_target_properties (globus_gridftp_server_CASTOR2int PROPERTIES
  LINK_FLAGS "${GLOBUS_FTPLIB} -lz")
install (TARGETS globus_gridftp_server_CASTOR2int
  DESTINATION ${CASTOR_DEST_LIB_DIR})
CastorInstallLogRotate(castor-gridftp-dsi-int)
