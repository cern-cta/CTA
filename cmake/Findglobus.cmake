#                      cmake/Findglobus.cmake
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

# - Find globus
# Finds the locations of the Globus headers and executables to be used by
# CASTOR.  the following variables are set if globus is found.
#
# GLOBUS_FOUND
# GLOBUS_CONFINC_PATH
# GLOBUS_INC_PATH
# GLOBUS_LIB_PATH
# GLOBUS_GSSLIB
# GLOBUS_FTPLIB
# GLOBUS_GSSLIB_THR

# Be silent if GLOBUS_CONFINC_PATH is already cached
if (GLOBUS_CONFINC_PATH)
  set(GLOBUS_FIND_QUIETLY TRUE)
endif (GLOBUS_CONFINC_PATH)

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --confinc OUTPUT_VARIABLE GLOBUS_CONFINC_PATH)
string (REGEX REPLACE "\n" "" GLOBUS_CONFINC_PATH ${GLOBUS_CONFINC_PATH})
message (STATUS "GLOBUS_CONFINC_PATH = ${GLOBUS_CONFINC_PATH}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --inc OUTPUT_VARIABLE GLOBUS_INC_PATH)
string (REGEX REPLACE "\n" "" GLOBUS_INC_PATH ${GLOBUS_INC_PATH})
message (STATUS "GLOBUS_INC_PATH = ${GLOBUS_INC_PATH}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --libdir OUTPUT_VARIABLE GLOBUS_LIB_PATH)
string (REGEX REPLACE "\n" "" GLOBUS_LIB_PATH ${GLOBUS_LIB_PATH})
message (STATUS "GLOBUS_LIB_PATH = ${GLOBUS_LIB_PATH}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --gsslib OUTPUT_VARIABLE GLOBUS_GSSLIB)
string (REGEX REPLACE "\n" "" GLOBUS_GSSLIB ${GLOBUS_GSSLIB})
message (STATUS "GLOBUS_GSSLIB = ${GLOBUS_GSSLIB}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --ftplib OUTPUT_VARIABLE GLOBUS_FTPLIB)
string (REGEX REPLACE "\n" "" GLOBUS_FTPLIB ${GLOBUS_FTPLIB})
message (STATUS "GLOBUS_FTPLIB = ${GLOBUS_FTPLIB}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/globuscompile.py
  --gsslibthr OUTPUT_VARIABLE GLOBUS_GSSLIB_THR)
string (REGEX REPLACE "\n" "" GLOBUS_GSSLIB_THR ${GLOBUS_GSSLIB_THR})
message (STATUS "GLOBUS_GSSLIB_THR = ${GLOBUS_GSSLIB_THR}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (globus DEFAULT_MSG 
  GLOBUS_FOUND
  GLOBUS_CONFINC_PATH
  GLOBUS_INC_PATH
  GLOBUS_LIB_PATH
  GLOBUS_GSSLIB
  GLOBUS_FTPLIB
  GLOBUS_GSSLIB_THR)
