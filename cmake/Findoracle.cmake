#                      cmake/Findxrootd.cmake
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

# - Find oracle
# Finds the locations of the Oracle headers and executables to be used by
# CASTOR
#
# ORACLE_FOUND    - true if oracle has been found
# ORACLE_HOME     - location of the Oracle installiation to be used
# ORACLE_BIN      - location of the Oracle binaries
# ORACLE_LIBDIR   - location of the Oracle libraries
# ORACLE_CPPFLAGS - Oracle CPP flags
# ORACLE_PROCINC  - Oracle Pro*C include flags

# Be silent if ORACLE_HOME is already cached
if (ORACLE_HOME)
  set(ORACLE_FIND_QUIETLY TRUE)
endif (ORACLE_HOME)

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --home OUTPUT_VARIABLE ORACLE_HOME)
string (REGEX REPLACE "\n" "" ORACLE_HOME ${ORACLE_HOME})
message (STATUS "ORACLE_HOME = '${ORACLE_HOME}'")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --bindir OUTPUT_VARIABLE ORACLE_BIN)
string (REGEX REPLACE "\n" "" ORACLE_BIN ${ORACLE_BIN})
message (STATUS "ORACLE_BIN = ${ORACLE_BIN}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --libdir OUTPUT_VARIABLE ORACLE_LIBDIR)
string (REGEX REPLACE "\n" "" ORACLE_LIBDIR ${ORACLE_LIBDIR})
message (STATUS "ORACLE_LIBDIR = ${ORACLE_LIBDIR}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --cppflags OUTPUT_VARIABLE ORACLE_CPPFLAGS)
string (REGEX REPLACE "\n" "" ORACLE_CPPFLAGS ${ORACLE_CPPFLAGS})
message (STATUS "ORACLE_CPPFLAGS = ${ORACLE_CPPFLAGS}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/oracompile.py
    --with-precomp --procinc OUTPUT_VARIABLE ORACLE_PROCINC)
string (REGEX REPLACE "\n" "" ORACLE_PROCINC ${ORACLE_PROCINC})
message (STATUS "ORACLE_PROCINC = ${ORACLE_PROCINC}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (oracle DEFAULT_MSG 
  ORACLE_HOME
  ORACLE_BIN
  ORACLE_LIBDIR
  ORACLE_CPPFLAGS
  ORACLE_PROCINC)
