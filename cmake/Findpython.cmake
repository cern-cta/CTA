#                      cmake/Findpython.cmake
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

# - Find python
# The following variables are set if python is found.
#
# CASTOR_PYTHON_LIBS
# CASTOR_PYTHON_INC
# CASTOR_DEST_PYTHON_LIBDIR

# Be silent if GLOBUS_CONFINC_PATH is already cached
if (CASTOR_PYTHON_LIBS)
  set(PYTHON_FIND_QUIETLY TRUE)
endif (CASTOR_PYTHON_LIBS)

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/pycompile.py --libs
  OUTPUT_VARIABLE CASTOR_PYTHON_LIBS)
string (REGEX REPLACE "\n" "" CASTOR_PYTHON_LIBS ${CASTOR_PYTHON_LIBS})
message (STATUS "CASTOR_PYTHON_LIBS = ${CASTOR_PYTHON_LIBS}")

execute_process (COMMAND python ${CMAKE_SOURCE_DIR}/config/pycompile.py --inc
  OUTPUT_VARIABLE CASTOR_PYTHON_INC)
string (REGEX REPLACE "\n" "" CASTOR_PYTHON_INC ${CASTOR_PYTHON_INC})
message (STATUS "CASTOR_PYTHON_INC = ${CASTOR_PYTHON_INC}")

execute_process ( COMMAND python -c "from distutils import sysconfig; print sysconfig.get_python_lib()"
  OUTPUT_VARIABLE CASTOR_DEST_PYTHON_LIBDIR)
string (REGEX REPLACE "\n" "" CASTOR_DEST_PYTHON_LIBDIR
  ${CASTOR_DEST_PYTHON_LIBDIR})
message (STATUS "CASTOR_DEST_PYTHON_LIBDIR = ${CASTOR_DEST_PYTHON_LIBDIR}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (python DEFAULT_MSG 
  CASTOR_PYTHON_LIBS
  CASTOR_PYTHON_INC
  CASTOR_DEST_PYTHON_LIBDIR)
