#                      cmake/Findceph.cmake
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
# castor-dev@cern.ch
#

# - Find ceph
#
# CEPH_FOUND               - true if ceph has been found
# RADOS_INCLUDE_DIR        - location of the ceph-devel header files for rados
# RADOS_LIBS               - list of rados libraries, with full path

# Be silent if CEPH_INCLUDE_DIR is already cached
if (RADOS_INCLUDE_DIR)
  set(RADOS_FIND_QUIETLY TRUE)
endif (RADOS_INCLUDE_DIR)

find_path (RADOS_INCLUDE_DIR NAMES radosstriper/libradosstriper.hpp
  PATH_SUFFIXES include/radosstriper
)

find_library (RADOSSTRIPER_LIB radosstriper)
find_library (RADOS_LIB rados)
set(RADOS_LIBS ${RADOS_LIB} ${RADOSSTRIPER_LIB})

message (STATUS "RADOS_INCLUDE_DIR        = ${RADOS_INCLUDE_DIR}")
message (STATUS "RADOS_LIBS               = ${RADOS_LIBS}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (ceph DEFAULT_MSG 
  RADOS_INCLUDE_DIR
  RADOS_LIBS)
