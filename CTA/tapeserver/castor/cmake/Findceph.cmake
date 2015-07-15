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
# RADOS_INCLUDE_DIR        - location of the ceph-devel header files for rados
# RADOS_LIBS               - list of rados libraries, with full path
#
# Note that a dirty hack was introduced for SLC5, removing the dependency on ceph
# and redirecting includes to a fake implementation present in the fakeradosstriper
# directory of CASTOR. This should be dropped when SLC5 support goes.
# The basic rule is : drop any line dealing with SLCVERSION

if (EXISTS /etc/redhat-release)
  file(STRINGS /etc/redhat-release SLC_VERSION REGEX "Scientific Linux CERN SLC release")
  string(SUBSTRING ${SLC_VERSION} 34 1 SLC_VERSION)
endif (EXISTS /etc/redhat-release)
message (STATUS "SLCVERSION               = ${SLC_VERSION}")

# Be silent if CEPH_INCLUDE_DIR is already cached
if (RADOS_INCLUDE_DIR)
  set(RADOS_FIND_QUIETLY TRUE)
endif (RADOS_INCLUDE_DIR)

if (SLC_VERSION AND SLC_VERSION LESS 6)
  set(RADOS_INCLUDE_DIR ${CMAKE_SOURCE_DIR}/fakeradosstriper)
  set(RADOS_LIBS "")
else (SLC_VERSION AND SLC_VERSION LESS 6)
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
endif (SLC_VERSION AND SLC_VERSION LESS 6)

