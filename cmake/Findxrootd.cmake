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

# - Find xroot
# Finds the header files of xrootd-devel by searching for XrdVersion.hh
# Finds the header files of xrootd-private-devel by searching for XrdOssApi.hh
#
# XROOTD_FOUND               - true if xrootd has been found
# XROOTD_INCLUDE_DIR         - location of the xrootd-devel header files
# XROOTD_PRIVATE_INCLUDE_DIR - location of the private xrootd files, in other
#                              words the header files that do not contribute to
#                              the xrootd ABI.
# XROOTD_XRDCL_LIB           - location of the XrdCl library
# XROOTD_XRDCLIENT_LIB       - location of the XrdClient library
# XROOTD_XRDOFS_LIB          - location of the XrdOfs library
# XROOTD_XRDUTILS_LIB        - location of the XrdUtils library
# XROOTD_XRDPOSIX_LIB        - location of the XrdPosix library

# Be silent if XROOTD_INCLUDE_DIR is already cached
if (XROOTD_INCLUDE_DIR)
  set(XROOTD_FIND_QUIETLY TRUE)
endif (XROOTD_INCLUDE_DIR)

find_path (XROOTD_INCLUDE_DIR XrdVersion.hh
  PATH_SUFFIXES include/xrootd
)

find_path (XROOTD_PRIVATE_INCLUDE_DIR XrdOss/XrdOssApi.hh
  PATH_SUFFIXES include/xrootd/private
)

find_library (XROOTD_XRDCL_LIB XrdCl)
find_library (XROOTD_XRDCLIENT_LIB XrdClient)
find_library (XROOTD_XRDOFS_LIB XrdOfs)
find_library (XROOTD_XRDUTILS_LIB XrdUtils)
find_library (XROOTD_XRDPOSIX_LIB XrdPosixPreload)

message (STATUS "XROOTD_INCLUDE_DIR         = ${XROOTD_INCLUDE_DIR}")
message (STATUS "XROOTD_PRIVATE_INCLUDE_DIR = ${XROOTD_PRIVATE_INCLUDE_DIR}")
message (STATUS "XROOTD_XRDCL_LIB           = ${XROOTD_XRDCL_LIB}")
message (STATUS "XROOTD_XRDCLIENT_LIB       = ${XROOTD_XRDCLIENT_LIB}")
message (STATUS "XROOTD_XRDOFS_LIB          = ${XROOTD_XRDOFS_LIB}")
message (STATUS "XROOTD_XRDPOSIX_LIB        = ${XROOTD_XRDPOSIX_LIB}")

include (FindPackageHandleStandardArgs)
find_package_handle_standard_args (xrootd DEFAULT_MSG 
  XROOTD_INCLUDE_DIR
  XROOTD_PRIVATE_INCLUDE_DIR
  XROOTD_XRDCL_LIB
  XROOTD_XRDCLIENT_LIB
  XROOTD_XRDOFS_LIB
  XROOTD_XRDUTILS_LIB
  XROOTD_XRDPOSIX_LIB)
