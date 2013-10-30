#
#                      xrootd/CMakeLists.txt
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

find_package (xrootd)

message (STATUS "XROOTD_FOUND               = ${XROOTD_FOUND}")
message (STATUS "XROOTD_INCLUDE_DIR         = ${XROOTD_INCLUDE_DIR}")
message (STATUS "XROOTD_PRIVATE_INCLUDE_DIR = ${XROOTD_PRIVATE_INCLUDE_DIR}")
message (STATUS "XROOTD_XRDCL_LIB           = ${XROOTD_XRDCL_LIB}")
message (STATUS "XROOTD_XRDCLIENT_LIB       = ${XROOTD_XRDCLIENT_LIB}")
message (STATUS "XROOTD_XRDOFS_LIB          = ${XROOTD_XRDOFS_LIB}")
message (STATUS "XROOTD_XRDUTILS_LIB        = ${XROOTD_XRDUTILS_LIB}")

include_directories (${XROOTD_INCLUDE_DIR} ${XROOTD_PRIVATE_INCLUDE_DIR})

################################################################################
# Rules to build and install libXrdxCastor2Fs.so
################################################################################
set (XRDXCASTOR2FS_LIB_SRC_FILES
  XrdxCastor2Fs.cpp
  XrdxCastor2FsConfig.cpp
  XrdxCastor2FsStats.cpp
  XrdxCastor2ServerAcc.cpp
  XrdxCastor2Stager.cpp
  XrdxCastor2Proc.cpp
  XrdxCastorLogging.cpp
  XrdxCastorTiming.cpp
  XrdxCastorClient.cpp)
add_library (XrdxCastor2Fs SHARED ${XRDXCASTOR2FS_LIB_SRC_FILES})
set_target_properties (XrdxCastor2Fs PROPERTIES
  SOVERSION ${MAJOR_CASTOR_VERSION}
  COMPILE_DEFINITIONS _FILE_OFFSET_BITS=64)
target_link_libraries (XrdxCastor2Fs castorclient castorcommon castorns crypto
  ${XROOTD_XRDOFS_LIB} ${XROOTD_XRDUTILS_LIB})
install (TARGETS XrdxCastor2Fs DESTINATION ${CASTOR_DEST_LIB_DIR})

################################################################################
# Rules to build and install libXrdxCastor2ServerAcc.so
################################################################################
set (XRDXCASTOR2SERVERACC_LIB_SRC_FILES
  XrdxCastor2ServerAcc.cpp
  XrdxCastorTiming.cpp)
add_library (XrdxCastor2ServerAcc SHARED ${XRDXCASTOR2SERVERACC_LIB_SRC_FILES})
target_link_libraries (XrdxCastor2ServerAcc crypto ${XROOTD_XRDUTILS_LIB})
install (TARGETS XrdxCastor2ServerAcc DESTINATION ${CASTOR_DEST_LIB_DIR})
