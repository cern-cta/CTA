# @project      The CERN Tape Archive (CTA)
# @copyright    Copyright © 2015-2022 CERN
# @license      This program is free software, distributed under the terms of the GNU General Public
#               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
#               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
#               option) any later version.
#
#               This program is distributed in the hope that it will be useful, but WITHOUT ANY
#               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
#               PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
#               In applying this licence, CERN does not waive the privileges and immunities
#               granted to it by virtue of its status as an Intergovernmental Organization or
#               submit itself to any jurisdiction.

# This module will set the following variables:
#     ORACLE-INSTANTCLIENT_FOUND
#     ORACLE-INSTANTCLIENT_INCLUDE_DIRS
#     ORACLE-INSTANTCLIENT_LIBRARIES
#     ORACLE-INSTANTCLIENT_RPATH
set(ORACLE-INSTANTCLIENT_VERSION 21.1)

file(GLOB ORACLE_INCLUDE_DIRS "/usr/include/oracle/*/client64")
file(GLOB ORACLE-INSTANTCLIENT_RPATH "/usr/lib/oracle/*/client64/lib")

message(STATUS "ORACLE-INSTANTCLIENT_RPATH=${ORACLE-INSTANTCLIENT_RPATH}")

find_path(ORACLE-INSTANTCLIENT_INCLUDE_DIRS
  occi.h
  PATHS ${ORACLE_INCLUDE_DIRS}
  NO_DEFAULT_PATH)

find_library(ORACLE-INSTANTCLIENT_CLNTSH_LIBRARY
  NAME clntsh
  PATHS ${ORACLE-INSTANTCLIENT_RPATH}
  NO_DEFAULT_PATH)

find_library(ORACLE-INSTANTCLIENT_OCCI_LIBRARY
  NAME libocci.so.${ORACLE-INSTANTCLIENT_VERSION}
  PATHS ${ORACLE-INSTANTCLIENT_RPATH}
  NO_DEFAULT_PATH)

set(ORACLE-INSTANTCLIENT_LIBRARIES
  ${ORACLE-INSTANTCLIENT_OCCI_LIBRARY}
  ${ORACLE-INSTANTCLIENT_CLNTSH_LIBRARY})

message(STATUS "ORACLE-INSTANTCLIENT_INCLUDE_DIRS=${ORACLE-INSTANTCLIENT_INCLUDE_DIRS}")
message(STATUS "ORACLE-INSTANTCLIENT_LIBRARIES=${ORACLE-INSTANTCLIENT_LIBRARIES}")

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(oracle-instantclient DEFAULT_MSG
  ORACLE-INSTANTCLIENT_INCLUDE_DIRS
  ORACLE-INSTANTCLIENT_LIBRARIES
  ORACLE-INSTANTCLIENT_RPATH)
