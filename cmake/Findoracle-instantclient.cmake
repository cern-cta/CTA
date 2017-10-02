# The CERN Tape Archive(CTA) project
# Copyright(C) 2015  CERN
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# This module will set the following variables:
#     ORACLE-INSTANTCLIENT_FOUND
#     ORACLE-INSTANTCLIENT_INCLUDE_DIRS
#     ORACLE-INSTANTCLIENT_LIBRARIES

set(ORACLE-INSTANTCLIENT_VERSION 12.2)

set(ORACLE-INSTANTCLIENT_RPATH /usr/lib/oracle/${ORACLE-INSTANTCLIENT_VERSION}/client64/lib)
message(STATUS "ORACLE-INSTANTCLIENT_RPATH=${ORACLE-INSTANTCLIENT_RPATH}")

find_path(ORACLE-INSTANTCLIENT_INCLUDE_DIRS
  occi.h
  PATHS /usr/include/oracle/${ORACLE-INSTANTCLIENT_VERSION}/client64
  NO_DEFAULT_PATH)

find_library(ORACLE-INSTANTCLIENT_CLNTSH_LIBRARY
  NAME clntsh
  PATHS ${ORACLE-INSTANTCLIENT_RPATH}
  NO_DEFAULT_PATH)

find_library(ORACLE-INSTANTCLIENT_OCCI_LIBRARY
  NAME occi
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
